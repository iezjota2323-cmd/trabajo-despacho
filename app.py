import os
import uuid
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

from flask import (
    Flask, request, send_file, render_template, abort, make_response,
    session, redirect, url_for, flash
)
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_login import (
    LoginManager, UserMixin, login_user, login_required, 
    logout_user, current_user
)
from werkzeug.security import generate_password_hash, check_password_hash

# --- IMPORTAMOS TUS MÓDULOS OPTIMIZADOS ---
from modules.modulo_auditoria import ejecutar_auditoria
from modules.modulo_conciliacion import ejecutar_conciliacion, generar_resumen_ia

app = Flask(__name__)
app.config['SECRET_KEY'] = 'clave-secreta-paniagua-palacios-2024'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///suite_financiera.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

# --- MODELOS ---

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    role = db.Column(db.String(20), default='admin') # 'superadmin' o 'admin'
    status = db.Column(db.String(20), default='pendiene') # 'activo' o 'pendiente'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class ActivityLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    action = db.Column(db.String(100))
    details = db.Column(db.String(255))
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    user = db.relationship('User', backref=db.backref('logs', lazy=True))

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# --- CONFIGURACIÓN DE RUTAS Y LIMITADORES ---

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["2000 per hour"],
    storage_uri="memory://"
)

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_FOLDER = BASE_DIR / 'uploads'
OUTPUT_FOLDER = BASE_DIR / 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# --- INICIALIZACIÓN DE DB Y USUARIOS MAESTROS ---

def init_db():
    with app.app_context():
        db.create_all()
        
        # Usuarios Maestros (Superadmins)
        masters = {
            'YASMINPALACIOS': '19080519',
            'LAURACRUZ': '19080518',
            'LUISPANIAGUA': '19080505'
        }
        
        for user, pin in masters.items():
            if not User.query.filter_by(username=user).first():
                new_master = User(
                    username=user,
                    password=generate_password_hash(pin),
                    role='superadmin',
                    status='activo'
                )
                db.session.add(new_master)
        db.session.commit()

init_db()

# --- FUNCIONES AUXILIARES ---

def log_activity(action, details):
    if current_user.is_authenticated:
        log = ActivityLog(user_id=current_user.id, action=action, details=details)
        db.session.add(log)
        db.session.commit()

# --- RUTAS DE AUTENTICACIÓN ---

@app.route('/login', methods=['GET', 'POST'])
@limiter.limit("10 per minute")
def login():
    if request.method == 'POST':
        username = request.form.get('username').upper()
        pin = request.form.get('pin')
        
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, pin):
            if user.status != 'activo':
                flash('Tu cuenta aún no ha sido aprobada por un administrador.', 'error')
                return redirect(url_for('login'))
            
            login_user(user)
            log_activity("Inicio de sesión", f"Usuario {username} ha entrado.")
            return redirect(url_for('index'))
        else:
            flash('Usuario o PIN incorrectos.', 'error')
            
    if current_user.is_authenticated: return redirect(url_for('index'))
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
@limiter.limit("5 per hour")
def register():
    if request.method == 'POST':
        username = request.form.get('username').upper()
        pin = request.form.get('pin')
        
        if User.query.filter_by(username=username).first():
            flash('El usuario ya existe.', 'error')
            return redirect(url_for('register'))
            
        new_user = User(
            username=username,
            password=generate_password_hash(pin),
            role='admin', # De acuerdo a la petición: admin para herramientas
            status='pendiente'
        )
        db.session.add(new_user)
        db.session.commit()
        flash('Solicitud enviada correctamente. Espera aprobación.', 'success')
        return redirect(url_for('login'))
        
    return render_template('register.html')

@app.route('/logout')
@login_required
def logout():
    log_activity("Cierre de sesión", f"Usuario {current_user.username} ha salido.")
    logout_user()
    flash('Has cerrado la sesión.', 'success')
    return redirect(url_for('login'))

# --- RUTA PRINCIPAL (HERRAMIENTAS) ---

@app.route('/')
@login_required
def index():
    active_tab = request.args.get('tab', 'conciliador') 
    return render_template('index.html', tab=active_tab)

# --- RUTAS DE ADMINISTRADOR (SUPERADMIN) ---

@app.route('/admin')
@login_required
def admin_dashboard():
    if current_user.role != 'superadmin':
        abort(403)
    
    users = User.query.all()
    logs = ActivityLog.query.order_by(ActivityLog.timestamp.desc()).limit(50).all()
    return render_template('admin_dashboard.html', users=users, logs=logs)

@app.route('/admin/approve/<int:user_id>')
@login_required
def approve_user(user_id):
    if current_user.role != 'superadmin': abort(403)
    user = User.query.get_or_404(user_id)
    user.status = 'activo'
    db.session.commit()
    log_activity("Aprobación de usuario", f"Superadmin aprobó a {user.username}")
    flash(f"Usuario {user.username} aprobado.", "success")
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/reject/<int:user_id>')
@login_required
def reject_user(user_id):
    if current_user.role != 'superadmin': abort(403)
    user = User.query.get_or_404(user_id)
    # No borramos, solo marcamos o eliminamos si se prefiere
    db.session.delete(user)
    db.session.commit()
    log_activity("Rechazo de usuario", f"Superadmin eliminó a {user.username}")
    flash(f"Usuario {user.username} eliminado.", "error")
    return redirect(url_for('admin_dashboard'))

# --- PROCESAMIENTO ---

@app.route('/procesar', methods=['POST'])
@login_required
@limiter.limit("5 per minute")
def procesar_archivo():
    if 'archivo_cfdi' not in request.files or 'archivo_aux' not in request.files or 'archivo_pdf' not in request.files:
        flash("Faltan archivos por subir.", "error")
        return redirect(url_for('index'))
        
    file_cfdi, file_aux, file_pdf = request.files['archivo_cfdi'], request.files['archivo_aux'], request.files['archivo_pdf']
    if file_cfdi.filename == '' or file_aux.filename == '' or file_pdf.filename == '':
        flash("No se seleccionó uno o más archivos.", "error")
        return redirect(url_for('index'))

    unique_id = str(uuid.uuid4())[:8]
    temp_dir = UPLOAD_FOLDER / unique_id
    pdf_extract_dir = temp_dir / "pdfs"
    entregables_dir = temp_dir / "entregables"
    os.makedirs(pdf_extract_dir, exist_ok=True)
    os.makedirs(entregables_dir, exist_ok=True)

    cfdi_input_path = temp_dir / f"{unique_id}_cfdi.xlsx"
    aux_input_path = temp_dir / f"{unique_id}_aux.xlsx"
    pdf_zip_path = temp_dir / f"{unique_id}_pdfs.zip"
    excel_output_filename = f"Conciliacion_{unique_id}.xlsx"
    excel_output_path = entregables_dir / excel_output_filename
    final_zip_name = f"Resultados_{unique_id}.zip"

    try:
        file_cfdi.save(cfdi_input_path)
        file_aux.save(aux_input_path)
        file_pdf.save(pdf_zip_path)
        
        with zipfile.ZipFile(pdf_zip_path, 'r') as zip_ref:
            zip_ref.extractall(pdf_extract_dir)

        log_activity("Conciliación", f"Procesando {file_cfdi.filename} y {file_aux.filename}")

        success, dashboard_data, resumen_ia = ejecutar_conciliacion(str(cfdi_input_path), str(aux_input_path), str(excel_output_path), str(pdf_extract_dir), str(entregables_dir))
        
        if success:
            shutil.make_archive(str(OUTPUT_FOLDER / f"Resultados_{unique_id}"), 'zip', str(entregables_dir))
            return render_template('index.html', tab='conciliador', dashboard=dashboard_data, consejo=resumen_ia, downloadFile=final_zip_name, success="¡Proceso completado con éxito!")
        else:
            flash(f"Falló proceso: {resumen_ia}", "error")
            return redirect(url_for('index', tab='conciliador'))
    except Exception as e:
        flash(f"Error interno: {e}", "error")
        return redirect(url_for('index', tab='conciliador'))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

@app.route('/procesar_auditoria', methods=['POST'])
@login_required
@limiter.limit("5 per minute")
def procesar_auditoria_ruta():
    if 'archivo_excel' not in request.files or 'archivo_pdf' not in request.files:
        flash("Faltan archivos por subir en el módulo Auditoría.", "error")
        return redirect(url_for('index', tab='auditoria'))
        
    file_excel = request.files['archivo_excel']
    file_pdf = request.files['archivo_pdf']
    
    if file_excel.filename == '' or file_pdf.filename == '':
        flash("No se seleccionó uno o más archivos.", "error")
        return redirect(url_for('index', tab='auditoria'))

    unique_id = str(uuid.uuid4())[:8]
    temp_dir = UPLOAD_FOLDER / unique_id
    pdf_extract_dir = temp_dir / "pdfs"
    entregables_dir = temp_dir / "entregables"
    os.makedirs(pdf_extract_dir, exist_ok=True)
    os.makedirs(entregables_dir, exist_ok=True)

    excel_input_path = temp_dir / f"{unique_id}_gsm.xlsx"
    pdf_zip_path = temp_dir / f"{unique_id}_pdfs.zip"
    final_zip_name = f"Entregables_Auditoria_GSM_{unique_id}.zip"

    try:
        file_excel.save(excel_input_path)
        file_pdf.save(pdf_zip_path)
        
        with zipfile.ZipFile(pdf_zip_path, 'r') as zip_ref:
            zip_ref.extractall(pdf_extract_dir)

        log_activity("Auditoría", f"Procesando {file_excel.filename} para GSM")

        success, mensaje = ejecutar_auditoria(str(excel_input_path), str(pdf_extract_dir), str(entregables_dir))
        
        if success:
            shutil.make_archive(str(OUTPUT_FOLDER / f"Entregables_Auditoria_GSM_{unique_id}"), 'zip', str(entregables_dir))
            return render_template('index.html', tab='auditoria', success_auditoria=mensaje, downloadFileAuditoria=final_zip_name)
        else:
            flash(f"Falló auditoría: {mensaje}", "error")
            return redirect(url_for('index', tab='auditoria'))
    except Exception as e:
        flash(f"Error interno: {e}", "error")
        return redirect(url_for('index', tab='auditoria'))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

@app.route('/descargar/<path:nombre_archivo>')
@login_required
def descargar_archivo(nombre_archivo):
    safe_path = Path(OUTPUT_FOLDER).resolve()
    file_path = (safe_path / nombre_archivo).resolve()
    if not str(file_path).startswith(str(safe_path)) or not os.path.exists(file_path): abort(404)
    log_activity("Descarga", f"Descargando archivo {nombre_archivo}")
    return make_response(send_file(file_path, as_attachment=True))

@app.after_request
def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')