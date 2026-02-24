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

# --- IMPORTAMOS LOS MÓDULOS ACTUALIZADOS ---
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

# --- INICIALIZACIÓN DE DB ---

def init_db():
    with app.app_context():
        db.create_all()
        # Usuarios Maestros
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

def log_activity(action, details):
    if current_user.is_authenticated:
        log = ActivityLog(user_id=current_user.id, action=action, details=details)
        db.session.add(log)
        db.session.commit()

# --- RUTAS ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username').upper()
        pin = request.form.get('pin')
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, pin):
            if user.status != 'activo':
                flash('Cuenta pendiente de aprobación.', 'error')
                return redirect(url_for('login'))
            login_user(user)
            log_activity("Login", f"Usuario {username} ha entrado.")
            return redirect(url_for('home'))
        flash('Credenciales incorrectas.', 'error')
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username').upper()
        pin = request.form.get('pin')
        if User.query.filter_by(username=username).first():
            flash('El usuario ya existe.', 'error')
            return redirect(url_for('register'))
        new_user = User(username=username, password=generate_password_hash(pin), status='pendiente')
        db.session.add(new_user)
        db.session.commit()
        flash('Solicitud enviada.', 'success')
        return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def root():
    return redirect(url_for('home'))

@app.route('/home')
@login_required
def home():
    return render_template('home.html')

@app.route('/herramientas')
@login_required
def index():
    active_tab = request.args.get('tab', 'conciliador') 
    return render_template('index.html', tab=active_tab)

@app.route('/admin')
@login_required
def admin_dashboard():
    if current_user.role != 'superadmin': abort(403)
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
    flash(f"Usuario {user.username} aprobado.", "success")
    return redirect(url_for('admin_dashboard'))

# --- PROCESAMIENTO IA ---
@app.route('/procesar', methods=['POST'])
@login_required
def procesar_ia():
    if 'archivo_cfdi' not in request.files or 'archivo_aux' not in request.files or 'archivo_pdf' not in request.files:
        flash("Faltan archivos.", "error")
        return redirect(url_for('index'))
        
    f_cfdi, f_aux, f_pdf = request.files['archivo_cfdi'], request.files['archivo_aux'], request.files['archivo_pdf']
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = UPLOAD_FOLDER / unique_id
    pdf_dir = temp_dir / "pdfs"
    ent_dir = temp_dir / "entregables"
    os.makedirs(pdf_dir, exist_ok=True)
    os.makedirs(ent_dir, exist_ok=True)

    try:
        cfdi_p = temp_dir / "cfdi.xlsx"
        aux_p = temp_dir / "aux.xlsx"
        pdf_z = temp_dir / "pdfs.zip"
        f_cfdi.save(cfdi_p)
        f_aux.save(aux_p)
        f_pdf.save(pdf_z)
        with zipfile.ZipFile(pdf_z, 'r') as z: z.extractall(pdf_dir)

        out_p = ent_dir / f"Conciliacion_IA_{unique_id}.xlsx"
        success, db_data, res_ia = ejecutar_conciliacion(str(cfdi_p), str(aux_p), str(out_p), str(pdf_dir), str(ent_dir))
        
        if success:
            shutil.make_archive(str(OUTPUT_FOLDER / f"Resultados_IA_{unique_id}"), 'zip', str(ent_dir))
            return render_template('index.html', tab='conciliador', dashboard=db_data, consejo=res_ia, downloadFile=f"Resultados_IA_{unique_id}.zip")
        flash(f"Error: {res_ia}", "error")
    except Exception as e: flash(f"Error: {e}", "error")
    finally: shutil.rmtree(temp_dir, ignore_errors=True)
    return redirect(url_for('index', tab='conciliador'))

# --- PROCESAMIENTO IVA ---
@app.route('/procesar_auditoria', methods=['POST'])
@login_required
def procesar_iva():
    # Nuevos nombres de campos desde el formulario de index.html
    if 'archivo_cfdi_iva' not in request.files or 'archivo_aux_iva' not in request.files or 'archivo_pdf_iva' not in request.files:
        flash("Faltan archivos para Conciliación IVA.", "error")
        return redirect(url_for('index', tab='auditoria'))
        
    f_cfdi = request.files['archivo_cfdi_iva']
    f_aux = request.files['archivo_aux_iva']
    f_pdf = request.files['archivo_pdf_iva']
    
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = UPLOAD_FOLDER / unique_id
    pdf_dir = temp_dir / "pdfs"
    ent_dir = temp_dir / "entregables"
    os.makedirs(pdf_dir, exist_ok=True)
    os.makedirs(ent_dir, exist_ok=True)

    try:
        # En el módulo auditoría (Conciliación IVA), necesitamos combinar CFDI y AUX en uno o procesarlos.
        # El módulo actual espera un solo 'ruta_excel'. Vamos a crear un temporal que tenga ambas hojas si es necesario, 
        # o mejor, modificamos el módulo para aceptar ambos. 
        # Pero por ahora, vamos a unir CFDI y AUX en un solo libro temporal para el módulo de auditoría.
        
        cfdi_p = temp_dir / "cfdi.xlsx"
        aux_p = temp_dir / "aux.xlsx"
        f_cfdi.save(cfdi_p)
        f_aux.save(aux_p)
        
        # Combinar en un solo Excel para el modulo_auditoria
        combined_p = temp_dir / "combined.xlsx"
        with pd.ExcelWriter(combined_p, engine='openpyxl') as writer:
            pd.read_excel(cfdi_p).to_excel(writer, sheet_name='CFDI', index=False)
            pd.read_excel(aux_p).to_excel(writer, sheet_name='AUX', index=False)

        pdf_z = temp_dir / "pdfs.zip"
        f_pdf.save(pdf_z)
        with zipfile.ZipFile(pdf_z, 'r') as z: z.extractall(pdf_dir)

        success, msg = ejecutar_auditoria(str(combined_p), str(pdf_dir), str(ent_dir))
        
        if success:
            shutil.make_archive(str(OUTPUT_FOLDER / f"Conciliacion_IVA_{unique_id}"), 'zip', str(ent_dir))
            return render_template('index.html', tab='auditoria', success_auditoria=msg, downloadFileAuditoria=f"Conciliacion_IVA_{unique_id}.zip")
        flash(f"Error: {msg}", "error")
    except Exception as e: flash(f"Error: {e}", "error")
    finally: shutil.rmtree(temp_dir, ignore_errors=True)
    return redirect(url_for('index', tab='auditoria'))

import pandas as pd # Importado para la combinación temporal

@app.route('/descargar/<path:filename>')
@login_required
def descargar(filename):
    return send_file(OUTPUT_FOLDER / filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')