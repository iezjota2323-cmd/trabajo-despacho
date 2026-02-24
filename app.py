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

# --- IMPORTAMOS TUS MÓDULOS OPTIMIZADOS ---
from modules.modulo_auditoria import ejecutar_auditoria
from modules.modulo_conciliacion import ejecutar_conciliacion, generar_resumen_ia

app = Flask(__name__)
app.config['SECRET_KEY'] = 'clave-secreta-muy-aleatoria-para-proteger-sesiones-12345'
PIN_SECRETO = '190805'

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["1000 per hour"],
    storage_uri="memory://"
)

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_FOLDER = BASE_DIR / 'uploads'
OUTPUT_FOLDER = BASE_DIR / 'outputs'
PIN_DATE_FILE = BASE_DIR / '.last_run'
PIN_CHECK_DAYS = 7
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def update_pin_date():
    try:
        with open(PIN_DATE_FILE, 'w') as f: f.write(datetime.utcnow().isoformat())
    except Exception: pass

update_pin_date()

@app.before_request
def require_login():
    allowed_routes = ['login', 'static']
    if request.endpoint and request.endpoint not in allowed_routes and 'logged_in' not in session:
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
@limiter.limit("5 per minute")
def login():
    if request.method == 'POST':
        if request.form.get('pin') == PIN_SECRETO:
            session['logged_in'] = True
            return redirect(url_for('index'))
        else:
            flash('PIN incorrecto. Inténtalo de nuevo.', 'error')
            return redirect(url_for('login'))
    if 'logged_in' in session: return redirect(url_for('index'))
    return render_template('login.html')

@app.errorhandler(429)
def ratelimit_handler(e):
    flash("Demasiados intentos de inicio de sesión.", "error")
    return redirect(url_for('login'))

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('Has cerrado la sesión.', 'success')
    return redirect(url_for('login'))

@app.route('/')
def index():
    active_tab = request.args.get('tab', 'conciliador') 
    return render_template('index.html', tab=active_tab)

@app.route('/procesar', methods=['POST'])
@limiter.limit("1 per 10 second")
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

        # MANDAMOS LLAMAR AL NUEVO MÓDULO DE CONCILIACIÓN
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
@limiter.limit("1 per 10 second")
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

        # MANDAMOS LLAMAR AL NUEVO MÓDULO DE AUDITORÍA
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
def descargar_archivo(nombre_archivo):
    safe_path = Path(OUTPUT_FOLDER).resolve()
    file_path = (safe_path / nombre_archivo).resolve()
    if not str(file_path).startswith(str(safe_path)) or not os.path.exists(file_path): abort(404)
    return make_response(send_file(file_path, as_attachment=True))

@app.after_request
def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')