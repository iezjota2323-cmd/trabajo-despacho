import pandas as pd
import numpy as np
import re
import sys
import csv
import os
import openpyxl
from pathlib import Path
import uuid
import webbrowser
from threading import Timer
import socket # Para encontrar tu IP local
import joblib
from fuzzywuzzy import fuzz
from sklearn.ensemble import RandomForestClassifier
import time
from datetime import datetime, timedelta

# Importaciones de Flask y Extensiones
from flask import (
    Flask, request, send_file, jsonify, render_template, abort, make_response,
    session, redirect, url_for, flash
)
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# --- Configuración de Flask ---
app = Flask(__name__)

# --- CONFIGURACIÓN DE SEGURIDAD OBLIGATORIA ---
# ¡IMPORTANTE! Hemos movido los secretos de systemd DE VUELTA al archivo .py
# para esta prueba.
app.config['SECRET_KEY'] = 'clave-secreta-muy-aleatoria-para-proteger-sesiones-12345'
# ¡PIN ESTÁTICO! Cámbialo aquí si lo deseas.
PIN_SECRETO = '190805'
# --- FIN DE CONFIGURACIÓN DE SEGURIDAD ---

# --- Configuración de Rate Limiter ---
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["1000 per hour"],
    storage_uri="memory://"
)

# --- File Paths and Setup ---
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_FOLDER = BASE_DIR / 'uploads'
OUTPUT_FOLDER = BASE_DIR / 'outputs'
PIN_DATE_FILE = BASE_DIR / '.last_run'
PIN_CHECK_DAYS = 7
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# --- Carga del Modelo IA ---
MODELO_FILENAME = 'modelo_conciliacion.pkl'
IA_FEATURES = ['diferencia_monto', 'diferencia_dias', 'similitud_folio', 'es_mismo_monto']
MODELO_IA = None
FEATURE_IMPORTANCES = None

print("\n--- Diagnóstico de Carga del Modelo IA ---")
modelo_path_absoluto = BASE_DIR / MODELO_FILENAME
print(f"Buscando el modelo en: {modelo_path_absoluto}")
if os.path.exists(modelo_path_absoluto):
    print("Estado: El archivo modelo_conciliacion.pkl SÍ existe.")
    try:
        MODELO_IA = joblib.load(modelo_path_absoluto)
        print(f"Modelo IA '{MODELO_FILENAME}' cargado.")
        if hasattr(MODELO_IA, 'feature_importances_'):
            importances = MODELO_IA.feature_importances_
            FEATURE_IMPORTANCES = pd.Series(importances, index=IA_FEATURES).sort_values(ascending=False)
            print("Importancia de features cargada.")
        else: FEATURE_IMPORTANCES = None
    except Exception as e:
        print(f"¡ERROR AL CARGAR MODELO!: {e}")
        MODELO_IA = None
else:
    print("Estado: ¡ERROR! Archivo modelo_conciliacion.pkl NO existe.")
print("--- Fin Diagnóstico ---")


# --- Funciones Fecha PIN ---
def update_pin_date():
    try:
        with open(PIN_DATE_FILE, 'w') as f: f.write(datetime.utcnow().isoformat())
    except Exception as e: print(f"Error al actualizar fecha PIN: {e}")

def check_pin_age():
    if not os.path.exists(PIN_DATE_FILE): update_pin_date(); return False
    try:
        with open(PIN_DATE_FILE, 'r') as f: last_date = datetime.fromisoformat(f.read().strip())
        if datetime.utcnow() > last_date + timedelta(days=PIN_CHECK_DAYS): return True
    except Exception as e: print(f"Error al leer fecha PIN: {e}"); return True
    return False

update_pin_date() # Update date on server start

# --- AI Logic Functions ---
def crear_features(cfdi_row, aux_row, calcular_similitud=True):
    try:
        monto_cfdi = cfdi_row.get('Monto_Total', 0)
        monto_aux_debe = aux_row.get('Monto_Debe', 0)
        monto_aux_haber = aux_row.get('Monto_Haber', 0)
        monto_aux = monto_aux_debe if monto_aux_debe > 0 else monto_aux_haber
        diferencia_monto = abs(monto_cfdi - monto_aux)
        diferencia_dias = 999
        if pd.notna(cfdi_row.get('Emisión')) and pd.notna(aux_row.get('Fecha')):
             ts_emision = pd.to_datetime(cfdi_row.get('Emisión'), errors='coerce')
             ts_fecha = pd.to_datetime(aux_row.get('Fecha'), errors='coerce')
             if pd.notna(ts_emision) and pd.notna(ts_fecha):
                diferencia_dias = abs((ts_emision - ts_fecha).days)
        similitud_folio = 0
        if calcular_similitud:
            folio_cfdi = str(cfdi_row.get('Folio_str', ''))
            concepto_aux = str(aux_row.get('Concepto_Upper', ''))
            similitud_folio = fuzz.token_set_ratio(folio_cfdi or '', concepto_aux or '')
        es_mismo_monto = 1 if diferencia_monto < 0.01 else 0
        features_dict = {'diferencia_monto': diferencia_monto, 'diferencia_dias': diferencia_dias, 'similitud_folio': similitud_folio, 'es_mismo_monto': es_mismo_monto}
        features_df = pd.DataFrame([features_dict], columns=IA_FEATURES)
        return features_df
    except Exception as e:
        print(f"Error creando features (UUID: {cfdi_row.get('UUID', 'N/A')}, ID_AUX: {aux_row.get('ID_AUX', 'N/A')}): {e}")
        return pd.DataFrame([[99999, 999, 0, 0]], columns=IA_FEATURES)

def generar_consejo_ia():
    if MODELO_IA is None or FEATURE_IMPORTANCES is None: return "El modelo de IA no se ejecutó o no está cargado."
    try:
        top_feature = FEATURE_IMPORTANCES.idxmax()
        if top_feature == 'similitud_folio': return "La IA prioriza la 'similitud_folio'. Asegúrate de que el Folio (o partes de él) esté escrito en el Concepto del AUX para obtener los mejores resultados."
        elif top_feature == 'diferencia_monto' or top_feature == 'es_mismo_monto': return "La IA prioriza el 'monto exacto'. Capturar los montos con centavos de forma idéntica entre el CFDI y el AUX es clave para el éxito."
        elif top_feature == 'diferencia_dias': return "La IA prioriza la 'cercanía de fechas'. Registrar los movimientos en el AUX en fechas cercanas a la emisión del CFDI mejora mucho la precisión."
        else: return "El modelo está balanceado. Asegúrate de que los montos, fechas y folios sean lo más consistentes posible."
    except Exception as e: print(f"Error generando consejo IA: {e}"); return "Error al generar consejo."

# --- Conciliation Logic ---
def ejecutar_conciliacion(cfdi_input_path, aux_input_path, output_file_path):
    
    # --- Inicio de ejecutar_conciliacion ---
    TOLERANCIA_MONTO = 1.00
    PALABRAS_EXCLUSION = ['NOMINA', 'IMSS', 'SAT', 'INFONAVIT', 'COMISION', 'TRASPASO', 'IMPUESTO']
    dashboard_data = []
    # --- Funciones de Carga y Limpieza ---
    def load_cfdi(filename):
        try:
            df = pd.read_excel(filename, header=4, engine='openpyxl')
            cols_to_keep = ['UUID', 'Folio', 'Total', 'Emisión']
            missing_cols = [col for col in cols_to_keep if col not in df.columns]
            if missing_cols: return None, f"Faltan columnas en CFDI: {', '.join(missing_cols)}"
            df_clean = df[cols_to_keep].copy()
            df_clean['Total'] = pd.to_numeric(df_clean['Total'], errors='coerce')
            df_clean['Emisión'] = pd.to_datetime(df_clean['Emisión'], errors='coerce')
            df_clean['UUID'] = df_clean['UUID'].astype(str).str.upper().str.strip()
            df_clean = df_clean[df_clean['UUID'].str.match(r'^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$', na=False)]
            if df_clean.empty and not df.empty: return None, "Formato UUID inválido o columna UUID vacía en CFDI."
            df_clean['Folio_str'] = df_clean['Folio'].astype(str).str.strip().str.upper().replace('NAN', np.nan)
            df_clean['Monto_Total'] = df_clean['Total'].round(2)
            df_clean.dropna(subset=['UUID', 'Total', 'Emisión'], inplace=True)
            return df_clean, f"TOTAL CFDI CARGADOS: {len(df_clean)}"
        except Exception as e: return None, f"Error al cargar CFDI: {e}"

    def load_aux(filename):
        try:
            df = pd.read_excel(filename, header=0, engine='openpyxl')
            df.columns = df.columns.str.strip()
            if 'Concepto' not in df.columns: return None, "Falta la columna 'Concepto' en AUX."
            df.dropna(subset=['Concepto'], inplace=True)
            if 'Tipo' not in df.columns: return None, "Falta la columna 'Tipo' en AUX."
            df_clean = df[~df['Tipo'].astype(str).str.startswith('-', na=True)].copy()
            cols_to_keep = ['Tipo', 'Numero', 'Fecha', 'Concepto', 'Debe', 'Haber']
            missing_cols = [col for col in cols_to_keep if col not in df_clean.columns]
            if missing_cols: return None, f"Faltan columnas en AUX: {', '.join(missing_cols)}"
            df_clean['Fecha'] = pd.to_datetime(df_clean['Fecha'], errors='coerce', dayfirst=True)
            df_clean['Debe'] = pd.to_numeric(df_clean['Debe'], errors='coerce').fillna(0)
            df_clean['Haber'] = pd.to_numeric(df_clean['Haber'], errors='coerce').fillna(0)
            df_clean.reset_index(drop=True, inplace=True)
            df_clean['ID_AUX'] = df_clean.index
            df_clean['Concepto_Upper'] = df_clean['Concepto'].astype(str).str.upper()
            df_clean['Monto_Debe'] = df_clean['Debe'].round(2)
            df_clean['Monto_Haber'] = df_clean['Haber'].round(2)
            uuid_pattern = re.compile(r'([0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})')
            df_clean['UUID_extract'] = df_clean['Concepto_Upper'].apply(lambda x: (uuid_pattern.search(x) or [None])[0])
            df_clean.dropna(subset=['Fecha'], inplace=True)
            return df_clean, f"TOTAL AUX CARGADOS: {len(df_clean)}"
        except Exception as e: return None, f"Error al cargar AUX: {e}"

    # --- Funciones de Cruce ---
    def match_by_folio_regex(cfdi_df, aux_df, regex_template, match_type_label):
        cfdi_to_match = cfdi_df.dropna(subset=['Folio_str', 'Monto_Total']).copy()
        aux_to_match = aux_df.copy()
        df_encontrados = pd.DataFrame()
        if cfdi_to_match.empty or aux_to_match.empty: return df_encontrados, aux_df, cfdi_df
        aux_debe = aux_to_match[aux_to_match['Monto_Debe'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
        aux_haber = aux_to_match[aux_to_match['Monto_Haber'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
        aux_melted = pd.concat([aux_debe, aux_haber]).dropna(subset=['Monto_Match', 'Concepto_Upper'])
        if aux_melted.empty: return df_encontrados, aux_df, cfdi_df
        unique_folios = cfdi_to_match['Folio_str'].unique()
        all_folio_matches_dfs = []
        for folio in unique_folios:
            try:
                if not folio or pd.isna(folio) or len(str(folio)) < 3: continue
                folio_escaped = re.escape(str(folio)); folio_regex = regex_template.format(folio=folio_escaped)
                aux_with_folio = aux_melted[aux_melted['Concepto_Upper'].str.contains(folio_regex, na=False, regex=True)]
                if aux_with_folio.empty: continue
                cfdi_with_folio = cfdi_to_match[cfdi_to_match['Folio_str'] == folio]
                potential_matches = pd.merge(cfdi_with_folio, aux_with_folio, left_on='Monto_Total', right_on='Monto_Match')
                if not potential_matches.empty: all_folio_matches_dfs.append(potential_matches)
            except Exception as e: print(f"Error folio regex {folio}: {e}"); continue
        if all_folio_matches_dfs:
            df_all_folio_matches = pd.concat(all_folio_matches_dfs, ignore_index=True).drop_duplicates(subset=['ID_AUX'], keep='first').drop_duplicates(subset=['UUID'], keep='first')
            df_merged1 = pd.merge(df_all_folio_matches[['ID_AUX', 'UUID']], aux_df, on='ID_AUX', how='left')
            if 'UUID' not in cfdi_df.columns: return pd.DataFrame(), aux_df, cfdi_df
            cfdi_df_suffixed = cfdi_df.add_suffix('_cfdi')
            if 'UUID' not in df_merged1.columns: return pd.DataFrame(), aux_df, cfdi_df
            df_encontrados = pd.merge(df_merged1, cfdi_df_suffixed, left_on='UUID', right_on='UUID_cfdi', how='left')
            df_encontrados.columns = [col.replace('_cfdi','') for col in df_encontrados.columns]; df_encontrados = df_encontrados.loc[:,~df_encontrados.columns.duplicated()]
            df_encontrados['Match_Type'] = match_type_label
            if 'ID_AUX' not in df_encontrados.columns or 'UUID' not in df_encontrados.columns: matched_aux_ids, matched_cfdi_uuids = [], []
            else: matched_aux_ids, matched_cfdi_uuids = df_encontrados['ID_AUX'].unique(), df_encontrados['UUID'].unique()
            sobrantes_aux, sobrantes_cfdi = aux_df[~aux_df['ID_AUX'].isin(matched_aux_ids)].copy(), cfdi_df[~cfdi_df['UUID'].isin(matched_cfdi_uuids)].copy()
            return df_encontrados, sobrantes_aux, sobrantes_cfdi
        return df_encontrados, aux_df, cfdi_df

    def match_by_monto_exacto(cfdi_df, aux_df, date_window_days, match_type_label):
        df_encontrados = pd.DataFrame()
        if aux_df.empty or cfdi_df.empty: return df_encontrados, aux_df, cfdi_df
        aux_debe = aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Fecha', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
        aux_haber = aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Fecha', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
        aux_melted = pd.concat([aux_debe, aux_haber]).dropna(subset=['Fecha', 'Monto_Match'])
        if aux_melted.empty: return df_encontrados, aux_df, cfdi_df
        cfdi_df_clean = cfdi_df.dropna(subset=['Emisión', 'Monto_Total'])
        merged = pd.merge(cfdi_df_clean, aux_melted, left_on='Monto_Total', right_on='Monto_Match', suffixes=('_CFDI', '_AUX'))
        if merged.empty: return df_encontrados, aux_df, cfdi_df
        merged['Emisión'] = pd.to_datetime(merged['Emisión'], errors='coerce'); merged['Fecha'] = pd.to_datetime(merged['Fecha'], errors='coerce')
        merged.dropna(subset=['Emisión', 'Fecha'], inplace=True)
        if merged.empty: return df_encontrados, aux_df, cfdi_df
        merged['Date_Diff'] = (merged['Emisión'] - merged['Fecha']).abs().dt.days
        matches = merged[merged['Date_Diff'] <= date_window_days].copy() if date_window_days is not None else merged.copy().sort_values(by=['UUID', 'Date_Diff'])
        matches.drop_duplicates(subset=['ID_AUX'], keep='first', inplace=True); matches.drop_duplicates(subset=['UUID'], keep='first', inplace=True)
        if not matches.empty:
             df_encontrados = pd.merge(matches[['UUID', 'ID_AUX', 'Date_Diff']], aux_df, on='ID_AUX', how='left', suffixes=('_MATCH', '_AUX_ORIG'))
             cfdi_cols_needed = ['UUID', 'Folio', 'Total', 'Emisión', 'Folio_str', 'Monto_Total']
             cfdi_cols_to_merge = [col for col in cfdi_cols_needed if col in cfdi_df.columns]
             if 'UUID' in cfdi_df.columns: df_encontrados = pd.merge(df_encontrados, cfdi_df[cfdi_cols_to_merge], on='UUID', how='left', suffixes=('', '_CFDI_ORIG'))
             else:
                  for col in cfdi_cols_to_merge:
                      if col != 'UUID' and col not in df_encontrados.columns: df_encontrados[col] = pd.NA
             df_encontrados = df_encontrados.loc[:,~df_encontrados.columns.duplicated()]; df_encontrados = df_encontrados[[col for col in df_encontrados.columns if not col.endswith('_CFDI_ORIG')]]
             df_encontrados['Match_Type'] = match_type_label
             if 'ID_AUX' not in df_encontrados.columns or 'UUID' not in df_encontrados.columns: matched_aux_ids, matched_cfdi_uuids = [], []
             else: matched_aux_ids, matched_cfdi_uuids = df_encontrados['ID_AUX'].unique(), df_encontrados['UUID'].unique()
             sobrantes_aux, sobrantes_cfdi = aux_df[~aux_df['ID_AUX'].isin(matched_aux_ids)].copy(), cfdi_df[~cfdi_df['UUID'].isin(matched_cfdi_uuids)].copy()
             return df_encontrados, sobrantes_aux, sobrantes_cfdi
        else: return df_encontrados, aux_df, cfdi_df

    def match_by_monto_proximo(cfdi_df, aux_df, tolerance, date_window_days, match_type_label):
        df_encontrados = pd.DataFrame()
        if aux_df.empty or cfdi_df.empty: return df_encontrados, aux_df, cfdi_df
        aux_debe = aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Fecha', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
        aux_haber = aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Fecha', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
        aux_melted = pd.concat([aux_debe, aux_haber]).dropna(subset=['Fecha'])
        cfdi_df_clean = cfdi_df.dropna(subset=['Emisión', 'Monto_Total'])
        all_matches_data, matched_aux_ids = [], set()
        for _, cfdi_row in cfdi_df_clean.iterrows():
            monto_total, fecha_emision = cfdi_row['Monto_Total'], cfdi_row['Emisión']
            monto_min, monto_max = monto_total - tolerance, monto_total + tolerance
            date_min, date_max = fecha_emision - pd.Timedelta(days=date_window_days), fecha_emision + pd.Timedelta(days=date_window_days)
            mask_monto = (aux_melted['Monto_Match'] >= monto_min) & (aux_melted['Monto_Match'] <= monto_max)
            mask_fecha = (aux_melted['Fecha'] >= date_min) & (aux_melted['Fecha'] <= date_max)
            mask_no_exacto = (aux_melted['Monto_Match'] != monto_total)
            candidates = aux_melted[mask_monto & mask_fecha & mask_no_exacto].copy()
            if not candidates.empty:
                candidates['Monto_Diff'] = (candidates['Monto_Match'] - monto_total).abs(); candidates.sort_values(by='Monto_Diff', inplace=True)
                for _, aux_candidate in candidates.iterrows():
                    if aux_candidate['ID_AUX'] not in matched_aux_ids:
                        all_matches_data.append({'UUID': cfdi_row['UUID'], 'ID_AUX': aux_candidate['ID_AUX'], 'Monto_Diff': aux_candidate['Monto_Diff']}); matched_aux_ids.add(aux_candidate['ID_AUX']); break
        if not all_matches_data: return df_encontrados, aux_df, cfdi_df
        matches = pd.DataFrame(all_matches_data)
        df_encontrados = pd.merge(matches, aux_df, on='ID_AUX', how='left', suffixes=('_MATCH', '_AUX_ORIG'))
        cfdi_cols_needed = ['UUID', 'Folio', 'Total', 'Emisión', 'Folio_str', 'Monto_Total']
        cfdi_cols_to_merge = [col for col in cfdi_cols_needed if col in cfdi_df.columns]
        if 'UUID' in cfdi_df.columns: df_encontrados = pd.merge(df_encontrados, cfdi_df[cfdi_cols_to_merge], on='UUID', how='left', suffixes=('', '_CFDI_ORIG'))
        else:
            for col in cfdi_cols_to_merge:
                 if col != 'UUID' and col not in df_encontrados.columns: df_encontrados[col] = pd.NA
        df_encontrados = df_encontrados.loc[:,~df_encontrados.columns.duplicated()]; df_encontrados = df_encontrados[[col for col in df_encontrados.columns if not col.endswith('_CFDI_ORIG')]]
        df_encontrados['Match_Type'] = match_type_label
        if 'ID_AUX' not in df_encontrados.columns or 'UUID' not in df_encontrados.columns: matched_aux_ids_set, matched_cfdi_uuids_set = set(), set()
        else: matched_aux_ids_set, matched_cfdi_uuids_set = set(df_encontrados['ID_AUX'].dropna().unique()), set(df_encontrados['UUID'].dropna().unique())
        sobrantes_aux, sobrantes_cfdi = aux_df[~aux_df['ID_AUX'].isin(matched_aux_ids_set)].copy(), cfdi_df[~cfdi_df['UUID'].isin(matched_cfdi_uuids_set)].copy()
        return df_encontrados, sobrantes_aux, sobrantes_cfdi

    # --- Proceso Principal ---
    start_time = time.time()
    all_encontrados_dfs = []
    df_cfdi_orig, msg_cfdi = load_cfdi(cfdi_input_path)
    dashboard_data.append({"Paso": "TOTAL CFDI CARGADOS", "Coincidencias": msg_cfdi.split(': ')[1] if df_cfdi_orig is not None else "Error"})
    if df_cfdi_orig is None: return False, dashboard_data, f"Error al cargar CFDI: {msg_cfdi}"
    df_aux_orig, msg_aux = load_aux(aux_input_path)
    if df_aux_orig is None: return False, dashboard_data, f"Error al cargar AUX: {msg_aux}"

    # ... (Pases 0-7) ...
    exclusion_regex = r'\b(?:' + '|'.join(PALABRAS_EXCLUSION) + r')\b'; mask_ruido = df_aux_orig['Concepto_Upper'].str.contains(exclusion_regex, na=False, regex=True)
    df_aux_ruido = df_aux_orig[mask_ruido].copy(); df_aux = df_aux_orig[~mask_ruido].copy(); dashboard_data.append({"Paso": "TOTAL AUX CARGADOS (Relevantes)", "Coincidencias": len(df_aux)})
    #P1
    if 'UUID_extract' not in df_aux.columns: df_aux['UUID_extract'] = None
    if 'UUID' not in df_cfdi_orig.columns: return False, dashboard_data, "Falta UUID en CFDI."
    df_encontrados_p1 = pd.merge(df_aux.dropna(subset=['UUID_extract']), df_cfdi_orig, left_on='UUID_extract', right_on='UUID', suffixes=('_AUX', '_CFDI'))
    if not df_encontrados_p1.empty: df_encontrados_p1['Match_Type'] = 'UUID'
    all_encontrados_dfs.append(df_encontrados_p1); matched_aux_ids_p1 = df_encontrados_p1['ID_AUX'].unique() if not df_encontrados_p1.empty else []; matched_cfdi_uuids_p1 = df_encontrados_p1['UUID'].unique() if not df_encontrados_p1.empty else []
    sobrantes_aux_p1 = df_aux[~df_aux['ID_AUX'].isin(matched_aux_ids_p1)].copy(); sobrantes_cfdi_p1 = df_cfdi_orig[~df_cfdi_orig['UUID'].isin(matched_cfdi_uuids_p1)].copy(); dashboard_data.append({"Paso": "100% Fiabilidad (UUID)", "Coincidencias": len(df_encontrados_p1)})
    #P2
    regex_p2 = r'\b{folio}\b'; df_encontrados_p2, sobrantes_aux_p2, sobrantes_cfdi_p2 = match_by_folio_regex(sobrantes_cfdi_p1, sobrantes_aux_p1, regex_p2, 'Folio+Monto'); all_encontrados_dfs.append(df_encontrados_p2); dashboard_data.append({"Paso": "98% Fiabilidad (Folio Exacto)", "Coincidencias": len(df_encontrados_p2)})
    #P3
    regex_p3 = r'{folio}(?:\b|$)'; df_encontrados_p3, sobrantes_aux_p3, sobrantes_cfdi_p3 = match_by_folio_regex(sobrantes_cfdi_p2, sobrantes_aux_p2, regex_p3, 'FolioParcial+Monto'); all_encontrados_dfs.append(df_encontrados_p3); dashboard_data.append({"Paso": "95% Fiabilidad (Folio Parcial)", "Coincidencias": len(df_encontrados_p3)})
    #P4
    df_encontrados_p4, sobrantes_aux_p4, sobrantes_cfdi_p4 = match_by_monto_exacto(sobrantes_cfdi_p3, sobrantes_aux_p3, 5, 'Monto+Fecha(5d)'); all_encontrados_dfs.append(df_encontrados_p4); dashboard_data.append({"Paso": "85% Fiabilidad (Monto+Fecha 5d)", "Coincidencias": len(df_encontrados_p4)})
    #P5
    df_encontrados_p5, sobrantes_aux_p5, sobrantes_cfdi_p5 = match_by_monto_exacto(sobrantes_cfdi_p4, sobrantes_aux_p4, 30, 'Monto+Fecha(30d)'); all_encontrados_dfs.append(df_encontrados_p5); dashboard_data.append({"Paso": "70% Fiabilidad (Monto+Fecha 30d)", "Coincidencias": len(df_encontrados_p5)})
    #P6
    df_encontrados_p6, sobrantes_aux_p6, sobrantes_cfdi_p6 = match_by_monto_exacto(sobrantes_cfdi_p5, sobrantes_aux_p5, None, 'Monto(Solo)'); all_encontrados_dfs.append(df_encontrados_p6); dashboard_data.append({"Paso": "60% Fiabilidad (Monto Solo)", "Coincidencias": len(df_encontrados_p6)})
    #P7
    df_encontrados_p7, sobrantes_aux_p7, sobrantes_cfdi_p7 = match_by_monto_proximo(sobrantes_cfdi_p6, sobrantes_aux_p6, TOLERANCIA_MONTO, 30, f'Monto_Proximo(${TOLERANCIA_MONTO})'); all_encontrados_dfs.append(df_encontrados_p7); dashboard_data.append({"Paso": "50% Fiabilidad (Monto Próximo)", "Coincidencias": len(df_encontrados_p7)})

    # --- PASE 8 ---
    # ... (código optimizado del Pase 8) ...
    df_encontrados_p8 = pd.DataFrame()
    sobrantes_aux_final = sobrantes_aux_p7.copy(); sobrantes_cfdi_final = sobrantes_cfdi_p7.copy(); start_time_ia = time.time()
    if MODELO_IA is not None and not sobrantes_aux_p7.empty and not sobrantes_cfdi_p7.empty:
        print("\n--- Iniciando Pase 8: IA (Optimizado) ---")
        try:
            UMBRAL_MONTO_IA, UMBRAL_DIAS_IA = 10.0, 90
            sobrantes_cfdi_p7_copy, sobrantes_aux_p7_copy = sobrantes_cfdi_p7.copy(), sobrantes_aux_p7.copy()
            sobrantes_cfdi_p7_copy.loc[:, 'key'], sobrantes_aux_p7_copy.loc[:, 'key'] = 1, 1
            df_pares_total = pd.merge(sobrantes_cfdi_p7_copy, sobrantes_aux_p7_copy, on='key', suffixes=('_CFDI', '_AUX')).drop('key', axis=1)
            total_pares_inicial = len(df_pares_total); print(f"Pares iniciales: {total_pares_inicial}")
            if not df_pares_total.empty:
                print("Calculando features rápidas..."); features_rapidas_list = []
                cfdi_cols_orig, aux_cols_orig = df_cfdi_orig.columns, df_aux_orig.columns
                for idx, row in df_pares_total.iterrows():
                    cfdi_row = pd.Series({col: row.get(f'{col}_CFDI') for col in cfdi_cols_orig}, index=cfdi_cols_orig)
                    aux_row = pd.Series({col: row.get(f'{col}_AUX') for col in aux_cols_orig}, index=aux_cols_orig)
                    try: # Forzar tipos
                        cfdi_row['Monto_Total']=pd.to_numeric(cfdi_row['Monto_Total'],errors='coerce'); cfdi_row['Emisión']=pd.to_datetime(cfdi_row['Emisión'],errors='coerce')
                        aux_row['Monto_Debe']=pd.to_numeric(aux_row['Monto_Debe'],errors='coerce'); aux_row['Monto_Haber']=pd.to_numeric(aux_row['Monto_Haber'],errors='coerce'); aux_row['Fecha']=pd.to_datetime(aux_row['Fecha'],errors='coerce')
                        features_rapidas_list.append(crear_features(cfdi_row, aux_row, calcular_similitud=False))
                    except Exception as ferr: features_rapidas_list.append(pd.DataFrame([[99999, 999, 0, 0]], columns=IA_FEATURES))
                if features_rapidas_list:
                    df_features_rapidas = pd.concat(features_rapidas_list, ignore_index=True)
                    df_pares_total['diferencia_monto'], df_pares_total['diferencia_dias'] = df_features_rapidas['diferencia_monto'], df_features_rapidas['diferencia_dias']
                    mask_monto, mask_dias = df_pares_total['diferencia_monto'] <= UMBRAL_MONTO_IA, df_pares_total['diferencia_dias'] <= UMBRAL_DIAS_IA
                    df_pares_filtrados = df_pares_total[mask_monto & mask_dias].copy()
                    total_pares_filtrados = len(df_pares_filtrados); print(f"Pares filtrados: {total_pares_filtrados}")
                    if not df_pares_filtrados.empty:
                        print(f"Calculando similitud folio para {total_pares_filtrados} pares..."); similitudes, contador_progreso = [], 0; paso_progreso = max(1, total_pares_filtrados // 10)
                        for idx, row in df_pares_filtrados.iterrows():
                            folio_cfdi, concepto_aux = str(row.get('Folio_str_CFDI', '')), str(row.get('Concepto_Upper_AUX', ''))
                            similitudes.append(fuzz.token_set_ratio(folio_cfdi or '', concepto_aux or ''))
                            contador_progreso += 1;
                            if contador_progreso % paso_progreso == 0: print(f"    ... Similitud par {contador_progreso}/{total_pares_filtrados} ({int(contador_progreso/total_pares_filtrados*100)}%)")
                        print("    ... Cálculo similitud completado."); df_pares_filtrados['similitud_folio'], df_pares_filtrados['es_mismo_monto'] = similitudes, (df_pares_filtrados['diferencia_monto'] < 0.01).astype(int)
                        df_features_finales = df_pares_filtrados[IA_FEATURES].copy().fillna(0).astype(float)
                        print("Prediciendo..."); probabilidades = MODELO_IA.predict_proba(df_features_finales)[:, 1]; df_pares_filtrados['IA_Probabilidad'] = probabilidades
                        umbral_confianza = 0.90; df_matches_ia = df_pares_filtrados[df_pares_filtrados['IA_Probabilidad'] >= umbral_confianza].copy().sort_values(by='IA_Probabilidad', ascending=False).drop_duplicates(subset='UUID_CFDI', keep='first').drop_duplicates(subset='ID_AUX_AUX', keep='first')
                        if not df_matches_ia.empty:
                            cols_ia_needed = ['UUID_CFDI', 'ID_AUX_AUX', 'IA_Probabilidad']
                            if 'diferencia_monto' in df_matches_ia.columns: df_matches_ia['Monto_Diff'] = df_matches_ia['diferencia_monto']; cols_ia_needed.append('Monto_Diff')
                            df_to_merge_ia = df_matches_ia[cols_ia_needed]
                            df_encontrados_p8 = pd.merge(df_to_merge_ia, df_cfdi_orig.add_suffix('_orig_cfdi'), left_on='UUID_CFDI', right_on='UUID_orig_cfdi', how='left')
                            df_encontrados_p8 = pd.merge(df_encontrados_p8, df_aux_orig.add_suffix('_orig_aux'), left_on='ID_AUX_AUX', right_on='ID_AUX_orig_aux', how='left')
                            rename_map_p8 = {f'{col}_orig_cfdi': col for col in df_cfdi_orig.columns}; rename_map_p8.update({f'{col}_orig_aux': col for col in df_aux_orig.columns})
                            df_encontrados_p8.rename(columns=rename_map_p8, inplace=True); df_encontrados_p8.drop(columns=['UUID_CFDI', 'ID_AUX_AUX', 'UUID_orig_cfdi', 'ID_AUX_orig_aux'], inplace=True, errors='ignore'); df_encontrados_p8 = df_encontrados_p8.loc[:,~df_encontrados_p8.columns.duplicated()]
                            df_encontrados_p8['Match_Type'] = 'IA_Prediccion (>' + str(int(umbral_confianza*100)) + '%)'; all_encontrados_dfs.append(df_encontrados_p8)
                            matched_aux_ids_p8, matched_cfdi_uuids_p8 = df_encontrados_p8['ID_AUX'].unique(), df_encontrados_p8['UUID'].unique()
                            sobrantes_aux_final, sobrantes_cfdi_final = sobrantes_aux_p7[~sobrantes_aux_p7['ID_AUX'].isin(matched_aux_ids_p8)].copy(), sobrantes_cfdi_p7[~sobrantes_cfdi_p7['UUID'].isin(matched_cfdi_uuids_p8)].copy(); print(f"Pase 8 (IA) completado: {len(df_encontrados_p8)} coincidencias.")
                        else: print("Pase 8 (IA): Ningún par filtrado superó umbral.")
                    else: print("Pase 8 (IA): Ningún par superó filtros iniciales.")
                else: print("Pase 8 (IA): No se generaron features rápidas.")
            else: print("Pase 8 (IA): No hay pares iniciales.")
        except Exception as e: import traceback; print(f"ERROR DETALLADO en Pase 8 (IA): {traceback.format_exc()}"); sobrantes_aux_final, sobrantes_cfdi_final = sobrantes_aux_p7.copy(), sobrantes_cfdi_p7.copy()
    else: print("Pase 8 (IA) omitido.")
    end_time_ia = time.time(); print(f"Tiempo Pase 8 (IA): {end_time_ia - start_time_ia:.2f} segs")
    dashboard_data.append({"Paso": "90-99% Fiabilidad (IA)", "Coincidencias": len(df_encontrados_p8)})

    # --- Resultados Finales y Guardado ---
    dashboard_data.append({"Paso": "---", "Coincidencias": "---"})
    if not all_encontrados_dfs or all(df.empty for df in all_encontrados_dfs): df_encontrados_final, total_conciliado = pd.DataFrame(), 0
    else:
        all_encontrados_dfs_validos = [df for df in all_encontrados_dfs if not df.empty]
        if not all_encontrados_dfs_validos: df_encontrados_final, total_conciliado = pd.DataFrame(), 0
        else:
            try:
                all_cols = set().union(*(df.columns for df in all_encontrados_dfs_validos)); all_cols = sorted(list(all_cols))
                dfs_alineados = [df.reindex(columns=all_cols).astype(object) for df in all_encontrados_dfs_validos]
                df_encontrados_final = pd.concat(dfs_alineados, ignore_index=True, sort=False)
                if 'UUID' in df_encontrados_final.columns:
                     df_encontrados_final['UUID'] = df_encontrados_final['UUID'].astype(str)
                     valid_uuids = df_encontrados_final['UUID'].str.match(r'^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$', na=False)
                     total_conciliado = df_encontrados_final.loc[valid_uuids & df_encontrados_final['UUID'].notna(), 'UUID'].nunique()
                else: total_conciliado = 0
            except Exception as e: import traceback; print(f"Error concatenando: {traceback.format_exc()}"); df_encontrados_final, total_conciliado = pd.DataFrame(), 0
    dashboard_data.append({"Paso": "TOTAL CONCILIADO", "Coincidencias": total_conciliado})
    dashboard_data.append({"Paso": "SOBRANTES CFDI", "Coincidencias": len(sobrantes_cfdi_final)})
    dashboard_data.append({"Paso": "SOBRANTES AUX", "Coincidencias": len(sobrantes_aux_final)})
    print(f"\n--- Generando Excel: {output_file_path} ---")
    try:
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            df_dashboard = pd.DataFrame(dashboard_data); df_dashboard.columns = ["Nivel de Fiabilidad / Concepto", "Conciliaciones / Cantidad"]
            consejo_ia_texto = generar_consejo_ia(); df_consejo = pd.DataFrame({"CONSEJO DE LA INTELIGENCIA ARTIFICIAL": [consejo_ia_texto], "": [""]})
            df_dashboard.to_excel(writer, sheet_name='Dashboard', index=False, startrow=1); df_consejo.to_excel(writer, sheet_name='Dashboard', index=False, startrow=len(df_dashboard) + 4)
            if not df_encontrados_final.empty:
                aux_cols_final = ['Fecha', 'Concepto', 'Debe', 'Haber', 'Tipo', 'Numero', 'ID_AUX']; cfdi_cols_final = ['UUID', 'Folio', 'Total', 'Emisión']
                extra_cols = ['Match_Type', 'Monto_Diff', 'IA_Probabilidad']
                df_encontrados_final.columns = [col.replace('_AUX_orig','').replace('_AUX','').replace('_CFDI','') for col in df_encontrados_final.columns]; df_encontrados_final = df_encontrados_final.loc[:,~df_encontrados_final.columns.duplicated()]
                final_cols = extra_cols + cfdi_cols_final + aux_cols_final
                final_cols_real_existing = [col for col in final_cols if col in df_encontrados_final.columns]
                df_encontrados_final_clean = df_encontrados_final[final_cols_real_existing].copy()
                for col in ['Total', 'Debe', 'Haber', 'Monto_Diff', 'IA_Probabilidad']:
                     if col in df_encontrados_final_clean.columns: df_encontrados_final_clean[col] = pd.to_numeric(df_encontrados_final_clean[col], errors='coerce')
                for col in ['Fecha', 'Emisión']:
                     if col in df_encontrados_final_clean.columns: df_encontrados_final_clean[col] = pd.to_datetime(df_encontrados_final_clean[col], errors='coerce')
                if 'Match_Type' in df_encontrados_final_clean.columns:
                    df_encontrados_final_clean['Match_Type'].fillna('Desconocido', inplace=True); df_encontrados_final_clean['Match_Type'] = df_encontrados_final_clean['Match_Type'].astype(str); df_encontrados_final_clean.sort_values(by='Match_Type', inplace=True, na_position='last')
            else: df_encontrados_final_clean = pd.DataFrame()
            tipos_alta = ['UUID', 'Folio+Monto', 'FolioParcial+Monto']; df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_alta)].to_excel(writer, sheet_name='Confianza_Alta_95_100', index=False)
            tipos_media = ['Monto+Fecha(5d)']; df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_media)].to_excel(writer, sheet_name='Confianza_Media_80', index=False)
            tipos_baja = ['Monto+Fecha(30d)', 'Monto(Solo)']; df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_baja)].to_excel(writer, sheet_name='Confianza_Baja_Revisar', index=False)
            tipos_proximo = [col for col in df_encontrados_final_clean['Match_Type'].unique() if 'Monto_Proximo' in str(col)]; df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_proximo)].to_excel(writer, sheet_name='Confianza_Muy_Baja_Proximidad', index=False)
            tipos_ia = [col for col in df_encontrados_final_clean['Match_Type'].unique() if 'IA_Prediccion' in str(col)]; df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_ia)].to_excel(writer, sheet_name='Confianza_IA_Prediccion', index=False)
            cols_to_drop_aux = ['Concepto_Upper', 'Monto_Debe', 'Monto_Haber', 'UUID_extract', 'key']; sobrantes_aux_final.drop(columns=cols_to_drop_aux, errors='ignore').to_excel(writer, sheet_name='No_Coincidencias_AUX', index=False)
            cols_to_drop_cfdi = ['Folio_str', 'Monto_Total', 'key']; sobrantes_cfdi_final.drop(columns=cols_to_drop_cfdi, errors='ignore').to_excel(writer, sheet_name='Sobrantes_CFDI', index=False)
            df_aux_ruido.drop(columns=cols_to_drop_aux, errors='ignore').to_excel(writer, sheet_name='AUX_Filtrado_No_Requerido', index=False)
        end_time = time.time(); print(f"\n¡Éxito! Excel generado en {end_time - start_time:.2f} segs.")
        return True, dashboard_data, consejo_ia_texto
    except Exception as e:
        import traceback; print(f"\nERROR AL GUARDAR Excel: {traceback.format_exc()}"); dashboard_data.append({"Paso": "ERROR AL GUARDAR", "Coincidencias": str(e)})
        return False, dashboard_data, f"Error al guardar Excel: {e}"

# ===========================================================================
# === INICIO DEL CÓDIGO DEL SERVIDOR (Back-End API) ===
# ===========================================================================

@app.before_request
def require_login():
    # Volvemos a la estructura de 2 páginas: /login y / (app principal)
    allowed_routes = ['login', 'static']
    if request.endpoint not in allowed_routes and 'logged_in' not in session:
        return redirect(url_for('login'))
    pass


@app.route('/login', methods=['GET', 'POST'])
@limiter.limit("5 per minute")
def login():
    if request.method == 'POST':
        pin_ingresado = request.form.get('pin')
        if pin_ingresado == PIN_SECRETO:
            session['logged_in'] = True
            print("Inicio de sesión exitoso.")
            return redirect(url_for('index')) # Redirige a la app principal
        else:
            print(f"Intento de login fallido con PIN: {pin_ingresado} desde IP: {get_remote_address()}")
            flash('PIN incorrecto. Inténtalo de nuevo.', 'error')
            return redirect(url_for('login'))
    # Si ya está logueado, redirigir a index
    if 'logged_in' in session:
         return redirect(url_for('index'))
    return render_template('login.html') # Renderiza la página de login

# Manejador de error para Rate Limiting
@app.errorhandler(429)
def ratelimit_handler(e):
    flash("Demasiados intentos de inicio de sesión. Por favor, espera un minuto.", "error")
    return redirect(url_for('login'))


@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    print("Sesión cerrada.")
    flash('Has cerrado la sesión.', 'success')
    return redirect(url_for('login')) # Redirige a la página de login

# Ruta Raíz (/) ahora es la App Principal
@app.route('/')
def index():
    # Protegido por @app.before_request
    print("Solicitud recibida en /. Sirviendo el front-end (index.html).")
    if check_pin_age():
        flash(f"Recordatorio Seguridad: >{PIN_CHECK_DAYS} días. Cambia PIN en app.py y reinicia.", "warning")
    return render_template('index.html') # Renderiza la app de conciliación


@app.route('/procesar', methods=['POST'])
@limiter.limit("1 per 10 second")
def procesar_archivo():
    # Protegido por @app.before_request
    print("Recibida solicitud en /procesar")
    if 'archivo_cfdi' not in request.files or 'archivo_aux' not in request.files:
        return jsonify({"error": "Faltan archivos. Se requieren 'archivo_cfdi' y 'archivo_aux'."}), 400
    file_cfdi = request.files['archivo_cfdi']
    file_aux = request.files['archivo_aux']
    if file_cfdi.filename == '' or file_aux.filename == '':
        return jsonify({"error": "No se seleccionó uno o más archivos."}), 400

    cfdi_name = "".join(c for c in file_cfdi.filename if c.isalnum() or c in (' ', '.', '_')).rstrip().replace('.xlsx', '').replace(' ', '_')
    aux_name = "".join(c for c in file_aux.filename if c.isalnum() or c in (' ', '.', '_')).rstrip().replace('.xlsx', '').replace(' ', '_')
    unique_id = str(uuid.uuid4())[:8]; output_filename = f"Conciliacion_{unique_id}.xlsx"
    cfdi_input_path, aux_input_path = UPLOAD_FOLDER / f"{unique_id}_cfdi.xlsx", UPLOAD_FOLDER / f"{unique_id}_aux.xlsx"
    output_path = OUTPUT_FOLDER / output_filename
    try:
        MAX_FILE_SIZE = 50 * 1024 * 1024
        cl_cfdi = request.content_length or file_cfdi.seek(0, os.SEEK_END)
        file_cfdi.seek(0)
        cl_aux = request.content_length or file_aux.seek(0, os.SEEK_END)
        file_aux.seek(0)
        if (cl_cfdi is not None and cl_cfdi > MAX_FILE_SIZE) or (cl_aux is not None and cl_aux > MAX_FILE_SIZE): return jsonify({"error": f"Archivo excede {MAX_FILE_SIZE // 1024 // 1024} MB."}), 413
        file_cfdi.save(cfdi_input_path); file_aux.save(aux_input_path)
        success, dashboard_data, consejo_ia = ejecutar_conciliacion(str(cfdi_input_path), str(aux_input_path), str(output_path))
        if success: return jsonify({"message": "¡Éxito!", "dashboard": dashboard_data, "consejo": consejo_ia, "downloadFile": output_filename})
        else: error_msg = consejo_ia; print(f"Error ejecución/guardado: {error_msg}"); return jsonify({"error": "Falló proceso/guardado.", "dashboard": dashboard_data, "consejo": consejo_ia}), 500
    except Exception as e:
        import traceback; print(f"ERROR CRÍTICO /procesar: {traceback.format_exc()}"); error_detail = str(e)
        if isinstance(e, MemoryError): error_detail = "Error memoria."
        elif isinstance(e, KeyError): error_detail = f"Error columna: {e}."
        elif "could not convert string to float" in error_detail: error_detail = "Error tipo dato IA."
        return jsonify({"error": f"Error interno: {error_detail}"}), 500
    finally:
        for f_path in [cfdi_input_path, aux_input_path]:
            try:
                if f_path and os.path.exists(f_path): os.remove(f_path)
            except Exception as e_clean: print(f"Error limpiando {f_path}: {e_clean}")


@app.route('/descargar/<path:nombre_archivo>')
def descargar_archivo(nombre_archivo):
    # Protegido por @app.before_request
    print(f"Solicitud descarga: {nombre_archivo}")
    try:
        safe_path = Path(OUTPUT_FOLDER).resolve()
        file_path = (safe_path / nombre_archivo).resolve()
        if not str(file_path).startswith(str(safe_path)): abort(400)
        if not os.path.exists(file_path): abort(404)
        response = make_response(send_file(file_path, as_attachment=True))
        return response
    except Exception as e: print(f"Error /descargar: {e}"); abort(500)


# Añadir encabezados de seguridad
@app.after_request
def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response

# ===========================================================================
# === LÓGICA DE INICIO DEL SERVIDOR (EJECUTAR DIRECTAMENTE CON PYTHON) ===
# ===========================================================================

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try: s.connect(('8.8.8.8', 1)); IP = s.getsockname()[0]
    except Exception:
        try: IP = socket.gethostbyname(socket.gethostname())
        except Exception: IP = '127.0.0.1'
    finally: s.close()
    return IP

def open_browser():
    """Función para abrir el navegador automáticamente."""
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
         # Apunta al puerto correcto 5001
        Timer(1, lambda: webbrowser.open_new('http://127.0.0.1:5001')).start()

# --- Punto de entrada para ejecutar el servidor DIRECTAMENTE CON PYTHON ---
if __name__ == '__main__':
    # Quitar open_browser() para que no se abra en el servidor
    # open_browser() 
    local_ip = get_local_ip()

    print("=================================================================")
    print(f" SERVIDOR DE CONCILIACIÓN INICIADO (Modo Desarrollo) ")
    print("=================================================================")
    print(f" * Acceso local (esta máquina): http://127.0.0.1:5001")
    if local_ip != '127.0.0.1':
        print(f" * Acceso en red local: http://{local_ip}:5001")
    print(f" * PIN de Acceso: {PIN_SECRETO}")
    print("=================================================================")
    print(" * Presiona CTRL+C para detener el servidor.")
    print("************************************************************")
    print("¡ADVERTENCIA DE SEGURIDAD!")
    print("Estás ejecutando en 'debug=True' en una IP de red (0.0.0.0).")
    print("Esto es PELIGROSO si alguien más está en tu red.")
    print("************************************************************")

   