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
import time # Para medir tiempos

# Importaciones de Flask para seguridad y sesiones
from flask import (
    Flask, request, send_file, jsonify, render_template, abort, make_response,
    session, redirect, url_for, flash
)

# --- Configuración de Flask ---
app = Flask(__name__)

# --- CONFIGURACIÓN DE SEGURIDAD OBLIGATORIA ---
# Esta clave secreta protege las "sesiones" de inicio de sesión.
app.config['SECRET_KEY'] = 'clave-secreta-muy-aleatoria-para-proteger-sesiones-12345'

# ¡CAMBIA ESTE PIN! Este es el PIN para acceder a la aplicación.
PIN_SECRETO = '191919' # ¡CAMBIA ESTO POR UN PIN SEGURO!
# --- FIN DE CONFIGURACIÓN DE SEGURIDAD ---

# Directorios para guardar archivos temporalmente
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_FOLDER = BASE_DIR / 'uploads'
OUTPUT_FOLDER = BASE_DIR / 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# --- Carga del Modelo IA ---
MODELO_FILENAME = 'modelo_conciliacion.pkl'
IA_FEATURES = ['diferencia_monto', 'diferencia_dias', 'similitud_folio', 'es_mismo_monto']
MODELO_IA = None
FEATURE_IMPORTANCES = None

# --- Diagnóstico de Carga del Modelo IA ---
print("\n--- Diagnóstico de Carga del Modelo IA ---")
modelo_path_absoluto = BASE_DIR / MODELO_FILENAME
print(f"Buscando el modelo en: {modelo_path_absoluto}")
if os.path.exists(modelo_path_absoluto):
    print("Estado: El archivo modelo_conciliacion.pkl SÍ existe en la ruta esperada.")
    try:
        MODELO_IA = joblib.load(modelo_path_absoluto)
        print(f"Modelo de IA '{MODELO_FILENAME}' cargado exitosamente.")
        # Manejo de error si el modelo no tiene 'feature_importances_'
        if hasattr(MODELO_IA, 'feature_importances_'):
            importances = MODELO_IA.feature_importances_
            FEATURE_IMPORTANCES = pd.Series(importances, index=IA_FEATURES).sort_values(ascending=False)
            print("Importancia de features cargada.")
        else:
            print("Advertencia: El modelo cargado no tiene 'feature_importances_'. No se podrán generar consejos.")
            FEATURE_IMPORTANCES = None
    except Exception as e:
        print(f"¡ERROR FATAL AL CARGAR EL MODELO!: {e}")
        MODELO_IA = None
else:
    print("Estado: ¡ERROR! El archivo modelo_conciliacion.pkl NO existe en la ruta esperada.")
    print("El Pase 8 (IA) no se ejecutará y no se darán consejos.")
    print("Asegúrate de ejecutar 'train_model.py' y que el archivo .pkl esté en la misma carpeta que app.py.")
print("--- Fin Diagnóstico ---")


# =============================================================================
# === LÓGICA DE IA (Funciones de Features y Consejos) ===
# =============================================================================

def crear_features(cfdi_row, aux_row, calcular_similitud=True):
    """
    Crea un vector de features (números) para un par CFDI-AUX.
    Añadido parámetro para calcular similitud condicionalmente.
    """
    try:
        # Asegurar que las columnas existan, si no usar valor por defecto seguro
        monto_cfdi = cfdi_row.get('Monto_Total', 0)
        monto_aux_debe = aux_row.get('Monto_Debe', 0)
        monto_aux_haber = aux_row.get('Monto_Haber', 0)
        monto_aux = monto_aux_debe if monto_aux_debe > 0 else monto_aux_haber
        diferencia_monto = abs(monto_cfdi - monto_aux)

        diferencia_dias = 999 # Valor por defecto alto si faltan fechas
        if pd.notna(cfdi_row.get('Emisión')) and pd.notna(aux_row.get('Fecha')):
             ts_emision = pd.to_datetime(cfdi_row.get('Emisión'), errors='coerce')
             ts_fecha = pd.to_datetime(aux_row.get('Fecha'), errors='coerce')
             if pd.notna(ts_emision) and pd.notna(ts_fecha):
                diferencia_dias = abs((ts_emision - ts_fecha).days)

        similitud_folio = 0 # Valor por defecto si no se calcula
        if calcular_similitud:
            folio_cfdi = str(cfdi_row.get('Folio_str', ''))
            concepto_aux = str(aux_row.get('Concepto_Upper', ''))
            # Asegurar que ambos strings no sean None antes de pasar a fuzz
            similitud_folio = fuzz.token_set_ratio(folio_cfdi or '', concepto_aux or '')

        es_mismo_monto = 1 if diferencia_monto < 0.01 else 0

        # Crear diccionario para asegurar orden correcto
        features_dict = {
            'diferencia_monto': diferencia_monto,
            'diferencia_dias': diferencia_dias,
            'similitud_folio': similitud_folio,
            'es_mismo_monto': es_mismo_monto
        }
        # Crear DataFrame usando el orden de IA_FEATURES
        features_df = pd.DataFrame([features_dict], columns=IA_FEATURES)
        return features_df

    except Exception as e:
        print(f"Error creando features para IA (UUID: {cfdi_row.get('UUID', 'N/A')}, ID_AUX: {aux_row.get('ID_AUX', 'N/A')}): {e}")
        # Devolver DataFrame con el orden correcto en caso de error
        return pd.DataFrame([[99999, 999, 0, 0]], columns=IA_FEATURES)


def generar_consejo_ia():
    # (Sin cambios respecto a la versión anterior)
    if MODELO_IA is None or FEATURE_IMPORTANCES is None:
        return "El modelo de IA no se ejecutó o no está cargado. (Ejecuta 'train_model.py')"
    try:
        top_feature = FEATURE_IMPORTANCES.idxmax()
        if top_feature == 'similitud_folio':
            return "La IA prioriza la 'similitud_folio'. Asegúrate de que el Folio (o partes de él) esté escrito en el Concepto del AUX para obtener los mejores resultados."
        elif top_feature == 'diferencia_monto' or top_feature == 'es_mismo_monto':
            return "La IA prioriza el 'monto exacto'. Capturar los montos con centavos de forma idéntica entre el CFDI y el AUX es clave para el éxito."
        elif top_feature == 'diferencia_dias':
            return "La IA prioriza la 'cercanía de fechas'. Registrar los movimientos en el AUX en fechas cercanas a la emisión del CFDI mejora mucho la precisión."
        else:
            return "El modelo está balanceado. Asegúrate de que los montos, fechas y folios sean lo más consistentes posible."
    except Exception as e:
        print(f"Error generando consejo IA: {e}")
        return "No se pudo generar el consejo de la IA debido a un error."

# =============================================================================
# === LÓGICA DE CONCILIACIÓN (Pases 1-8 y Dashboard) ===
# =============================================================================

def ejecutar_conciliacion(cfdi_input_path, aux_input_path, output_file_path):
    TOLERANCIA_MONTO = 1.00
    PALABRAS_EXCLUSION = ['NOMINA', 'IMSS', 'SAT', 'INFONAVIT', 'COMISION', 'TRASPASO', 'IMPUESTO']
    dashboard_data = []

    # --- Funciones de Carga y Limpieza (Anidadas) ---
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
            df_clean['Monto_Total'] = df_clean['Total'].round(2) # Redondear aquí
            df_clean.dropna(subset=['UUID', 'Total', 'Emisión'], inplace=True)
            return df_clean, f"TOTAL CFDI CARGADOS: {len(df_clean)}"
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, f"Error al cargar CFDI: {e}"

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
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, f"Error al cargar AUX: {e}"

    # --- Funciones de Cruce (Pases 2-7, anidadas) ---
    def match_by_folio_regex(cfdi_df, aux_df, regex_template, match_type_label):
        # (Corrección KeyError ya aplicada)
        cfdi_to_match = cfdi_df.dropna(subset=['Folio_str', 'Monto_Total']).copy()
        aux_to_match = aux_df.copy()
        df_encontrados = pd.DataFrame()
        if cfdi_to_match.empty or aux_to_match.empty: return df_encontrados, aux_df, cfdi_df
        aux_debe = aux_to_match[aux_to_match['Monto_Debe'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
        aux_haber = aux_to_match[aux_to_match['Monto_Haber'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
        aux_melted = pd.concat([aux_debe, aux_haber])
        aux_melted.dropna(subset=['Monto_Match', 'Concepto_Upper'], inplace=True)
        if aux_melted.empty: return df_encontrados, aux_df, cfdi_df
        unique_folios = cfdi_to_match['Folio_str'].unique()
        all_folio_matches_dfs = []
        for folio in unique_folios:
            try:
                if not folio or pd.isna(folio) or len(str(folio)) < 3: continue
                folio_escaped = re.escape(str(folio))
                folio_regex = regex_template.format(folio=folio_escaped)
                aux_with_folio = aux_melted[aux_melted['Concepto_Upper'].str.contains(folio_regex, na=False, regex=True)]
                if aux_with_folio.empty: continue
                cfdi_with_folio = cfdi_to_match[cfdi_to_match['Folio_str'] == folio]
                potential_matches = pd.merge(cfdi_with_folio, aux_with_folio, left_on='Monto_Total', right_on='Monto_Match')
                if not potential_matches.empty: all_folio_matches_dfs.append(potential_matches)
            except re.error as re_err: print(f"Error de Regex en match_by_folio_regex con folio {folio}: {re_err}"); continue
            except Exception as e: print(f"Error inesperado en match_by_folio_regex con folio {folio}: {e}"); continue
        if all_folio_matches_dfs:
            df_all_folio_matches = pd.concat(all_folio_matches_dfs, ignore_index=True)
            df_all_folio_matches.drop_duplicates(subset=['ID_AUX'], keep='first', inplace=True)
            df_all_folio_matches.drop_duplicates(subset=['UUID'], keep='first', inplace=True)
            df_merged1 = pd.merge(df_all_folio_matches[['ID_AUX', 'UUID']], aux_df, on='ID_AUX', how='left')
            if 'UUID' not in cfdi_df.columns:
                 print("ERROR CRÍTICO: cfdi_df no tiene columna UUID en match_by_folio_regex")
                 return pd.DataFrame(), aux_df, cfdi_df
            cfdi_df_suffixed = cfdi_df.add_suffix('_cfdi')
            if 'UUID' not in df_merged1.columns:
                 print("ERROR CRÍTICO: df_merged1 no tiene columna UUID en match_by_folio_regex")
                 return pd.DataFrame(), aux_df, cfdi_df
            df_encontrados = pd.merge(df_merged1, cfdi_df_suffixed, left_on='UUID', right_on='UUID_cfdi', how='left')
            df_encontrados.columns = [col.replace('_cfdi','') for col in df_encontrados.columns]
            df_encontrados = df_encontrados.loc[:,~df_encontrados.columns.duplicated()]
            df_encontrados['Match_Type'] = match_type_label
            if 'ID_AUX' not in df_encontrados.columns or 'UUID' not in df_encontrados.columns:
                 print("Advertencia: Faltan ID_AUX o UUID en df_encontrados después del merge.")
                 matched_aux_ids = []
                 matched_cfdi_uuids = []
            else:
                 matched_aux_ids = df_encontrados['ID_AUX'].unique()
                 matched_cfdi_uuids = df_encontrados['UUID'].unique()
            sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(matched_aux_ids)].copy()
            sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(matched_cfdi_uuids)].copy()
            return df_encontrados, sobrantes_aux, sobrantes_cfdi
        return df_encontrados, aux_df, cfdi_df

    def match_by_monto_exacto(cfdi_df, aux_df, date_window_days, match_type_label):
        # (Sin cambios funcionales, asumir que funciona)
        df_encontrados = pd.DataFrame()
        if aux_df.empty or cfdi_df.empty: return df_encontrados, aux_df, cfdi_df
        aux_debe = aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Fecha', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
        aux_haber = aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Fecha', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
        aux_melted = pd.concat([aux_debe, aux_haber])
        if aux_melted.empty: return df_encontrados, aux_df, cfdi_df
        cfdi_df_clean = cfdi_df.dropna(subset=['Emisión', 'Monto_Total'])
        aux_melted_clean = aux_melted.dropna(subset=['Fecha', 'Monto_Match'])
        merged = pd.merge(cfdi_df_clean, aux_melted_clean, left_on='Monto_Total', right_on='Monto_Match', suffixes=('_CFDI', '_AUX'))
        if merged.empty: return df_encontrados, aux_df, cfdi_df
        merged['Emisión'] = pd.to_datetime(merged['Emisión'], errors='coerce')
        merged['Fecha'] = pd.to_datetime(merged['Fecha'], errors='coerce')
        merged.dropna(subset=['Emisión', 'Fecha'], inplace=True)
        if merged.empty: return df_encontrados, aux_df, cfdi_df

        merged['Date_Diff'] = (merged['Emisión'] - merged['Fecha']).abs().dt.days
        if date_window_days is not None:
            matches = merged[merged['Date_Diff'] <= date_window_days].copy()
        else:
            matches = merged.copy()
            # Ordenar solo si date_window_days es None (Pase 6)
            matches.sort_values(by=['UUID', 'Date_Diff'], inplace=True)
        matches.drop_duplicates(subset=['ID_AUX'], keep='first', inplace=True)
        matches.drop_duplicates(subset=['UUID'], keep='first', inplace=True)
        if not matches.empty:
             df_encontrados = pd.merge(matches[['UUID', 'ID_AUX', 'Date_Diff']], aux_df, on='ID_AUX', how='left', suffixes=('_MATCH', '_AUX_ORIG'))
             cfdi_cols_needed = ['UUID', 'Folio', 'Total', 'Emisión', 'Folio_str', 'Monto_Total']
             cfdi_cols_to_merge = [col for col in cfdi_cols_needed if col in cfdi_df.columns]
             if 'UUID' in cfdi_df.columns:
                 df_encontrados = pd.merge(df_encontrados, cfdi_df[cfdi_cols_to_merge], on='UUID', how='left', suffixes=('', '_CFDI_ORIG'))
             else:
                  print("Advertencia: cfdi_df no tiene UUID en match_by_monto_exacto. Añadiendo columnas CFDI con NA.")
                  for col in cfdi_cols_to_merge:
                      if col != 'UUID' and col not in df_encontrados.columns: df_encontrados[col] = pd.NA

             df_encontrados = df_encontrados.loc[:,~df_encontrados.columns.duplicated()]
             df_encontrados = df_encontrados[[col for col in df_encontrados.columns if not col.endswith('_CFDI_ORIG')]]
             df_encontrados['Match_Type'] = match_type_label

             if 'ID_AUX' not in df_encontrados.columns or 'UUID' not in df_encontrados.columns:
                  print("Advertencia: Faltan ID_AUX o UUID en df_encontrados (monto exacto).")
                  matched_aux_ids = []
                  matched_cfdi_uuids = []
             else:
                  matched_aux_ids = df_encontrados['ID_AUX'].unique()
                  matched_cfdi_uuids = df_encontrados['UUID'].unique()

             sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(matched_aux_ids)].copy()
             sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(matched_cfdi_uuids)].copy()
             return df_encontrados, sobrantes_aux, sobrantes_cfdi
        else:
            return df_encontrados, aux_df, cfdi_df

    def match_by_monto_proximo(cfdi_df, aux_df, tolerance, date_window_days, match_type_label):
        # (Sin cambios funcionales, asumir que funciona)
        df_encontrados = pd.DataFrame()
        if aux_df.empty or cfdi_df.empty: return df_encontrados, aux_df, cfdi_df
        aux_debe = aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Fecha', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
        aux_haber = aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Fecha', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
        aux_melted = pd.concat([aux_debe, aux_haber])
        aux_melted.dropna(subset=['Fecha'], inplace=True)
        cfdi_df_clean = cfdi_df.dropna(subset=['Emisión', 'Monto_Total'])
        all_matches_data = []
        matched_aux_ids = set()
        for _, cfdi_row in cfdi_df_clean.iterrows():
            monto_total = cfdi_row['Monto_Total']
            fecha_emision = cfdi_row['Emisión']
            monto_min, monto_max = monto_total - tolerance, monto_total + tolerance
            date_min, date_max = fecha_emision - pd.Timedelta(days=date_window_days), fecha_emision + pd.Timedelta(days=date_window_days)
            mask_monto = (aux_melted['Monto_Match'] >= monto_min) & (aux_melted['Monto_Match'] <= monto_max)
            mask_fecha = (aux_melted['Fecha'] >= date_min) & (aux_melted['Fecha'] <= date_max)
            mask_no_exacto = (aux_melted['Monto_Match'] != monto_total)
            candidates = aux_melted[mask_monto & mask_fecha & mask_no_exacto].copy()
            if not candidates.empty:
                candidates['Monto_Diff'] = (candidates['Monto_Match'] - monto_total).abs()
                candidates.sort_values(by='Monto_Diff', inplace=True)
                for _, aux_candidate in candidates.iterrows():
                    if aux_candidate['ID_AUX'] not in matched_aux_ids:
                        match_data = {'UUID': cfdi_row['UUID'], 'ID_AUX': aux_candidate['ID_AUX'], 'Monto_Diff': aux_candidate['Monto_Diff']}
                        all_matches_data.append(match_data)
                        matched_aux_ids.add(aux_candidate['ID_AUX'])
                        break
        if not all_matches_data: return df_encontrados, aux_df, cfdi_df
        matches = pd.DataFrame(all_matches_data)
        # Merge más cuidadoso
        df_encontrados = pd.merge(matches, aux_df, on='ID_AUX', how='left', suffixes=('_MATCH', '_AUX_ORIG'))
        cfdi_cols_needed = ['UUID', 'Folio', 'Total', 'Emisión', 'Folio_str', 'Monto_Total']
        cfdi_cols_to_merge = [col for col in cfdi_cols_needed if col in cfdi_df.columns]
        if 'UUID' in cfdi_df.columns:
            df_encontrados = pd.merge(df_encontrados, cfdi_df[cfdi_cols_to_merge], on='UUID', how='left', suffixes=('', '_CFDI_ORIG'))
        else:
            print("Advertencia: cfdi_df no tiene UUID en match_by_monto_proximo. Añadiendo cols CFDI con NA.")
            for col in cfdi_cols_to_merge:
                 if col != 'UUID' and col not in df_encontrados.columns: df_encontrados[col] = pd.NA

        df_encontrados = df_encontrados.loc[:,~df_encontrados.columns.duplicated()]
        df_encontrados = df_encontrados[[col for col in df_encontrados.columns if not col.endswith('_CFDI_ORIG')]]
        df_encontrados['Match_Type'] = match_type_label

        if 'ID_AUX' not in df_encontrados.columns or 'UUID' not in df_encontrados.columns:
             print("Advertencia: Faltan ID_AUX o UUID en df_encontrados (monto próximo).")
             matched_aux_ids_set = set() # Usar set para eficiencia
             matched_cfdi_uuids_set = set()
        else:
             # Asegurar que los IDs sean únicos y no nulos antes de convertirlos a set
             matched_aux_ids_set = set(df_encontrados['ID_AUX'].dropna().unique())
             matched_cfdi_uuids_set = set(df_encontrados['UUID'].dropna().unique())


        # Filtrar usando isin que es más eficiente
        sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(matched_aux_ids_set)].copy()
        sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(matched_cfdi_uuids_set)].copy()
        return df_encontrados, sobrantes_aux, sobrantes_cfdi


    # --- Proceso Principal de Conciliación ---
    start_time = time.time() # Medir tiempo total
    all_encontrados_dfs = []
    df_cfdi_orig, msg_cfdi = load_cfdi(cfdi_input_path)
    dashboard_data.append({"Paso": "TOTAL CFDI CARGADOS", "Coincidencias": msg_cfdi.split(': ')[1] if df_cfdi_orig is not None else "Error"})
    if df_cfdi_orig is None: return False, dashboard_data, f"Error al cargar CFDI: {msg_cfdi}"
    df_aux_orig, msg_aux = load_aux(aux_input_path)
    if df_aux_orig is None: return False, dashboard_data, f"Error al cargar AUX: {msg_aux}"

    # --- Pases 0-7 ---
    # (El código de los Pases 0 a 7 es idéntico al anterior)
    exclusion_regex = r'\b(?:' + '|'.join(PALABRAS_EXCLUSION) + r')\b'
    mask_ruido = df_aux_orig['Concepto_Upper'].str.contains(exclusion_regex, na=False, regex=True)
    df_aux_ruido = df_aux_orig[mask_ruido].copy()
    df_aux = df_aux_orig[~mask_ruido].copy()
    dashboard_data.append({"Paso": "TOTAL AUX CARGADOS (Relevantes)", "Coincidencias": len(df_aux)})

    # P1
    if 'UUID_extract' not in df_aux.columns: df_aux['UUID_extract'] = None
    if 'UUID' not in df_cfdi_orig.columns: return False, dashboard_data, "Falta columna UUID en CFDI para Pase 1."
    df_encontrados_p1 = pd.merge(df_aux.dropna(subset=['UUID_extract']), df_cfdi_orig, left_on='UUID_extract', right_on='UUID', suffixes=('_AUX', '_CFDI'))
    if not df_encontrados_p1.empty: df_encontrados_p1['Match_Type'] = 'UUID'
    all_encontrados_dfs.append(df_encontrados_p1)
    matched_aux_ids_p1 = df_encontrados_p1['ID_AUX'].unique() if not df_encontrados_p1.empty else []
    matched_cfdi_uuids_p1 = df_encontrados_p1['UUID'].unique() if not df_encontrados_p1.empty else []
    sobrantes_aux_p1 = df_aux[~df_aux['ID_AUX'].isin(matched_aux_ids_p1)].copy()
    sobrantes_cfdi_p1 = df_cfdi_orig[~df_cfdi_orig['UUID'].isin(matched_cfdi_uuids_p1)].copy()
    dashboard_data.append({"Paso": "100% Fiabilidad (UUID)", "Coincidencias": len(df_encontrados_p1)})

    # P2
    regex_p2 = r'\b{folio}\b'
    df_encontrados_p2, sobrantes_aux_p2, sobrantes_cfdi_p2 = match_by_folio_regex(sobrantes_cfdi_p1, sobrantes_aux_p1, regex_p2, 'Folio+Monto')
    all_encontrados_dfs.append(df_encontrados_p2)
    dashboard_data.append({"Paso": "98% Fiabilidad (Folio Exacto)", "Coincidencias": len(df_encontrados_p2)})

    # P3
    regex_p3 = r'{folio}(?:\b|$)'
    df_encontrados_p3, sobrantes_aux_p3, sobrantes_cfdi_p3 = match_by_folio_regex(sobrantes_cfdi_p2, sobrantes_aux_p2, regex_p3, 'FolioParcial+Monto')
    all_encontrados_dfs.append(df_encontrados_p3)
    dashboard_data.append({"Paso": "95% Fiabilidad (Folio Parcial)", "Coincidencias": len(df_encontrados_p3)})

    # P4
    df_encontrados_p4, sobrantes_aux_p4, sobrantes_cfdi_p4 = match_by_monto_exacto(sobrantes_cfdi_p3, sobrantes_aux_p3, 5, 'Monto+Fecha(5d)')
    all_encontrados_dfs.append(df_encontrados_p4)
    dashboard_data.append({"Paso": "85% Fiabilidad (Monto+Fecha 5d)", "Coincidencias": len(df_encontrados_p4)})

    # P5
    df_encontrados_p5, sobrantes_aux_p5, sobrantes_cfdi_p5 = match_by_monto_exacto(sobrantes_cfdi_p4, sobrantes_aux_p4, 30, 'Monto+Fecha(30d)')
    all_encontrados_dfs.append(df_encontrados_p5)
    dashboard_data.append({"Paso": "70% Fiabilidad (Monto+Fecha 30d)", "Coincidencias": len(df_encontrados_p5)})

    # P6
    df_encontrados_p6, sobrantes_aux_p6, sobrantes_cfdi_p6 = match_by_monto_exacto(sobrantes_cfdi_p5, sobrantes_aux_p5, None, 'Monto(Solo)')
    all_encontrados_dfs.append(df_encontrados_p6)
    dashboard_data.append({"Paso": "60% Fiabilidad (Monto Solo)", "Coincidencias": len(df_encontrados_p6)})

    # P7
    df_encontrados_p7, sobrantes_aux_p7, sobrantes_cfdi_p7 = match_by_monto_proximo(sobrantes_cfdi_p6, sobrantes_aux_p6, TOLERANCIA_MONTO, 30, f'Monto_Proximo(${TOLERANCIA_MONTO})')
    all_encontrados_dfs.append(df_encontrados_p7)
    dashboard_data.append({"Paso": "50% Fiabilidad (Monto Próximo)", "Coincidencias": len(df_encontrados_p7)})


    # --- PASE 8: CONCILIACIÓN POR IA (OPTIMIZADO) ---
    df_encontrados_p8 = pd.DataFrame()
    sobrantes_aux_final = sobrantes_aux_p7.copy()
    sobrantes_cfdi_final = sobrantes_cfdi_p7.copy()
    start_time_ia = time.time() # Medir tiempo solo del Pase 8

    if MODELO_IA is not None and not sobrantes_aux_p7.empty and not sobrantes_cfdi_p7.empty:
        print("\n--- Iniciando Pase 8: Conciliación por IA (Optimizado) ---")
        try:
            # Definir umbrales para pre-filtrado
            UMBRAL_MONTO_IA = 10.0 # Ej: +/- 10 pesos
            UMBRAL_DIAS_IA = 90    # Ej: +/- 90 días

            # Crear copia para evitar SettingWithCopyWarning
            sobrantes_cfdi_p7_copy = sobrantes_cfdi_p7.copy()
            sobrantes_aux_p7_copy = sobrantes_aux_p7.copy()
            sobrantes_cfdi_p7_copy['key'] = 1
            sobrantes_aux_p7_copy['key'] = 1

            # Crear todos los pares posibles
            df_pares_total = pd.merge(sobrantes_cfdi_p7_copy, sobrantes_aux_p7_copy, on='key', suffixes=('_CFDI', '_AUX')).drop('key', axis=1)
            total_pares_inicial = len(df_pares_total)
            print(f"Total de pares iniciales: {total_pares_inicial}")

            if not df_pares_total.empty:
                # 1. Calcular features rápidas (monto, días) para TODOS los pares
                print("Calculando features rápidas (monto, días)...")
                features_rapidas_list = []
                cfdi_cols_orig = df_cfdi_orig.columns
                aux_cols_orig = df_aux_orig.columns

                for idx, row in df_pares_total.iterrows():
                    cfdi_row_data = {col: row.get(f'{col}_CFDI') for col in cfdi_cols_orig}
                    aux_row_data = {col: row.get(f'{col}_AUX') for col in aux_cols_orig}
                    cfdi_row = pd.Series(cfdi_row_data, index=cfdi_cols_orig)
                    aux_row = pd.Series(aux_row_data, index=aux_cols_orig)
                    # Llamar a crear_features SIN calcular similitud
                    features_list.append(crear_features(cfdi_row, aux_row, calcular_similitud=False))

                if features_list:
                    df_features_rapidas = pd.concat(features_list, ignore_index=True)
                    # Añadir features rápidas a df_pares_total para filtrar
                    df_pares_total['diferencia_monto'] = df_features_rapidas['diferencia_monto']
                    df_pares_total['diferencia_dias'] = df_features_rapidas['diferencia_dias']

                    # 2. Pre-filtrar pares basado en umbrales
                    mask_monto = df_pares_total['diferencia_monto'] <= UMBRAL_MONTO_IA
                    mask_dias = df_pares_total['diferencia_dias'] <= UMBRAL_DIAS_IA
                    df_pares_filtrados = df_pares_total[mask_monto & mask_dias].copy()
                    total_pares_filtrados = len(df_pares_filtrados)
                    print(f"Pares después de filtrar por monto (<={UMBRAL_MONTO_IA}) y días (<={UMBRAL_DIAS_IA}): {total_pares_filtrados}")

                    if not df_pares_filtrados.empty:
                        # 3. Calcular feature lenta (similitud_folio) SOLO para pares filtrados
                        print(f"Calculando similitud de folio para {total_pares_filtrados} pares filtrados...")
                        similitudes = []
                        contador_progreso = 0
                        paso_progreso = max(1, total_pares_filtrados // 10)

                        for idx, row in df_pares_filtrados.iterrows():
                            folio_cfdi = str(row.get('Folio_str_CFDI', ''))
                            concepto_aux = str(row.get('Concepto_Upper_AUX', ''))
                            similitudes.append(fuzz.token_set_ratio(folio_cfdi or '', concepto_aux or ''))

                            contador_progreso += 1
                            if contador_progreso % paso_progreso == 0:
                                print(f"    ... Calculando similitud par {contador_progreso} de {total_pares_filtrados} ({int(contador_progreso/total_pares_filtrados*100)}%)")

                        print(f"    ... Cálculo de similitud completado.")
                        df_pares_filtrados['similitud_folio'] = similitudes
                        # Calcular 'es_mismo_monto' para los filtrados
                        df_pares_filtrados['es_mismo_monto'] = (df_pares_filtrados['diferencia_monto'] < 0.01).astype(int)

                        # 4. Seleccionar las features correctas para la predicción
                        df_features_finales = df_pares_filtrados[IA_FEATURES].copy()
                        # Asegurar tipos y manejar NaNs
                        df_features_finales.fillna({'diferencia_monto': 99999, 'diferencia_dias': 999, 'similitud_folio': 0, 'es_mismo_monto': 0}, inplace=True)
                        df_features_finales = df_features_finales.astype(float)


                        # 5. Predecir probabilidades SOLO para pares filtrados
                        print("Prediciendo probabilidades con IA...")
                        probabilidades = MODELO_IA.predict_proba(df_features_finales)[:, 1]
                        df_pares_filtrados['IA_Probabilidad'] = probabilidades

                        # 6. Seleccionar y de-duplicar matches de IA
                        umbral_confianza = 0.90
                        df_matches_ia = df_pares_filtrados[df_pares_filtrados['IA_Probabilidad'] >= umbral_confianza].copy()
                        df_matches_ia = df_matches_ia.sort_values(by='IA_Probabilidad', ascending=False)
                        df_matches_ia.drop_duplicates(subset='UUID_CFDI', keep='first', inplace=True)
                        df_matches_ia.drop_duplicates(subset='ID_AUX_AUX', keep='first', inplace=True)

                        if not df_matches_ia.empty:
                            # 7. Reconstruir DataFrame de encontrados (igual que antes)
                            cols_ia_needed = ['UUID_CFDI', 'ID_AUX_AUX', 'IA_Probabilidad']
                            # Añadir Monto_Diff si existe en df_matches_ia (puede venir de match_by_monto_proximo si se reutiliza)
                            if 'Monto_Diff' in df_matches_ia.columns: cols_ia_needed.append('Monto_Diff')
                            df_to_merge_ia = df_matches_ia[cols_ia_needed]
                            df_encontrados_p8 = pd.merge(df_to_merge_ia, df_cfdi_orig.add_suffix('_cfdi_orig'), left_on='UUID_CFDI', right_on='UUID_cfdi_orig', how='left')
                            df_encontrados_p8 = pd.merge(df_encontrados_p8, df_aux_orig.add_suffix('_aux_orig'), left_on='ID_AUX_AUX', right_on='ID_AUX_aux_orig', how='left')
                            rename_map_p8 = {f'{col}_cfdi_orig': col for col in df_cfdi_orig.columns}
                            rename_map_p8.update({f'{col}_aux_orig': col for col in df_aux_orig.columns})
                            df_encontrados_p8.rename(columns=rename_map_p8, inplace=True)
                            df_encontrados_p8.drop(columns=['UUID_CFDI', 'ID_AUX_AUX', 'UUID_cfdi_orig', 'ID_AUX_aux_orig'], inplace=True, errors='ignore')
                            df_encontrados_p8 = df_encontrados_p8.loc[:,~df_encontrados_p8.columns.duplicated()]
                            df_encontrados_p8['Match_Type'] = 'IA_Prediccion (>' + str(int(umbral_confianza*100)) + '%)'
                            all_encontrados_dfs.append(df_encontrados_p8)
                            matched_aux_ids_p8 = df_encontrados_p8['ID_AUX'].unique()
                            matched_cfdi_uuids_p8 = df_encontrados_p8['UUID'].unique()
                            sobrantes_aux_final = sobrantes_aux_p7[~sobrantes_aux_p7['ID_AUX'].isin(matched_aux_ids_p8)].copy()
                            sobrantes_cfdi_final = sobrantes_cfdi_p7[~sobrantes_cfdi_p7['UUID'].isin(matched_cfdi_uuids_p8)].copy()
                            print(f"Pase 8 (IA) completado: {len(df_encontrados_p8)} coincidencias encontradas.")
                        else:
                            print("Pase 8 (IA): Ningún par filtrado superó el umbral de confianza.")
                    else:
                        print("Pase 8 (IA): Ningún par superó los umbrales iniciales de monto y días.")
                else:
                    print("Pase 8 (IA): No se pudieron generar features rápidas.")
            else:
                 print("Pase 8 (IA): No hay pares iniciales para analizar.")
        except Exception as e:
            import traceback
            print(f"ERROR DETALLADO en Pase 8 (IA): {traceback.format_exc()}")
            sobrantes_aux_final = sobrantes_aux_p7.copy() # Mantener sobrantes anteriores en caso de error
            sobrantes_cfdi_final = sobrantes_cfdi_p7.copy()
    else:
        print("Pase 8 (IA) omitido. Modelo no cargado o no hay sobrantes.")

    end_time_ia = time.time()
    print(f"Tiempo ejecución Pase 8 (IA): {end_time_ia - start_time_ia:.2f} segundos")
    dashboard_data.append({"Paso": "90-99% Fiabilidad (IA)", "Coincidencias": len(df_encontrados_p8)})

    # --- Resultados Finales y Guardado ---
    # (El código de guardado en Excel es idéntico al anterior)
    dashboard_data.append({"Paso": "---", "Coincidencias": "---"})
    if not all_encontrados_dfs or all(df.empty for df in all_encontrados_dfs):
        df_encontrados_final = pd.DataFrame()
        total_conciliado = 0
    else:
        all_encontrados_dfs_validos = [df for df in all_encontrados_dfs if not df.empty]
        if not all_encontrados_dfs_validos:
             df_encontrados_final = pd.DataFrame()
             total_conciliado = 0
        else:
            try:
                # Determinar todas las columnas posibles para alinear
                all_cols = set()
                for df in all_encontrados_dfs_validos: all_cols.update(df.columns)
                all_cols = sorted(list(all_cols)) # Ordenar para consistencia

                dfs_alineados = []
                for df in all_encontrados_dfs_validos:
                    # Añadir columnas faltantes con None o NaN
                    for col in all_cols:
                        if col not in df.columns:
                            df[col] = pd.NA
                    # Reordenar y convertir a object para evitar errores de tipo
                    dfs_alineados.append(df[all_cols].astype(object))

                df_encontrados_final = pd.concat(dfs_alineados, ignore_index=True, sort=False)

                if 'UUID' in df_encontrados_final.columns:
                     df_encontrados_final['UUID'] = df_encontrados_final['UUID'].astype(str)
                     valid_uuids = df_encontrados_final['UUID'].str.match(r'^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$', na=False)
                     total_conciliado = df_encontrados_final.loc[valid_uuids & df_encontrados_final['UUID'].notna(), 'UUID'].nunique()
                else:
                     total_conciliado = 0
            except Exception as e:
                 import traceback
                 print(f"Error detallado al concatenar resultados finales: {traceback.format_exc()}")
                 df_encontrados_final = pd.DataFrame()
                 total_conciliado = 0

    dashboard_data.append({"Paso": "TOTAL CONCILIADO", "Coincidencias": total_conciliado})
    dashboard_data.append({"Paso": "SOBRANTES CFDI", "Coincidencias": len(sobrantes_cfdi_final)})
    dashboard_data.append({"Paso": "SOBRANTES AUX", "Coincidencias": len(sobrantes_aux_final)})

    print(f"\n--- Generando archivo de salida Excel: {output_file_path} ---")
    try:
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            # Hoja 1: DASHBOARD
            df_dashboard = pd.DataFrame(dashboard_data)
            df_dashboard.columns = ["Nivel de Fiabilidad / Concepto", "Conciliaciones / Cantidad"]
            consejo_ia_texto = generar_consejo_ia()
            df_consejo = pd.DataFrame({"CONSEJO DE LA INTELIGENCIA ARTIFICIAL": [consejo_ia_texto], "": [""]})
            df_dashboard.to_excel(writer, sheet_name='Dashboard', index=False, startrow=1)
            df_consejo.to_excel(writer, sheet_name='Dashboard', index=False, startrow=len(df_dashboard) + 4)

            # --- Limpieza de columnas para hojas de Pases ---
            if not df_encontrados_final.empty:
                aux_cols_final = ['Fecha', 'Concepto', 'Debe', 'Haber', 'Tipo', 'Numero', 'ID_AUX']
                cfdi_cols_final = ['UUID', 'Folio', 'Total', 'Emisión']
                extra_cols = ['Match_Type', 'Monto_Diff', 'IA_Probabilidad']
                # Limpiar nombres consistentemente
                df_encontrados_final.columns = [col.replace('_AUX_orig','').replace('_AUX','').replace('_CFDI','') for col in df_encontrados_final.columns]
                df_encontrados_final = df_encontrados_final.loc[:,~df_encontrados_final.columns.duplicated()]

                final_cols = extra_cols + cfdi_cols_final + aux_cols_final
                final_cols_real_existing = [col for col in final_cols if col in df_encontrados_final.columns]

                df_encontrados_final_clean = df_encontrados_final[final_cols_real_existing].copy()

                for col in ['Total', 'Debe', 'Haber', 'Monto_Diff', 'IA_Probabilidad']:
                     if col in df_encontrados_final_clean.columns:
                         df_encontrados_final_clean[col] = pd.to_numeric(df_encontrados_final_clean[col], errors='coerce')
                for col in ['Fecha', 'Emisión']:
                     if col in df_encontrados_final_clean.columns:
                         df_encontrados_final_clean[col] = pd.to_datetime(df_encontrados_final_clean[col], errors='coerce')

                if 'Match_Type' in df_encontrados_final_clean.columns:
                    df_encontrados_final_clean['Match_Type'].fillna('Desconocido', inplace=True)
                    df_encontrados_final_clean['Match_Type'] = df_encontrados_final_clean['Match_Type'].astype(str)
                    df_encontrados_final_clean.sort_values(by='Match_Type', inplace=True, na_position='last')
            else:
                 df_encontrados_final_clean = pd.DataFrame()

            # --- Hojas 2-6: Pases ---
            tipos_alta = ['UUID', 'Folio+Monto', 'FolioParcial+Monto']
            df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_alta)].to_excel(writer, sheet_name='Confianza_Alta_95_100', index=False)
            tipos_media = ['Monto+Fecha(5d)']
            df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_media)].to_excel(writer, sheet_name='Confianza_Media_80', index=False)
            tipos_baja = ['Monto+Fecha(30d)', 'Monto(Solo)']
            df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_baja)].to_excel(writer, sheet_name='Confianza_Baja_Revisar', index=False)
            tipos_proximo = [col for col in df_encontrados_final_clean['Match_Type'].unique() if 'Monto_Proximo' in str(col)]
            df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_proximo)].to_excel(writer, sheet_name='Confianza_Muy_Baja_Proximidad', index=False)

            # --- Hoja 7: IA (NUEVA) ---
            tipos_ia = [col for col in df_encontrados_final_clean['Match_Type'].unique() if 'IA_Prediccion' in str(col)]
            df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_ia)].to_excel(writer, sheet_name='Confianza_IA_Prediccion', index=False)

            # --- Hojas 8-10: Sobrantes y Ruido ---
            cols_to_drop_aux = ['Concepto_Upper', 'Monto_Debe', 'Monto_Haber', 'UUID_extract', 'key']
            sobrantes_aux_final.drop(columns=cols_to_drop_aux, errors='ignore').to_excel(writer, sheet_name='No_Coincidencias_AUX', index=False)
            cols_to_drop_cfdi = ['Folio_str', 'Monto_Total', 'key']
            sobrantes_cfdi_final.drop(columns=cols_to_drop_cfdi, errors='ignore').to_excel(writer, sheet_name='Sobrantes_CFDI', index=False)
            df_aux_ruido.drop(columns=cols_to_drop_aux, errors='ignore').to_excel(writer, sheet_name='AUX_Filtrado_No_Requerido', index=False)

        end_time = time.time() # Medir tiempo total
        print(f"\n¡Éxito! Archivo Excel 'TOTAL' generado en {end_time - start_time:.2f} segundos.")
        return True, dashboard_data, consejo_ia_texto

    except Exception as e:
        import traceback
        print(f"\nERROR CRÍTICO AL GUARDAR el archivo Excel: {traceback.format_exc()}")
        dashboard_data.append({"Paso": "ERROR AL GUARDAR", "Coincidencias": str(e)})
        return False, dashboard_data, f"Error al guardar Excel: {e}"

# ===========================================================================
# === INICIO DEL CÓDIGO DEL SERVIDOR (Back-End API) ===
# ===========================================================================
# (Rutas de Login, Logout, Index, Procesar, Descargar - Sin cambios)

@app.before_request
def require_login():
    if request.path.startswith('/login') or request.path.startswith('/static'):
        return
    if 'logged_in' not in session:
        return redirect(url_for('login'))
    pass


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        pin_ingresado = request.form.get('pin')
        if pin_ingresado == PIN_SECRETO:
            session['logged_in'] = True
            print("Inicio de sesión exitoso.")
            return redirect(url_for('index'))
        else:
            print(f"Intento de login fallido con PIN: {pin_ingresado}")
            flash('PIN incorrecto. Inténtalo de nuevo.', 'error')
            return redirect(url_for('login'))
    if 'logged_in' in session:
         return redirect(url_for('index'))
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    print("Sesión cerrada.")
    flash('Has cerrado la sesión.', 'success')
    return redirect(url_for('login'))

@app.route('/')
def index():
    # Protegido por @app.before_request
    print("Solicitud recibida en /. Sirviendo el front-end (index.html).")
    return render_template('index.html')

@app.route('/procesar', methods=['POST'])
def procesar_archivo():
    # Protegido por @app.before_request
    print("Recibida solicitud en /procesar")
    if 'archivo_cfdi' not in request.files or 'archivo_aux' not in request.files:
        return jsonify({"error": "Faltan archivos. Se requieren 'archivo_cfdi' y 'archivo_aux'."}), 400
    file_cfdi = request.files['archivo_cfdi']
    file_aux = request.files['archivo_aux']
    if file_cfdi.filename == '' or file_aux.filename == '':
        return jsonify({"error": "No se seleccionó uno o más archivos."}), 400

    cfdi_name = "".join(c for c in file_cfdi.filename if c.isalnum() or c in (' ', '.', '_')).rstrip()
    aux_name = "".join(c for c in file_aux.filename if c.isalnum() or c in (' ', '.', '_')).rstrip()
    cfdi_name = cfdi_name.replace('.xlsx', '').replace(' ', '_')
    aux_name = aux_name.replace('.xlsx', '').replace(' ', '_')
    unique_id = str(uuid.uuid4())[:8]
    output_filename = f"Conciliacion_{unique_id}.xlsx"

    cfdi_input_path = UPLOAD_FOLDER / f"{unique_id}_cfdi.xlsx"
    aux_input_path = UPLOAD_FOLDER / f"{unique_id}_aux.xlsx"
    output_path = OUTPUT_FOLDER / output_filename
    try:
        file_cfdi.save(cfdi_input_path)
        file_aux.save(aux_input_path)
        success, dashboard_data, consejo_ia = ejecutar_conciliacion(
            str(cfdi_input_path), str(aux_input_path), str(output_path)
        )
        if success:
            return jsonify({
                "message": "¡Éxito! Archivo procesado y listo para descargar.",
                "dashboard": dashboard_data,
                "consejo": consejo_ia,
                "downloadFile": output_filename
            })
        else:
             error_msg = consejo_ia
             print(f"Error durante la ejecución o guardado: {error_msg}")
             return jsonify({
                "error": f"Falló el procesamiento o guardado. Revisa los archivos o los logs.",
                "dashboard": dashboard_data,
                "consejo": consejo_ia
            }), 500
    except Exception as e:
        import traceback
        print(f"ERROR CRÍTICO en /procesar: {traceback.format_exc()}")
        error_detail = str(e)
        if isinstance(e, MemoryError): error_detail = "Error de memoria."
        elif "ValueError: All arrays must be of the same length" in error_detail: error_detail = "Error interno de IA."
        elif isinstance(e, KeyError): error_detail = f"Error de columna faltante: {e}. Revisa tus archivos Excel."
        elif "could not convert string to float" in error_detail: error_detail = "Error de tipo de dato en IA. Revisa los datos de entrada."
        return jsonify({"error": f"Error interno del servidor: {error_detail}"}), 500
    finally:
        for f_path in [cfdi_input_path, aux_input_path]:
            try:
                if f_path and os.path.exists(f_path): os.remove(f_path)
            except Exception as e_clean:
                print(f"Error limpiando archivo temporal {f_path}: {e_clean}")

@app.route('/descargar/<path:nombre_archivo>')
def descargar_archivo(nombre_archivo):
    # Protegido por @app.before_request
    print(f"Solicitud de descarga recibida para: {nombre_archivo}")
    try:
        safe_path = Path(OUTPUT_FOLDER).resolve()
        file_path = (safe_path / nombre_archivo).resolve()
        if not str(file_path).startswith(str(safe_path)):
             print(f"Error: Intento de Path Traversal bloqueado para {nombre_archivo}")
             abort(400)
        if not os.path.exists(file_path):
            print(f"Error 404: Archivo no encontrado en descarga: {file_path}")
            abort(404)
        response = make_response(send_file(file_path, as_attachment=True))
        return response
    except Exception as e:
        print(f"Error en la ruta de descarga /descargar: {e}")
        abort(500)

# ===========================================================================
# === LÓGICA DE INICIO DEL SERVIDOR (MODIFICADO PARA EJECUTAR DIRECTAMENTE) ===
# ===========================================================================

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 1)); IP = s.getsockname()[0]
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
    open_browser()
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
    print("Para un uso más seguro en red, considera usar 'waitress'")
    print("o cambia 'debug=True' a 'debug=False'.")
    print("************************************************************")

    # Ejecutar con el servidor de desarrollo de Flask
    # host='0.0.0.0' permite acceso desde la red
    # debug=True habilita recarga automática y depurador (¡riesgo de seguridad en red!)
    app.run(debug=True, port=5001, host='0.0.0.0')