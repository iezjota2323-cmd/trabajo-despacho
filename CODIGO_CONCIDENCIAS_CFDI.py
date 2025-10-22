import pandas as pd
import numpy as np
import re
import sys
import csv
import os
import openpyxl  # Necesario para leer y escribir archivos .xlsx
from pathlib import Path

# --- Configuración de Rutas ---
# Esta es la ruta exacta que proporcionaste
input_file_path = '/Users/luispaniaguapalacios/Downloads/LUIS.xlsx'

# Nombre del archivo de salida (se guardará en la misma carpeta)
output_file_path = '/Users/luispaniaguapalacios/Downloads/LUIS_CONCILIACION_TOTAL.xlsx'

# --- CONFIGURACIÓN DE PASES ---
# Tolerancia en pesos para el Pase 7 (errores de dedo)
TOLERANCIA_MONTO = 1.00 # +/- 1 peso
# Palabras clave para el Pase 0 (ruido contable)
PALABRAS_EXCLUSION = ['NOMINA', 'IMSS', 'SAT', 'INFONAVIT', 'COMISION', 'TRASPASO', 'IMPUESTO']


print(f"Archivo de entrada: {input_file_path}")
print(f"Archivo de salida: {output_file_path}")

# Aumentar el límite del campo CSV (se mantiene por si acaso)
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    print("Advertencia: No se pudo establecer el límite máximo de CSV.")
    pass 

print("Iniciando el script de conciliación TOTAL (7 Pases)...")

# --- Verificación de archivo de entrada ---
if not os.path.exists(input_file_path):
    print(f"ERROR CRÍTICO: Archivo no encontrado en: {input_file_path}")
    print("Por favor, asegúrate de que el archivo 'LUIS.xlsx' exista en esa ruta exacta.")
    sys.exit()

print("Archivo de entrada encontrado. Procediendo a la carga.")

# --- Funciones de Carga y Limpieza (Modificadas para leer .xlsx) ---

def load_cfdi(filename):
    """Carga y limpia la hoja 'CFDI REC PROV' del archivo Excel."""
    print("Cargando hoja 'CFDI REC PROV'...")
    try:
        # Leer de Excel, Hoja 'CFDI REC PROV'
        df = pd.read_excel(filename, sheet_name='CFDI REC PROV', header=4, engine='openpyxl')
        
        cols_to_keep = ['UUID', 'Folio', 'Total', 'Emisión']
        
        if not all(col in df.columns for col in cols_to_keep):
            print(f"ERROR: La hoja 'CFDI REC PROV' debe contener las columnas: {cols_to_keep}")
            return None
            
        df_clean = df[cols_to_keep].copy()
        df_clean['Total'] = pd.to_numeric(df_clean['Total'], errors='coerce')
        df_clean['Emisión'] = pd.to_datetime(df_clean['Emisión'], errors='coerce')
        df_clean['UUID'] = df_clean['UUID'].astype(str).str.upper().str.strip()
        df_clean['Folio_str'] = df_clean['Folio'].astype(str).str.strip().str.upper().replace('NAN', np.nan)
        df_clean['Monto_Total'] = round(df_clean['Total'], 2)
        df_clean.dropna(subset=['UUID', 'Total', 'Emisión'], inplace=True)
        print(f"CFDI cargado: {len(df_clean)} filas válidas.")
        return df_clean
        
    except Exception as e:
        print(f"Error fatal al cargar la hoja 'CFDI REC PROV': {e}")
        print("Asegúrate de que la hoja exista y se llame exactamente 'CFDI REC PROV'.")
        return None

def load_aux(filename):
    """Carga y limpia la hoja 'AUX' del archivo Excel."""
    print("Cargando hoja 'AUX'...")
    try:
        # CORREGIDO: Quitado 'skipinitialspace=True'
        df = pd.read_excel(filename, sheet_name='AUX', header=0, engine='openpyxl')
        
        df.columns = df.columns.str.strip()
        
        df_clean = df[~df['Tipo'].astype(str).str.startswith('-', na=True) & df['Concepto'].notna()].copy()
        
        cols_to_keep = ['Tipo', 'Numero', 'Fecha', 'Concepto', 'Debe', 'Haber']
        if not all(col in df.columns for col in cols_to_keep):
            print(f"ERROR: La hoja 'AUX' debe contener las columnas: {cols_to_keep}")
            return None

        df_clean['Fecha'] = pd.to_datetime(df_clean['Fecha'], errors='coerce', dayfirst=True) 
        df_clean['Debe'] = pd.to_numeric(df_clean['Debe'], errors='coerce').fillna(0)
        df_clean['Haber'] = pd.to_numeric(df_clean['Haber'], errors='coerce').fillna(0)
        df_clean['ID_AUX'] = range(len(df_clean))
        df_clean['Concepto_Upper'] = df_clean['Concepto'].astype(str).str.upper()
        df_clean['Monto_Debe'] = round(df_clean['Debe'], 2)
        df_clean['Monto_Haber'] = round(df_clean['Haber'], 2)
        
        uuid_pattern = re.compile(r'([0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})')
        df_clean['UUID_extract'] = df_clean['Concepto_Upper'].apply(lambda x: (uuid_pattern.search(x) or [None])[0])
        
        print(f"AUX cargado: {len(df_clean)} filas de movimientos válidas.")
        return df_clean

    except Exception as e:
        print(f"Error fatal al cargar la hoja 'AUX': {e}")
        print("Asegúrate de que la hoja exista y se llame exactamente 'AUX'.")
        return None

# --- Función Auxiliar para Pases 2 y 3 ---
def match_by_folio_regex(cfdi_df, aux_df, regex_template, match_type_label):
    """Función optimizada para cruzar por folio + monto usando un regex"""
    
    cfdi_to_match = cfdi_df.dropna(subset=['Folio_str', 'Monto_Total']).copy()
    aux_to_match = aux_df.copy()
    
    df_encontrados = pd.DataFrame()
    
    if cfdi_to_match.empty or aux_to_match.empty:
        return df_encontrados, aux_df, cfdi_df

    aux_debe = aux_to_match[aux_to_match['Monto_Debe'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
    aux_haber = aux_to_match[aux_to_match['Monto_Haber'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
    aux_melted = pd.concat([aux_debe, aux_haber])
    aux_melted.dropna(subset=['Monto_Match', 'Concepto_Upper'], inplace=True)

    if aux_melted.empty:
        return df_encontrados, aux_df, cfdi_df

    unique_folios = cfdi_to_match['Folio_str'].unique()
    all_folio_matches_dfs = []
    
    # print(f"Iterando sobre {len(unique_folios)} folios únicos para {match_type_label}...")
    
    for folio in unique_folios:
        # Aplicar el template de regex
        folio_regex = regex_template.format(folio=re.escape(folio))
        
        aux_with_folio = aux_melted[aux_melted['Concepto_Upper'].str.contains(folio_regex, na=False, regex=True)]
        if aux_with_folio.empty:
            continue
        
        cfdi_with_folio = cfdi_to_match[cfdi_to_match['Folio_str'] == folio]
        
        potential_matches = pd.merge(
            cfdi_with_folio,
            aux_with_folio,
            left_on='Monto_Total',
            right_on='Monto_Match'
        )
        if not potential_matches.empty:
            all_folio_matches_dfs.append(potential_matches)

    if all_folio_matches_dfs:
        df_all_folio_matches = pd.concat(all_folio_matches_dfs, ignore_index=True)
        df_all_folio_matches.drop_duplicates(subset=['ID_AUX'], keep='first', inplace=True)
        df_all_folio_matches.drop_duplicates(subset=['UUID'], keep='first', inplace=True)
        
        df_encontrados = pd.merge(
            df_all_folio_matches[['ID_AUX', 'UUID']],
            aux_df,
            on='ID_AUX',
            how='left'
        ).merge(
            cfdi_df,
            on='UUID',
            how='left',
            suffixes=('_AUX', '_CFDI')
        )
        df_encontrados['Match_Type'] = match_type_label
        
        # Identificar sobrantes
        matched_aux_ids = df_encontrados['ID_AUX'].unique()
        matched_cfdi_uuids = df_encontrados['UUID'].unique()
        
        sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(matched_aux_ids)].copy()
        sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(matched_cfdi_uuids)].copy()
        return df_encontrados, sobrantes_aux, sobrantes_cfdi

    return df_encontrados, aux_df, cfdi_df

# --- Función Auxiliar para Pases 4, 5 y 6 (Monto Exacto) ---
def match_by_monto_exacto(cfdi_df, aux_df, date_window_days, match_type_label):
    """Función para cruzar por monto exacto y un rango de días (o sin rango)"""
    
    df_encontrados = pd.DataFrame()
    
    if aux_df.empty or cfdi_df.empty:
        return df_encontrados, aux_df, cfdi_df

    aux_debe = aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Fecha', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
    aux_haber = aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Fecha', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
    aux_melted = pd.concat([aux_debe, aux_haber])

    if aux_melted.empty:
        return df_encontrados, aux_df, cfdi_df

    # Unir por monto exacto
    merged = pd.merge(
        cfdi_df,
        aux_melted,
        left_on='Monto_Total',
        right_on='Monto_Match',
        suffixes=('_CFDI', '_AUX')
    )

    if date_window_days is not None:
        # Pases 4 y 5
        merged['Date_Diff'] = (merged['Emisión'] - merged['Fecha']).abs().dt.days
        # Filtrar por diferencia de días
        matches = merged[merged['Date_Diff'] <= date_window_days].copy()
    else:
        # Es el Pase 6 (Monto Solo), no filtrar por fecha
        matches = merged.copy()
        
        # --- INICIO DE LA CORRECCIÓN ---
        # Calcular Date_Diff en el DataFrame 'matches', no en 'merged'
        matches['Date_Diff'] = (matches['Emisión'] - matches['Fecha']).abs().dt.days 
        # --- FIN DE LA CORRECCIÓN ---
        
        # Priorizar la fecha más cercana en caso de múltiples matches
        matches.sort_values(by=['UUID', 'Date_Diff'], inplace=True)

    
    # De-duplicar: un movimiento de AUX solo puede usarse una vez
    matches.drop_duplicates(subset=['ID_AUX'], keep='first', inplace=True)
    # De-duplicar: un CFDI solo puede usarse una vez
    matches.drop_duplicates(subset=['UUID'], keep='first', inplace=True)
    
    if not matches.empty:
        df_encontrados = pd.merge(
            matches,
            aux_df.drop(columns=['UUID_extract'], errors='ignore'),
            on='ID_AUX',
            suffixes=('_CFDI_MATCHED', '_AUX_ORIG')
        )
        df_encontrados['Match_Type'] = match_type_label
        
        # Identificar sobrantes finales
        matched_aux_ids = df_encontrados['ID_AUX'].unique()
        matched_cfdi_uuids = df_encontrados['UUID'].unique()
        
        sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(matched_aux_ids)].copy()
        sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(matched_cfdi_uuids)].copy()
        return df_encontrados, sobrantes_aux, sobrantes_cfdi
        
    else:
        # print(f"No se encontraron coincidencias en el Pase '{match_type_label}'.")
        return df_encontrados, aux_df, cfdi_df

# --- CORREGIDO: Función Auxiliar para Pase 7 (Monto Próximo Eficiente) ---
def match_by_monto_proximo(cfdi_df, aux_df, tolerance, date_window_days, match_type_label):
    """Función EFICIENTE para cruzar por monto CON TOLERANCIA"""
    
    df_encontrados = pd.DataFrame()
    
    if aux_df.empty or cfdi_df.empty:
        print("No hay datos para el cruce por proximidad.")
        return df_encontrados, aux_df, cfdi_df

    print(f"Iniciando cruce por proximidad (Tolerancia: +/- ${tolerance})...")
    print(f"Esto puede tardar unos minutos...")

    aux_debe = aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Fecha', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'})
    aux_haber = aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Fecha', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
    aux_melted = pd.concat([aux_debe, aux_haber])
    
    # Quitar nulos en Fecha para evitar errores
    aux_melted.dropna(subset=['Fecha'], inplace=True)
    cfdi_df_clean = cfdi_df.dropna(subset=['Emisión'])
    
    # Listas para guardar los matches
    all_matches_data = []
    matched_aux_ids = set()
    
    # Iterar sobre el DF más pequeño (CFDI)
    for _, cfdi_row in cfdi_df_clean.iterrows():
        
        monto_total = cfdi_row['Monto_Total']
        fecha_emision = cfdi_row['Emisión']

        # 1. Definir rangos de búsqueda
        monto_min = monto_total - tolerance
        monto_max = monto_total + tolerance
        date_min = fecha_emision - pd.Timedelta(days=date_window_days)
        date_max = fecha_emision + pd.Timedelta(days=date_window_days)

        # 2. Filtrar candidatos en AUX (¡Mucho más rápido!)
        mask_monto = (aux_melted['Monto_Match'] >= monto_min) & (aux_melted['Monto_Match'] <= monto_max)
        mask_fecha = (aux_melted['Fecha'] >= date_min) & (aux_melted['Fecha'] <= date_max)
        mask_no_exacto = (aux_melted['Monto_Match'] != monto_total) # No re-amarar montos exactos
        
        candidates = aux_melted[mask_monto & mask_fecha & mask_no_exacto].copy()
        
        if not candidates.empty:
            # 3. Encontrar el MEJOR candidato (menor diferencia de monto)
            candidates['Monto_Diff'] = (candidates['Monto_Match'] - monto_total).abs()
            candidates.sort_values(by='Monto_Diff', inplace=True)
            
            # 4. Seleccionar el mejor, si no ha sido usado ya
            for _, aux_candidate in candidates.iterrows():
                if aux_candidate['ID_AUX'] not in matched_aux_ids:
                    best_match = aux_candidate
                    
                    # Guardar el match
                    match_data = {
                        'UUID': cfdi_row['UUID'],
                        'ID_AUX': best_match['ID_AUX'],
                        'Monto_Diff': best_match['Monto_Diff'],
                    }
                    all_matches_data.append(match_data)
                    
                    # Marcar como usado
                    matched_aux_ids.add(best_match['ID_AUX'])
                    # Romper el loop interno, pasar al siguiente CFDI
                    break 

    if not all_matches_data:
        print("No se encontraron coincidencias por proximidad.")
        return df_encontrados, aux_df, cfdi_df

    # Convertir los matches en un DataFrame
    matches = pd.DataFrame(all_matches_data)
    
    # Unir con los datos originales para obtener la información completa
    df_encontrados = pd.merge(
        matches,
        aux_df,
        on='ID_AUX',
        suffixes=('_MATCH', '_AUX_ORIG')
    ).merge(
        cfdi_df,
        on='UUID',
        suffixes=('', '_CFDI_ORIG')
    )
    df_encontrados['Match_Type'] = match_type_label
    
    # Identificar sobrantes finales
    final_matched_aux_ids = df_encontrados['ID_AUX'].unique()
    final_matched_cfdi_uuids = df_encontrados['UUID'].unique()
    
    sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(final_matched_aux_ids)].copy()
    sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(final_matched_cfdi_uuids)].copy()
    
    return df_encontrados, sobrantes_aux, sobrantes_cfdi
    
# --- Proceso Principal de Conciliación ---

all_encontrados_dfs = []
df_cfdi_orig = load_cfdi(input_file_path)
df_aux_orig = load_aux(input_file_path)

if df_cfdi_orig is None or df_aux_orig is None:
    print("\nNo se pudieron cargar una o más hojas. Terminando script.")
else:
    # --- PASE 0: Filtro de Ruido Contable (Eficiencia) ---
    print("\n--- Iniciando Pase 0: Filtro de Ruido Contable ---")
    
    # --- CORRECCIÓN (Quitar warnings) ---
    # Usar (?:...) para crear un "non-capturing group"
    exclusion_regex = r'\b(?:' + '|'.join(PALABRAS_EXCLUSION) + r')\b'
    
    mask_ruido = df_aux_orig['Concepto_Upper'].str.contains(exclusion_regex, na=False, regex=True)
    
    df_aux_ruido = df_aux_orig[mask_ruido].copy()
    # Continuar la conciliación SÓLO con los movimientos que NO son ruido
    df_aux = df_aux_orig[~mask_ruido].copy()
    
    print(f"Pase 0 completado: {len(df_aux_ruido)} movimientos de 'ruido' separados.")
    print(f"Continuando conciliación con {len(df_aux)} movimientos relevantes.")

    # --- PASE 1: Cruce por UUID (Alta Precisión) ---
    print("\n--- Iniciando Pase 1: Cruce por UUID ---")
    df_encontrados_p1 = pd.merge(
        df_aux.dropna(subset=['UUID_extract']),
        df_cfdi_orig,
        left_on='UUID_extract',
        right_on='UUID',
        suffixes=('_AUX', '_CFDI')
    )
    df_encontrados_p1['Match_Type'] = 'UUID'
    all_encontrados_dfs.append(df_encontrados_p1)
    
    matched_aux_ids_p1 = df_encontrados_p1['ID_AUX'].unique()
    matched_cfdi_uuids_p1 = df_encontrados_p1['UUID'].unique()
    
    sobrantes_aux_p1 = df_aux[~df_aux['ID_AUX'].isin(matched_aux_ids_p1)].copy()
    sobrantes_cfdi_p1 = df_cfdi_orig[~df_cfdi_orig['UUID'].isin(matched_cfdi_uuids_p1)].copy()
    
    print(f"Pase 1 completado: {len(df_encontrados_p1)} coincidencias encontradas.")

    # --- PASE 2: Cruce por Folio Exacto + Monto ---
    print("\n--- Iniciando Pase 2: Folio Exacto + Monto ---")
    regex_p2 = r'\b{folio}\b' # Folio como palabra exacta
    df_encontrados_p2, sobrantes_aux_p2, sobrantes_cfdi_p2 = match_by_folio_regex(
        sobrantes_cfdi_p1, sobrantes_aux_p1, regex_p2, 'Folio+Monto'
    )
    all_encontrados_dfs.append(df_encontrados_p2)
    print(f"Pase 2 completado: {len(df_encontrados_p2)} coincidencias encontradas.")

    # --- PASE 3: Cruce por Folio Parcial + Monto ---
    print("\n--- Iniciando Pase 3: Folio Parcial + Monto ---")
    # --- CORRECCIÓN (Quitar warnings) ---
    regex_p3 = r'{folio}(?:\b|$)' # Folio al final de una palabra o de la línea
    df_encontrados_p3, sobrantes_aux_p3, sobrantes_cfdi_p3 = match_by_folio_regex(
        sobrantes_cfdi_p2, sobrantes_aux_p2, regex_p3, 'FolioParcial+Monto'
    )
    all_encontrados_dfs.append(df_encontrados_p3)
    print(f"Pase 3 completado: {len(df_encontrados_p3)} coincidencias encontradas.")

    # --- PASE 4: Cruce por Monto Exacto + Fecha (Cercana, 5 días) ---
    print("\n--- Iniciando Pase 4: Monto Exacto + Fecha (5 días) ---")
    df_encontrados_p4, sobrantes_aux_p4, sobrantes_cfdi_p4 = match_by_monto_exacto(
        sobrantes_cfdi_p3, sobrantes_aux_p3, 5, 'Monto+Fecha(5d)'
    )
    all_encontrados_dfs.append(df_encontrados_p4)
    print(f"Pase 4 completado: {len(df_encontrados_p4)} coincidencias encontradas.")

    # --- PASE 5: Cruce por Monto Exacto + Fecha (Amplia, 30 días) ---
    print("\n--- Iniciando Pase 5: Monto Exacto + Fecha (30 días) ---")
    df_encontrados_p5, sobrantes_aux_p5, sobrantes_cfdi_p5 = match_by_monto_exacto(
        sobrantes_cfdi_p4, sobrantes_aux_p4, 30, 'Monto+Fecha(30d)'
    )
    all_encontrados_dfs.append(df_encontrados_p5)
    print(f"Pase 5 completado: {len(df_encontrados_p5)} coincidencias encontradas.")

    # --- PASE 6: Cruce por Monto Exacto (Solo) ---
    print("\n--- Iniciando Pase 6: Monto Exacto (Solo) - 'Amarrar sí o sí' ---")
    df_encontrados_p6, sobrantes_aux_p6, sobrantes_cfdi_p6 = match_by_monto_exacto(
        sobrantes_cfdi_p5, sobrantes_aux_p5, None, 'Monto(Solo)' # None = Sin límite de fecha
    )
    all_encontrados_dfs.append(df_encontrados_p6)
    print(f"Pase 6 completado: {len(df_encontrados_p6)} coincidencias encontradas.")
    
    # --- PASE 7: Cruce por Monto Próximo (Tolerancia) ---
    print("\n--- Iniciando Pase 7: Monto Próximo (Errores de Dedo) ---")
    df_encontrados_p7, sobrantes_aux_final, sobrantes_cfdi_final = match_by_monto_proximo(
        sobrantes_cfdi_p6, sobrantes_aux_p6, TOLERANCIA_MONTO, 30, f'Monto_Proximo(${TOLERANCIA_MONTO})'
    )
    all_encontrados_dfs.append(df_encontrados_p7)
    print(f"Pase 7 completado: {len(df_encontrados_p7)} coincidencias encontradas.")

    # --- Resultados Finales ---
    print("\n--- Resumen de Pases ---")
    print(f"Pase 0 (Ruido): {len(df_aux_ruido)} filas separadas")
    print(f"Pase 1 (UUID): {len(df_encontrados_p1)} filas")
    print(f"Pase 2 (Folio Exacto): {len(df_encontrados_p2)} filas")
    print(f"Pase 3 (Folio Parcial): {len(df_encontrados_p3)} filas")
    print(f"Pase 4 (Monto 5d): {len(df_encontrados_p4)} filas")
    print(f"Pase 5 (Monto 30d): {len(df_encontrados_p5)} filas")
    print(f"Pase 6 (Monto Solo): {len(df_encontrados_p6)} filas")
    print(f"Pase 7 (Monto Próximo): {len(df_encontrados_p7)} filas")
    
    df_encontrados_final = pd.concat(all_encontrados_dfs, ignore_index=True)
    print(f"\nTotal Coincidencias: {len(df_encontrados_final)} filas")
    print(f"Sobrantes FINALES AUX: {len(sobrantes_aux_final)} filas")
    print(f"Sobrantes FINALES CFDI: {len(sobrantes_cfdi_final)} filas")
    
    # --- Guardar Archivo Final de EXCEL (Con 7 Hojas) ---
    print(f"\n--- Generando archivo de salida Excel: {output_file_path} ---")
    
    try:
        print("Intentando escribir el archivo Excel...")
        
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            
            # --- Limpiar columnas (se hace una sola vez) ---
            aux_cols_final = ['Fecha', 'Concepto', 'Debe', 'Haber', 'Tipo', 'Numero']
            cfdi_cols_final = ['UUID', 'Folio', 'Total', 'Emisión']
            match_col = ['Match_Type']
            
            final_cols_encontrados = match_col.copy()
            
            # Columnas extra para Pase 7
            extra_cols = ['Monto_Diff'] 
            
            for col in aux_cols_final:
                if f"{col}_AUX" in df_encontrados_final.columns:
                    df_encontrados_final.rename(columns={f"{col}_AUX": col}, inplace=True)
                if col in df_encontrados_final.columns:
                    final_cols_encontrados.append(col)
            
            for col in cfdi_cols_final:
                if f"{col}_CFDI" in df_encontrados_final.columns:
                    df_encontrados_final.rename(columns={f"{col}_CFDI": col}, inplace=True)
                if col in df_encontrados_final.columns:
                    final_cols_encontrados.append(col)
            
            for col in extra_cols:
                if col in df_encontrados_final.columns:
                    final_cols_encontrados.append(col)
            
            final_cols_unique = list(dict.fromkeys(final_cols_encontrados))
            final_cols_existing = [col for col in final_cols_unique if col in df_encontrados_final.columns]
            
            df_encontrados_final_clean = df_encontrados_final[final_cols_existing].copy()
            df_encontrados_final_clean.sort_values(by='Match_Type', inplace=True)

            # --- CORRECCIÓN DE NOMBRES DE HOJAS ---
            # --- Hoja 1: Confianza_Alta_95_100 ---
            tipos_alta = ['UUID', 'Folio+Monto', 'FolioParcial+Monto']
            df_conf_alta = df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_alta)]
            df_conf_alta.to_excel(writer, sheet_name='Confianza_Alta_95_100', index=False)
            print(f"Hoja 'Confianza_Alta_95_100' guardada ({len(df_conf_alta)} filas)")

            # --- Hoja 2: Confianza_Media_80 ---
            tipos_media = ['Monto+Fecha(5d)']
            df_conf_media = df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_media)]
            df_conf_media.to_excel(writer, sheet_name='Confianza_Media_80', index=False)
            print(f"Hoja 'Confianza_Media_80' guardada ({len(df_conf_media)} filas)")

            # --- Hoja 3: Confianza_Baja_Revisar ---
            tipos_baja = ['Monto+Fecha(30d)', 'Monto(Solo)']
            df_conf_baja = df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_baja)]
            df_conf_baja.to_excel(writer, sheet_name='Confianza_Baja_Revisar', index=False)
            print(f"Hoja 'Confianza_Baja_Revisar' guardada ({len(df_conf_baja)} filas)")

            # --- Hoja 4: Confianza_Muy_Baja_Proximidad ---
            tipos_proximo = [f'Monto_Proximo(${TOLERANCIA_MONTO})']
            df_conf_proximo = df_encontrados_final_clean[df_encontrados_final_clean['Match_Type'].isin(tipos_proximo)]
            df_conf_proximo.to_excel(writer, sheet_name='Confianza_Muy_Baja_Proximidad', index=False)
            print(f"Hoja 'Confianza_Muy_Baja_Proximidad' guardada ({len(df_conf_proximo)} filas)")

            # --- Hoja 5: No_Coincidencias_AUX (Sobrantes del AUX) ---
            cols_to_drop_aux = ['Concepto_Upper', 'Monto_Debe', 'Monto_Haber', 'UUID_extract']
            sobrantes_aux_final_clean = sobrantes_aux_final.drop(columns=cols_to_drop_aux, errors='ignore')
            sobrantes_aux_final_clean.to_excel(writer, sheet_name='No_Coincidencias_AUX', index=False)
            print(f"Hoja 'No_Coincidencias_AUX' guardada ({len(sobrantes_aux_final_clean)} filas)")
            
            # --- Hoja 6: Sobrantes_CFDI (Sobrantes del CFDI) ---
            cols_to_drop_cfdi = ['Folio_str', 'Monto_Total']
            sobrantes_cfdi_final_clean = sobrantes_cfdi_final.drop(columns=cols_to_drop_cfdi, errors='ignore')
            sobrantes_cfdi_final_clean.to_excel(writer, sheet_name='Sobrantes_CFDI', index=False)
            print(f"Hoja 'Sobrantes_CFDI' guardada ({len(sobrantes_cfdi_final_clean)} filas)")
            
            # --- Hoja 7: AUX_Filtrado_No_Requerido (Ruido) ---
            df_aux_ruido_clean = df_aux_ruido.drop(columns=cols_to_drop_aux, errors='ignore')
            df_aux_ruido_clean.to_excel(writer, sheet_name='AUX_Filtrado_No_Requerido', index=False)
            print(f"Hoja 'AUX_Filtrado_No_Requerido' guardada ({len(df_aux_ruido_clean)} filas)")
            
        print("\n¡Éxito! Archivo Excel 'TOTAL' generado en:")
        print(output_file_path)
        
    except Exception as e:
        print(f"\nERROR CRÍTICO AL GUARDAR el archivo Excel: {e}")
        print("Posibles causas:")
        print(f"1. ¿Tienes el archivo '{os.path.basename(output_file_path)}' abierto en Excel?")
        print("2. ¿No tienes permisos de escritura en tu carpeta de Descargas?")
        print("Por favor, cierra el archivo e intenta de nuevo.")

print("\nProceso de conciliación TOTAL terminado.")