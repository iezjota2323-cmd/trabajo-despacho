import pandas as pd
import numpy as np
import re
import sys
import os
import warnings

# Silenciamos advertencias de formato de Excel para tener una consola limpia
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# --- CONFIGURACIÓN DE RUTAS ---
INPUT_FILE_PATH = '/Users/luispaniaguapalacios/Downloads/LUIS.xlsx'
OUTPUT_FILE_PATH = '/Users/luispaniaguapalacios/Downloads/LUIS_CONCILIACION_TOTAL.xlsx'

# --- CONFIGURACIÓN DE PASES ---
TOLERANCIA_MONTO = 1.00 # +/- 1 peso
PALABRAS_EXCLUSION = ['NOMINA', 'IMSS', 'SAT', 'INFONAVIT', 'COMISION', 'TRASPASO', 'IMPUESTO']

def load_cfdi(filename):
    print("Cargando hoja 'CFDI REC PROV'...")
    try:
        df = pd.read_excel(filename, sheet_name='CFDI REC PROV', header=4, engine='openpyxl')
        cols_to_keep = ['UUID', 'Folio', 'Total', 'Emisión']
        
        if not all(col in df.columns for col in cols_to_keep):
            print(f"ERROR: Faltan columnas: {cols_to_keep}")
            return None
            
        df_clean = df[cols_to_keep].copy()
        df_clean['Total'] = pd.to_numeric(df_clean['Total'], errors='coerce')
        df_clean['Emisión'] = pd.to_datetime(df_clean['Emisión'], errors='coerce')
        df_clean['UUID'] = df_clean['UUID'].astype(str).str.upper().str.strip()
        df_clean['Folio_str'] = df_clean['Folio'].astype(str).str.strip().str.upper().replace('NAN', np.nan)
        df_clean['Monto_Total'] = df_clean['Total'].round(2)
        df_clean.dropna(subset=['UUID', 'Total', 'Emisión'], inplace=True)
        
        print(f"CFDI cargado: {len(df_clean)} filas válidas.")
        return df_clean
    except Exception as e:
        print(f"Error cargando CFDI: {e}")
        return None

def load_aux(filename):
    print("Cargando hoja 'AUX'...")
    try:
        df = pd.read_excel(filename, sheet_name='AUX', header=0, engine='openpyxl')
        df.columns = df.columns.str.strip()
        
        df_clean = df[~df['Tipo'].astype(str).str.startswith('-', na=True) & df['Concepto'].notna()].copy()
        
        df_clean['Fecha'] = pd.to_datetime(df_clean['Fecha'], errors='coerce', dayfirst=True) 
        df_clean['Debe'] = pd.to_numeric(df_clean['Debe'], errors='coerce').fillna(0)
        df_clean['Haber'] = pd.to_numeric(df_clean['Haber'], errors='coerce').fillna(0)
        df_clean['ID_AUX'] = range(len(df_clean))
        df_clean['Concepto_Upper'] = df_clean['Concepto'].astype(str).str.upper()
        df_clean['Monto_Debe'] = df_clean['Debe'].round(2)
        df_clean['Monto_Haber'] = df_clean['Haber'].round(2)
        
        uuid_pattern = re.compile(r'([0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})')
        df_clean['UUID_extract'] = df_clean['Concepto_Upper'].apply(lambda x: (uuid_pattern.search(x) or [None])[0])
        
        print(f"AUX cargado: {len(df_clean)} movimientos válidos.")
        return df_clean
    except Exception as e:
        print(f"Error cargando AUX: {e}")
        return None

def match_by_folio_regex(cfdi_df, aux_df, regex_template, match_type_label):
    cfdi_to_match = cfdi_df.dropna(subset=['Folio_str', 'Monto_Total']).copy()
    if cfdi_to_match.empty or aux_df.empty: return pd.DataFrame(), aux_df, cfdi_df

    aux_melted = pd.concat([
        aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'}),
        aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
    ]).dropna(subset=['Monto_Match', 'Concepto_Upper'])

    all_matches = []
    for folio in cfdi_to_match['Folio_str'].unique():
        folio_regex = regex_template.format(folio=re.escape(str(folio)))
        aux_with_folio = aux_melted[aux_melted['Concepto_Upper'].str.contains(folio_regex, na=False, regex=True)]
        if aux_with_folio.empty: continue
        
        cfdi_with_folio = cfdi_to_match[cfdi_to_match['Folio_str'] == folio]
        matches = pd.merge(cfdi_with_folio, aux_with_folio, left_on='Monto_Total', right_on='Monto_Match')
        if not matches.empty: all_matches.append(matches)

    if all_matches:
        df_matches = pd.concat(all_matches, ignore_index=True).drop_duplicates('ID_AUX').drop_duplicates('UUID')
        df_encontrados = pd.merge(df_matches[['ID_AUX', 'UUID']], aux_df, on='ID_AUX', how='left').merge(cfdi_df, on='UUID', how='left', suffixes=('_AUX', '_CFDI'))
        df_encontrados['Match_Type'] = match_type_label
        
        sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(df_encontrados['ID_AUX'])].copy()
        sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(df_encontrados['UUID'])].copy()
        return df_encontrados, sobrantes_aux, sobrantes_cfdi

    return pd.DataFrame(), aux_df, cfdi_df

def match_by_monto_exacto(cfdi_df, aux_df, date_window_days, match_type_label):
    if aux_df.empty or cfdi_df.empty: return pd.DataFrame(), aux_df, cfdi_df

    aux_melted = pd.concat([
        aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Fecha', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'}),
        aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Fecha', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
    ])

    merged = pd.merge(cfdi_df, aux_melted, left_on='Monto_Total', right_on='Monto_Match', suffixes=('_CFDI', '_AUX'))
    merged['Date_Diff'] = (merged['Emisión'] - merged['Fecha']).abs().dt.days

    if date_window_days is not None:
        matches = merged[merged['Date_Diff'] <= date_window_days].copy()
    else:
        matches = merged.sort_values(by=['UUID', 'Date_Diff']).copy()

    matches.drop_duplicates('ID_AUX', keep='first', inplace=True)
    matches.drop_duplicates('UUID', keep='first', inplace=True)
    
    if not matches.empty:
        df_encontrados = pd.merge(matches, aux_df.drop(columns=['UUID_extract'], errors='ignore'), on='ID_AUX', suffixes=('_CFDI_MATCHED', '_AUX_ORIG'))
        df_encontrados['Match_Type'] = match_type_label
        
        sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(df_encontrados['ID_AUX'])].copy()
        sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(df_encontrados['UUID'])].copy()
        return df_encontrados, sobrantes_aux, sobrantes_cfdi
        
    return pd.DataFrame(), aux_df, cfdi_df

def match_by_monto_proximo(cfdi_df, aux_df, tolerance, date_window_days, match_type_label):
    if aux_df.empty or cfdi_df.empty: return pd.DataFrame(), aux_df, cfdi_df

    aux_melted = pd.concat([
        aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Fecha', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'}),
        aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Fecha', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
    ]).dropna(subset=['Fecha'])
    
    cfdi_df_clean = cfdi_df.dropna(subset=['Emisión'])
    all_matches_data, matched_aux_ids = [], set()
    
    for _, cfdi_row in cfdi_df_clean.iterrows():
        m_total, f_emision = cfdi_row['Monto_Total'], cfdi_row['Emisión']
        mask_monto = (aux_melted['Monto_Match'] >= m_total - tolerance) & (aux_melted['Monto_Match'] <= m_total + tolerance)
        mask_fecha = (aux_melted['Fecha'] >= f_emision - pd.Timedelta(days=date_window_days)) & (aux_melted['Fecha'] <= f_emision + pd.Timedelta(days=date_window_days))
        mask_no_exacto = (aux_melted['Monto_Match'] != m_total)
        
        candidates = aux_melted[mask_monto & mask_fecha & mask_no_exacto].copy()
        if not candidates.empty:
            candidates['Monto_Diff'] = (candidates['Monto_Match'] - m_total).abs()
            for _, cand in candidates.sort_values('Monto_Diff').iterrows():
                if cand['ID_AUX'] not in matched_aux_ids:
                    all_matches_data.append({'UUID': cfdi_row['UUID'], 'ID_AUX': cand['ID_AUX'], 'Monto_Diff': cand['Monto_Diff']})
                    matched_aux_ids.add(cand['ID_AUX'])
                    break 

    if not all_matches_data: return pd.DataFrame(), aux_df, cfdi_df

    matches = pd.DataFrame(all_matches_data)
    df_encontrados = pd.merge(matches, aux_df, on='ID_AUX', suffixes=('_MATCH', '_AUX_ORIG')).merge(cfdi_df, on='UUID', suffixes=('', '_CFDI_ORIG'))
    df_encontrados['Match_Type'] = match_type_label
    
    sobrantes_aux = aux_df[~aux_df['ID_AUX'].isin(df_encontrados['ID_AUX'])].copy()
    sobrantes_cfdi = cfdi_df[~cfdi_df['UUID'].isin(df_encontrados['UUID'])].copy()
    return df_encontrados, sobrantes_aux, sobrantes_cfdi

def main():
    print("="*50)
    print("   INICIANDO CONCILIACIÓN LOCAL (7 PASOS)   ")
    print("="*50)
    
    if not os.path.exists(INPUT_FILE_PATH):
        print(f"❌ ERROR: Archivo no encontrado en: {INPUT_FILE_PATH}")
        sys.exit()

    all_encontrados_dfs = []
    df_cfdi_orig = load_cfdi(INPUT_FILE_PATH)
    df_aux_orig = load_aux(INPUT_FILE_PATH)

    if df_cfdi_orig is None or df_aux_orig is None:
        print("❌ Error al cargar hojas. Terminando script.")
        return

    # Pase 0
    exclusion_regex = r'\b(?:' + '|'.join(PALABRAS_EXCLUSION) + r')\b'
    mask_ruido = df_aux_orig['Concepto_Upper'].str.contains(exclusion_regex, na=False, regex=True)
    df_aux_ruido = df_aux_orig[mask_ruido].copy()
    df_aux = df_aux_orig[~mask_ruido].copy()

    # Pase 1
    df_p1 = pd.merge(df_aux.dropna(subset=['UUID_extract']), df_cfdi_orig, left_on='UUID_extract', right_on='UUID', suffixes=('_AUX', '_CFDI'))
    df_p1['Match_Type'] = 'UUID'
    all_encontrados_dfs.append(df_p1)
    sob_aux_1 = df_aux[~df_aux['ID_AUX'].isin(df_p1['ID_AUX'])]
    sob_cfdi_1 = df_cfdi_orig[~df_cfdi_orig['UUID'].isin(df_p1['UUID'])]

    # Pase 2
    df_p2, sob_aux_2, sob_cfdi_2 = match_by_folio_regex(sob_cfdi_1, sob_aux_1, r'\b{folio}\b', 'Folio+Monto')
    all_encontrados_dfs.append(df_p2)

    # Pase 3
    df_p3, sob_aux_3, sob_cfdi_3 = match_by_folio_regex(sob_cfdi_2, sob_aux_2, r'{folio}(?:\b|$)', 'FolioParcial+Monto')
    all_encontrados_dfs.append(df_p3)

    # Pase 4
    df_p4, sob_aux_4, sob_cfdi_4 = match_by_monto_exacto(sob_cfdi_3, sob_aux_3, 5, 'Monto+Fecha(5d)')
    all_encontrados_dfs.append(df_p4)

    # Pase 5
    df_p5, sob_aux_5, sob_cfdi_5 = match_by_monto_exacto(sob_cfdi_4, sob_aux_4, 30, 'Monto+Fecha(30d)')
    all_encontrados_dfs.append(df_p5)

    # Pase 6
    df_p6, sob_aux_6, sob_cfdi_6 = match_by_monto_exacto(sob_cfdi_5, sob_aux_5, None, 'Monto(Solo)')
    all_encontrados_dfs.append(df_p6)
    
    # Pase 7
    df_p7, sob_aux_fin, sob_cfdi_fin = match_by_monto_proximo(sob_cfdi_6, sob_aux_6, TOLERANCIA_MONTO, 30, f'Monto_Proximo(${TOLERANCIA_MONTO})')
    all_encontrados_dfs.append(df_p7)

    print("\n--- RESUMEN DE PASES ---")
    print(f"Pase 0 (Ruido): {len(df_aux_ruido)}")
    print(f"Pase 1 (UUID): {len(df_p1)}")
    print(f"Pase 2 (Folio Exacto): {len(df_p2)}")
    print(f"Pase 3 (Folio Parcial): {len(df_p3)}")
    print(f"Pase 4 (Monto 5d): {len(df_p4)}")
    print(f"Pase 5 (Monto 30d): {len(df_p5)}")
    print(f"Pase 6 (Monto Solo): {len(df_p6)}")
    print(f"Pase 7 (Monto Próximo): {len(df_p7)}")

    df_final = pd.concat(all_encontrados_dfs, ignore_index=True)
    print(f"\n✅ Total Coincidencias: {len(df_final)}")
    
    try:
        with pd.ExcelWriter(OUTPUT_FILE_PATH, engine='openpyxl') as writer:
            if not df_final.empty:
                df_final.sort_values('Match_Type', inplace=True)
                df_final[df_final['Match_Type'].isin(['UUID', 'Folio+Monto', 'FolioParcial+Monto'])].to_excel(writer, sheet_name='Confianza_Alta', index=False)
                df_final[df_final['Match_Type'] == 'Monto+Fecha(5d)'].to_excel(writer, sheet_name='Confianza_Media', index=False)
                df_final[df_final['Match_Type'].isin(['Monto+Fecha(30d)', 'Monto(Solo)'])].to_excel(writer, sheet_name='Confianza_Baja', index=False)
                df_final[df_final['Match_Type'].str.contains('Proximo', na=False)].to_excel(writer, sheet_name='Revisar_Proximidad', index=False)
            
            sob_aux_fin.drop(columns=['Concepto_Upper', 'Monto_Debe', 'Monto_Haber', 'UUID_extract'], errors='ignore').to_excel(writer, sheet_name='Sobrantes_AUX', index=False)
            sob_cfdi_fin.drop(columns=['Folio_str', 'Monto_Total'], errors='ignore').to_excel(writer, sheet_name='Sobrantes_CFDI', index=False)
            df_aux_ruido.to_excel(writer, sheet_name='AUX_Ruido', index=False)
            
        print(f"\n💾 Archivo guardado exitosamente en: {OUTPUT_FILE_PATH}")
    except Exception as e:
        print(f"❌ Error al guardar el archivo. ¿Lo tienes abierto en Excel? Detalles: {e}")

if __name__ == '__main__':
    main()