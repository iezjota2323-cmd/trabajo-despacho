# modulo_conciliacion.py
import pandas as pd
import numpy as np
import re
import os
import warnings
import shutil
from datetime import datetime

# Silenciamos advertencias de formato de Excel
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

TOLERANCIA_MONTO = 1.00 # +/- 1 peso
PALABRAS_EXCLUSION = ['NOMINA', 'IMSS', 'SAT', 'INFONAVIT', 'COMISION', 'TRASPASO', 'IMPUESTO']

def load_cfdi(filename):
    try:
        # Intentamos cargar la hoja 'CFDI REC PROV' o la primera hoja si no existe
        try:
            df = pd.read_excel(filename, sheet_name='CFDI REC PROV', header=4, engine='openpyxl')
        except:
            df = pd.read_excel(filename, header=4, engine='openpyxl')
            
        # Limpiar nombres de columnas
        df.columns = [str(c).strip() for c in df.columns]
        
        # Columnas necesarias: UUID, Folio, Total, Emisión e IVA
        # Buscamos una columna que contenga 'IVA' si no se llama exactamente así
        iva_col = next((c for c in df.columns if 'IVA' in c.upper()), None)
        
        cols_to_keep = ['UUID', 'Folio', 'Total', 'Emisión']
        if iva_col:
            cols_to_keep.append(iva_col)
            
        # Verificar si existen las columnas mínimas
        if 'UUID' not in df.columns or 'Total' not in df.columns:
            return None
            
        df_clean = df[[c for c in cols_to_keep if c in df.columns]].copy()
        
        if 'Total' in df_clean.columns:
            df_clean['Total'] = pd.to_numeric(df_clean['Total'], errors='coerce')
        if 'Emisión' in df_clean.columns:
            df_clean['Emisión'] = pd.to_datetime(df_clean['Emisión'], errors='coerce')
        if 'UUID' in df_clean.columns:
            df_clean['UUID'] = df_clean['UUID'].astype(str).str.upper().str.strip()
        if 'Folio' in df_clean.columns:
            df_clean['Folio_str'] = df_clean['Folio'].astype(str).str.strip().str.upper().replace('NAN', np.nan)
        
        if iva_col:
            df_clean['IVA_Monto'] = pd.to_numeric(df_clean[iva_col], errors='coerce').fillna(0).round(2)
        else:
            df_clean['IVA_Monto'] = 0.0
            
        # Para "Conciliacion IA", el usuario quiere buscar Debe/Haber en la columna IVA de CFDI
        df_clean['Monto_Target'] = df_clean['IVA_Monto']
        
        df_clean.dropna(subset=['UUID'], inplace=True)
        return df_clean
    except Exception as e:
        print(f"Error cargando CFDI: {e}")
        return None

def load_aux(filename):
    try:
        try:
            df = pd.read_excel(filename, sheet_name='AUX', header=0, engine='openpyxl')
        except:
            df = pd.read_excel(filename, header=0, engine='openpyxl')
            
        df.columns = [str(c).strip() for c in df.columns]
        
        if 'Concepto' in df.columns:
            df_clean = df[df['Concepto'].notna()].copy()
        else:
            df_clean = df.copy()
            
        if 'Fecha' in df_clean.columns:
            df_clean['Fecha'] = pd.to_datetime(df_clean['Fecha'], errors='coerce') 
        if 'Debe' in df_clean.columns:
            df_clean['Debe'] = pd.to_numeric(df_clean['Debe'], errors='coerce').fillna(0)
        if 'Haber' in df_clean.columns:
            df_clean['Haber'] = pd.to_numeric(df_clean['Haber'], errors='coerce').fillna(0)
            
        df_clean['ID_AUX'] = range(len(df_clean))
        
        if 'Concepto' in df_clean.columns:
            df_clean['Concepto_Upper'] = df_clean['Concepto'].astype(str).str.upper()
            uuid_pattern = re.compile(r'([0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})')
            df_clean['UUID_extract'] = df_clean['Concepto_Upper'].apply(lambda x: (uuid_pattern.search(x) or [None])[0])
        else:
            df_clean['Concepto_Upper'] = ""
            df_clean['UUID_extract'] = None

        df_clean['Monto_Debe'] = df_clean['Debe'].round(2) if 'Debe' in df_clean.columns else 0
        df_clean['Monto_Haber'] = df_clean['Haber'].round(2) if 'Haber' in df_clean.columns else 0
        
        return df_clean
    except Exception as e:
        print(f"Error cargando AUX: {e}")
        return None

def match_by_folio_regex(cfdi_df, aux_df, regex_template, match_type_label):
    if 'Folio_str' not in cfdi_df.columns or cfdi_df.empty or aux_df.empty: 
        return pd.DataFrame(), aux_df, cfdi_df

    cfdi_to_match = cfdi_df.dropna(subset=['Folio_str', 'Monto_Target']).copy()
    if cfdi_to_match.empty: return pd.DataFrame(), aux_df, cfdi_df

    aux_melted = pd.concat([
        aux_df[aux_df['Monto_Debe'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Debe']].rename(columns={'Monto_Debe': 'Monto_Match'}),
        aux_df[aux_df['Monto_Haber'] > 0][['ID_AUX', 'Concepto_Upper', 'Monto_Haber']].rename(columns={'Monto_Haber': 'Monto_Match'})
    ]).dropna(subset=['Monto_Match', 'Concepto_Upper'])

    all_matches = []
    for folio in cfdi_to_match['Folio_str'].unique():
        if not folio or str(folio).lower() == 'nan': continue
        folio_regex = regex_template.format(folio=re.escape(str(folio)))
        aux_with_folio = aux_melted[aux_melted['Concepto_Upper'].str.contains(folio_regex, na=False, regex=True)]
        if aux_with_folio.empty: continue
        
        cfdi_with_folio = cfdi_to_match[cfdi_to_match['Folio_str'] == folio]
        matches = pd.merge(cfdi_with_folio, aux_with_folio, left_on='Monto_Target', right_on='Monto_Match')
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

    merged = pd.merge(cfdi_df, aux_melted, left_on='Monto_Target', right_on='Monto_Match', suffixes=('_CFDI', '_AUX'))
    
    if 'Emisión' in merged.columns and 'Fecha' in merged.columns:
        merged['Date_Diff'] = (merged['Emisión'] - merged['Fecha']).abs().dt.days
        if date_window_days is not None:
            matches = merged[merged['Date_Diff'] <= date_window_days].copy()
        else:
            matches = merged.sort_values(by=['UUID', 'Date_Diff']).copy()
    else:
        matches = merged.copy()

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
        m_target, f_emision = cfdi_row['Monto_Target'], cfdi_row['Emisión']
        mask_monto = (aux_melted['Monto_Match'] >= m_target - tolerance) & (aux_melted['Monto_Match'] <= m_target + tolerance)
        mask_fecha = (aux_melted['Fecha'] >= f_emision - pd.Timedelta(days=date_window_days)) & (aux_melted['Fecha'] <= f_emision + pd.Timedelta(days=date_window_days))
        mask_no_exacto = (aux_melted['Monto_Match'] != m_target)
        
        candidates = aux_melted[mask_monto & mask_fecha & mask_no_exacto].copy()
        if not candidates.empty:
            candidates['Monto_Diff'] = (candidates['Monto_Match'] - m_target).abs()
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

def generar_resumen_ia(df_final, sob_aux, sob_cfdi):
    total_matches = len(df_final)
    total_aux = len(sob_aux) + total_matches
    porcentaje = (total_matches / total_aux * 100) if total_aux > 0 else 0
    
    resumen = f"Se han conciliado {total_matches} movimientos (Debe/Haber vs IVA). "
    resumen += f"Representa un {porcentaje:.1f}% de coincidencia. "
    
    if porcentaje > 80:
        resumen += "Los montos de IVA fiscal coinciden ampliamente con los registros contables."
    else:
        resumen += "Existen discrepancias entre el IVA de las facturas y los montos registrados en el auxiliar."
        
    return resumen

def ejecutar_conciliacion(cfdi_path, aux_path, output_path, pdf_dir, entregables_dir):
    try:
        df_cfdi_orig = load_cfdi(cfdi_path)
        df_aux_orig = load_aux(aux_path)

        if df_cfdi_orig is None or df_aux_orig is None:
            return False, [], "Error al cargar los archivos de Excel. Verifique el formato."

        all_encontrados_dfs = []
        dashboard_data = []

        # Pase 0: Ruido
        exclusion_regex = r'\b(?:' + '|'.join(PALABRAS_EXCLUSION) + r')\b'
        mask_ruido = df_aux_orig['Concepto_Upper'].str.contains(exclusion_regex, na=False, regex=True)
        df_aux_ruido = df_aux_orig[mask_ruido].copy()
        df_aux = df_aux_orig[~mask_ruido].copy()
        dashboard_data.append({"Paso": "0. Filtrado de Ruido", "Coincidencias": len(df_aux_ruido)})

        # Pase 1: UUID
        df_p1 = pd.merge(df_aux.dropna(subset=['UUID_extract']), df_cfdi_orig, left_on='UUID_extract', right_on='UUID', suffixes=('_AUX', '_CFDI'))
        df_p1['Match_Type'] = 'UUID'
        all_encontrados_dfs.append(df_p1)
        sob_aux_1 = df_aux[~df_aux['ID_AUX'].isin(df_p1['ID_AUX'])]
        sob_cfdi_1 = df_cfdi_orig[~df_cfdi_orig['UUID'].isin(df_p1['UUID'])]
        dashboard_data.append({"Paso": "1. Match por UUID", "Coincidencias": len(df_p1)})

        # Pase 2: Folio Exacto + IVA
        df_p2, sob_aux_2, sob_cfdi_2 = match_by_folio_regex(sob_cfdi_1, sob_aux_1, r'\b{folio}\b', 'Folio+IVA')
        all_encontrados_dfs.append(df_p2)
        dashboard_data.append({"Paso": "2. Folio + IVA", "Coincidencias": len(df_p2)})

        # Pase 3: IVA + Fecha (5d)
        df_p3, sob_aux_3, sob_cfdi_3 = match_by_monto_exacto(sob_cfdi_2, sob_aux_2, 5, 'IVA+Fecha(5d)')
        all_encontrados_dfs.append(df_p3)
        dashboard_data.append({"Paso": "3. IVA + Fecha (5d)", "Coincidencias": len(df_p3)})

        # Pase 4: IVA + Fecha (30d)
        df_p4, sob_aux_4, sob_cfdi_4 = match_by_monto_exacto(sob_cfdi_3, sob_aux_3, 30, 'IVA+Fecha(30d)')
        all_encontrados_dfs.append(df_p4)
        dashboard_data.append({"Paso": "4. IVA + Fecha (30d)", "Coincidencias": len(df_p4)})

        # Pase 5: IVA Solo
        df_p5, sob_aux_5, sob_cfdi_5 = match_by_monto_exacto(sob_cfdi_4, sob_aux_4, None, 'IVA(Solo)')
        all_encontrados_dfs.append(df_p5)
        dashboard_data.append({"Paso": "5. IVA Solo", "Coincidencias": len(df_p5)})
        
        # Pase 6: IVA Próximo
        df_p6, sob_aux_fin, sob_cfdi_fin = match_by_monto_proximo(sob_cfdi_5, sob_aux_5, TOLERANCIA_MONTO, 30, f'IVA_Proximo(${TOLERANCIA_MONTO})')
        all_encontrados_dfs.append(df_p6)
        dashboard_data.append({"Paso": "6. IVA Próximo ($1)", "Coincidencias": len(df_p6)})

        df_final = pd.concat(all_encontrados_dfs, ignore_index=True)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            if not df_final.empty:
                df_final.sort_values('Match_Type', inplace=True)
                df_final.to_excel(writer, sheet_name='Coincidencias_IVA', index=False)
            
            sob_aux_fin.to_excel(writer, sheet_name='Sobrantes_AUX', index=False)
            sob_cfdi_fin.to_excel(writer, sheet_name='Sobrantes_CFDI', index=False)

        resumen_ia = generar_resumen_ia(df_final, sob_aux_fin, sob_cfdi_fin)
        return True, dashboard_data, resumen_ia

    except Exception as e:
        import traceback
        return False, [], f"Error: {str(e)}"
