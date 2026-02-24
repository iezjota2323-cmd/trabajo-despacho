# modulo_conciliacion.py
import pandas as pd
import numpy as np
import re
import os
import warnings
from datetime import datetime

# Silenciamos advertencias de formato de Excel
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

TOLERANCIA_MONTO = 1.00 # +/- 1 peso
PALABRAS_EXCLUSION = ['NOMINA', 'IMSS', 'SAT', 'INFONAVIT', 'COMISION', 'TRASPASO', 'IMPUESTO']

def load_cfdi(filename):
    try:
        try:
            df = pd.read_excel(filename, sheet_name='CFDI REC PROV', header=4, engine='openpyxl')
        except:
            df = pd.read_excel(filename, header=4, engine='openpyxl')
            
        df.columns = [str(c).strip() for c in df.columns]
        iva_col = next((c for c in df.columns if 'IVA' in c.upper()), None)
        
        cols_to_keep = ['UUID', 'Folio', 'Total', 'Emisión']
        if iva_col: cols_to_keep.append(iva_col)
            
        if 'UUID' not in df.columns or 'Total' not in df.columns: return None
            
        df_clean = df[[c for c in cols_to_keep if c in df.columns]].copy()
        
        if 'Total' in df_clean.columns:
            df_clean['Total'] = pd.to_numeric(df_clean['Total'], errors='coerce')
        if 'Emisión' in df_clean.columns:
            df_clean['Emisión'] = pd.to_datetime(df_clean['Emisión'], errors='coerce')
        if 'UUID' in df_clean.columns:
            df_clean['UUID'] = df_clean['UUID'].astype(str).str.upper().str.strip()
        
        if iva_col:
            df_clean['Monto_Target'] = pd.to_numeric(df_clean[iva_col], errors='coerce').fillna(0).round(2)
        else:
            df_clean['Monto_Target'] = df_clean['Total'].round(2)
            
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
        df_clean = df.copy()
            
        if 'Fecha' in df_clean.columns:
            df_clean['Fecha'] = pd.to_datetime(df_clean['Fecha'], errors='coerce') 
        if 'Debe' in df_clean.columns:
            df_clean['Debe'] = pd.to_numeric(df_clean['Debe'], errors='coerce').fillna(0)
        if 'Haber' in df_clean.columns:
            df_clean['Haber'] = pd.to_numeric(df_clean['Haber'], errors='coerce').fillna(0)
            
        df_clean['ID_AUX'] = range(len(df_clean))
        df_clean['Monto_Search'] = df_clean['Debe'] + df_clean['Haber'] # Combinamos para buscar en el IVA del CFDI
        
        return df_clean
    except Exception as e:
        print(f"Error cargando AUX: {e}")
        return None

def ejecutar_conciliacion(cfdi_path, aux_path, output_path, *args, **kwargs):
    """
    Programa: Conciliacion IA (Solo Excel)
    Cruza Debe/Haber de AUX vs Columna IVA de CFDI.
    """
    try:
        df_cfdi = load_cfdi(cfdi_path)
        df_aux = load_aux(aux_path)
        if df_cfdi is None or df_aux is None: return False, [], "Error en carga de archivos."

        # Merge por monto
        merged = pd.merge(df_aux, df_cfdi, left_on='Monto_Search', right_on='Monto_Target', suffixes=('_AUX', '_CFDI'))
        merged['Match_Type'] = 'Monto_IA_IVA'
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            merged.to_excel(writer, sheet_name='Coincidencias', index=False)
            df_aux[~df_aux['ID_AUX'].isin(merged['ID_AUX'])].to_excel(writer, sheet_name='Sobrantes_AUX', index=False)

        dashboard = [{"Paso": "Match Monto IA (Debe/Haber vs IVA)", "Coincidencias": len(merged)}]
        resumen = f"Se encontraron {len(merged)} coincidencias entre los montos de tu auxiliar y el IVA de las facturas."
        return True, dashboard, resumen
    except Exception as e: return False, [], str(e)

def generar_resumen_ia(df_final, *args, **kwargs):
    """Función placeholder para mantener compatibilidad con app.py"""
    if df_final is None or (isinstance(df_final, pd.DataFrame) and df_final.empty):
        return "No se encontraron coincidencias suficientes para generar un análisis."
    return f"Análisis completado: Se procesaron {len(df_final)} registros con éxito."
