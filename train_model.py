import pandas as pd
import numpy as np
import os
import joblib
import sys # <--- AÑADIDO: Necesario para sys.exit()
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from fuzzywuzzy import fuzz

# --- CONFIGURACIÓN ---
# Las 4 "features" (características) que la IA usará para aprender.
# DEBEN ser las mismas que en app.py
IA_FEATURES = ['diferencia_monto', 'diferencia_dias', 'similitud_folio', 'es_mismo_monto']

# Archivo de entrenamiento base (el que generamos con datos falsos)
BASE_TRAIN_FILE = 'entrenamiento.csv'
# Archivo de re-entrenamiento (donde TÚ pones los nuevos matches verificados)
NEW_TRAIN_FILE = 'nuevos_matches_verificados.csv'
# El archivo final de la IA
MODELO_FILENAME = 'modelo_conciliacion.pkl'


def crear_features_manual(monto_cfdi, monto_aux, fecha_cfdi, fecha_aux, folio_cfdi, concepto_aux):
    """
    Función de ayuda para que puedas crear tu archivo de re-entrenamiento.
    """
    try:
        monto_cfdi = float(monto_cfdi)
        monto_aux = float(monto_aux)
        fecha_cfdi = pd.to_datetime(fecha_cfdi)
        fecha_aux = pd.to_datetime(fecha_aux)

        diff_monto = abs(monto_cfdi - monto_aux)
        diff_dias = abs((fecha_cfdi - fecha_aux).days)
        sim_folio = fuzz.token_set_ratio(str(folio_cfdi), str(concepto_aux))
        es_mismo_monto = 1 if diff_monto < 0.01 else 0
        return [diff_monto, diff_dias, sim_folio, es_mismo_monto]
    except Exception as e:
        print(f"Error creando feature manual: {e}")
        return [99999, 999, 0, 0]


def generar_datos_falsos_iniciales():
    """
    Crea un archivo 'entrenamiento.csv' con datos falsos
    si no existe.
    """
    print(f"No se encontró '{BASE_TRAIN_FILE}'. Creando datos falsos de entrenamiento...")
    datos = []
    # 1. Matches perfectos (Folio y Monto)
    for _ in range(200):
        datos.append({'diferencia_monto': 0, 'diferencia_dias': np.random.randint(0, 10), 'similitud_folio': 100, 'es_mismo_monto': 1, 'es_match': 1})
    # 2. Matches buenos (Monto Próximo, Folio Parcial)
    for _ in range(100):
        datos.append({'diferencia_monto': np.random.uniform(0.01, 1.0), 'diferencia_dias': np.random.randint(0, 30), 'similitud_folio': np.random.randint(70, 99), 'es_mismo_monto': 0, 'es_match': 1})
    # 3. No Matches (Montos muy diferentes)
    for _ in range(300):
        datos.append({'diferencia_monto': np.random.uniform(100, 5000), 'diferencia_dias': np.random.randint(0, 90), 'similitud_folio': np.random.randint(0, 40), 'es_mismo_monto': 0, 'es_match': 0})
    # 4. No Matches (Misma fecha, pero montos/folios diferentes)
    for _ in range(200):
        datos.append({'diferencia_monto': np.random.uniform(10, 50), 'diferencia_dias': np.random.randint(0, 5), 'similitud_folio': np.random.randint(0, 30), 'es_mismo_monto': 0, 'es_match': 0})

    df_falso = pd.DataFrame(datos)
    # CORRECCIÓN: Usar BASE_DIR para guardar en la carpeta correcta
    output_path = os.path.join(os.path.dirname(__file__), BASE_TRAIN_FILE)
    df_falso.to_csv(output_path, index=False)
    print(f"Archivo '{BASE_TRAIN_FILE}' creado con {len(df_falso)} ejemplos.")
    return df_falso

def entrenar_modelo():
    """
    Función principal para cargar datos y (re)entrenar el modelo.
    """
    df_base = None
    df_nuevos = None
    base_dir = os.path.dirname(__file__) # Directorio actual del script

    # 1. Cargar datos base (o crearlos si no existen)
    base_file_path = os.path.join(base_dir, BASE_TRAIN_FILE)
    try:
        df_base = pd.read_csv(base_file_path)
        print(f"Datos base cargados desde '{BASE_TRAIN_FILE}' ({len(df_base)} filas).")
    except FileNotFoundError:
        df_base = generar_datos_falsos_iniciales()

    # 2. Cargar NUEVOS datos verificados por el humano (si existen)
    new_file_path = os.path.join(base_dir, NEW_TRAIN_FILE)
    try:
        df_nuevos = pd.read_csv(new_file_path)
        print(f"¡Excelente! Se encontraron datos nuevos verificados en '{NEW_TRAIN_FILE}' ({len(df_nuevos)} filas).")
        # Asegurarse que el archivo nuevo tenga las columnas correctas
        if not all(col in df_nuevos.columns for col in IA_FEATURES + ['es_match']):
             print(f"ADVERTENCIA: '{NEW_TRAIN_FILE}' no tiene las columnas correctas. Se omitirá.")
             df_nuevos = None
    except FileNotFoundError:
        print(f"No se encontró '{NEW_TRAIN_FILE}'. (Esto es opcional).")
        print("Para re-entrenar, crea ese archivo con tus matches manuales.")
    except Exception as e:
        print(f"Error al leer '{NEW_TRAIN_FILE}': {e}. Se omitirá.")

    # 3. Combinar todos los datos
    if df_nuevos is not None:
        # Combinar y eliminar duplicados (dando prioridad a los nuevos)
        df_total = pd.concat([df_base, df_nuevos]).drop_duplicates(subset=IA_FEATURES, keep='last')
        print(f"Total de datos para entrenar (combinados y sin duplicados): {len(df_total)} filas.")
    else:
        df_total = df_base

    # Verificar si df_total tiene datos suficientes
    if df_total.empty or len(df_total) < 2: # Necesita al menos 2 filas para train/test split
        print("ERROR: No hay suficientes datos para entrenar el modelo.")
        sys.exit(1)

    # 4. Entrenar el modelo
    print("Iniciando entrenamiento del modelo Random Forest...")

    X = df_total[IA_FEATURES]
    y = df_total['es_match']

    # Dividir solo para ver la precisión. El modelo final se entrena con TODO.
    # Añadir manejo de error si solo hay una clase en 'y'
    try:
        if len(np.unique(y)) > 1:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        else:
            # Si solo hay una clase, no podemos estratificar ni hacer split significativo
            print("ADVERTENCIA: Solo se encontró una clase en los datos de entrenamiento. La precisión no será representativa.")
            X_train, X_test, y_train, y_test = X, X, y, y # Usar todo como train y test
    except ValueError as e:
        print(f"Error durante train_test_split: {e}. Puede que necesites más datos diversos.")
        sys.exit(1)


    modelo = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    modelo.fit(X_train, y_train)

    # Solo calcular precisión si X_test no está vacío
    if not X_test.empty:
        precision = modelo.score(X_test, y_test)
        print(f"\nPrecisión del modelo en datos de prueba: {precision * 100:.2f}%")
    else:
        print("\nNo hay datos de prueba para calcular la precisión.")

    # 5. Re-entrenar con TODOS los datos (para el modelo final)
    print("Entrenando modelo final con el 100% de los datos...")
    modelo_final = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    modelo_final.fit(X, y) # Entrenar con todo X y todo y

    # 6. Guardar el modelo final
    model_output_path = os.path.join(base_dir, MODELO_FILENAME)
    joblib.dump(modelo_final, model_output_path)
    print("\n¡Éxito!")
    print(f"Modelo de IA guardado como: '{MODELO_FILENAME}'")
    print("Ya puedes ejecutar 'app.py'.")

    # Mostrar importancia de features
    try:
        importances = modelo_final.feature_importances_
        feature_importance_df = pd.DataFrame({'Feature': IA_FEATURES, 'Importancia': importances})
        print("\n--- Importancia de las Características (Qué aprendió la IA) ---")
        print(feature_importance_df.sort_values(by='Importancia', ascending=False))
        print("-----------------------------------------------------------------")
    except Exception as e:
        print(f"No se pudo calcular la importancia de las features: {e}")

if __name__ == "__main__":
    # Asegurarse que las librerías necesarias estén instaladas
    try:
        import sklearn
        import fuzzywuzzy
    except ImportError as e:
        print(f"ERROR: Falta la librería '{e.name}'. Ejecuta esto en tu terminal:")
        # CORREGIDO: Usar sys.executable para garantizar que se usa el python correcto
        print(f"{sys.executable} -m pip install scikit-learn fuzzywuzzy python-Levenshtein")
        sys.exit(1) # <--- CORREGIDO: Detener ejecución si faltan librerías

    entrenar_modelo()