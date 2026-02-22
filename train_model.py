import pandas as pd
import numpy as np
import os
import joblib
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix

ARCHIVO_DATOS = 'entrenamiento.csv'
MODELO_SALIDA = 'modelo_conciliacion.pkl'

# Features utilizadas por la IA para predecir si es un "match"
FEATURES = ['diferencia_monto', 'diferencia_dias', 'similitud_folio', 'similitud_razon_social', 'es_mismo_monto']
TARGET = 'es_match'

def entrenar_modelo():
    print("===================================================")
    print("   🧠 ENTRENAMIENTO DE IA PARA CONCILIACIÓN 🧠   ")
    print("===================================================")

    if not os.path.exists(ARCHIVO_DATOS):
        print(f"❌ ERROR: No se encontró el archivo de datos '{ARCHIVO_DATOS}'.")
        print("Asegúrate de tener un histórico de conciliaciones previas para entrenar a la IA.")
        return

    print(f"📂 Cargando datos históricos desde {ARCHIVO_DATOS}...")
    try:
        df = pd.read_csv(ARCHIVO_DATOS)
    except Exception as e:
        print(f"❌ ERROR al leer el archivo CSV: {e}")
        return
    
    # Validación de columnas
    columnas_faltantes = [col for col in FEATURES + [TARGET] if col not in df.columns]
    if columnas_faltantes:
        print(f"❌ ERROR: Faltan las siguientes columnas en tu CSV: {columnas_faltantes}")
        return

    # Limpieza de datos nulos
    df = df.dropna(subset=FEATURES + [TARGET])
    
    if len(df) < 20:
        print("⚠️ ADVERTENCIA: Tienes muy pocos datos para un entrenamiento cruzado efectivo.")
        print(f"Filas actuales: {len(df)}. Se recomienda tener al menos 100 ejemplos históricos.")

    X = df[FEATURES]
    y = df[TARGET]

    print(f"📊 Total de ejemplos para entrenar: {len(df)} (Matches reales: {int(y.sum())})")

    # División de datos (80% entrenamiento, 20% prueba)
    try:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    except ValueError as e:
        print(f"❌ ERROR al dividir los datos: {e}")
        print("Posible causa: Necesitas tener ejemplos de ambas clases (matches exitosos y fallidos) en tu CSV.")
        return

    print("⚙️ Buscando la configuración matemática óptima (esto puede tardar unos segundos)...")
    
    # Parámetros para buscar el mejor modelo
    parametros_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [None, 10, 20],
        'min_samples_split': [2, 5, 10],
        'class_weight': ['balanced']
    }

    modelo_base = RandomForestClassifier(random_state=42)
    
    # Usamos validación cruzada para asegurar que el modelo sea robusto
    buscador = GridSearchCV(estimator=modelo_base, param_grid=parametros_grid, cv=5, scoring='accuracy', n_jobs=-1)
    
    try:
        buscador.fit(X_train, y_train)
    except Exception as e:
        print(f"❌ ERROR durante el entrenamiento: {e}")
        print("Si el error menciona 'splits', es porque tienes muy pocos datos en el CSV para hacer validación cruzada (cv=5).")
        return

    mejor_modelo = buscador.best_estimator_

    print("\n✅ ¡Entrenamiento completado!")
    print(f"Mejor configuración encontrada: {buscador.best_params_}")
    
    # Evaluación del modelo
    predicciones = mejor_modelo.predict(X_test)
    precision = accuracy_score(y_test, predicciones)
    
    print("\n--- 📈 REPORTE DE RENDIMIENTO ---")
    print(f"Precisión General (Accuracy): {precision * 100:.2f}%")
    print("\nMatriz de Confusión (V. Positivos, F. Positivos, etc.):")
    print(confusion_matrix(y_test, predicciones))
    print("\nReporte Detallado:")
    print(classification_report(y_test, predicciones))

    print("\n--- 🧠 IMPORTANCIA DE VARIABLES ---")
    importancias = mejor_modelo.feature_importances_
    # Ordenamos de mayor a menor importancia
    importancias_ordenadas = sorted(zip(FEATURES, importancias), key=lambda x: x[1], reverse=True)
    for feature, importancia in importancias_ordenadas:
        print(f" - {feature}: {importancia * 100:.1f}%")

    # Guardar el modelo
    try:
        joblib.dump(mejor_modelo, MODELO_SALIDA)
        print(f"\n💾 ¡Modelo guardado con éxito como '{MODELO_SALIDA}'!")
        print("Tu servidor (app.py) ya puede usar esta nueva IA actualizada.")
    except Exception as e:
        print(f"❌ ERROR al guardar el modelo: {e}")

if __name__ == "__main__":
    entrenar_modelo()