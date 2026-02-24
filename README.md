# Suite Financiera y de Auditoría

Esta aplicación es una herramienta integral diseñada para facilitar la conciliación de registros CFDI con auxiliares contables y realizar auditorías automatizadas sobre estados de cuenta en formato PDF.

## 🚀 Características

### 1. Conciliador IA
- **Matching Multicapa**: Realiza 7 pasos de comparación (UUID, Folio, Monto exacto, Monto con tolerancia, etc.).
- **Dashboard de Resultados**: Resumen visual de cuántas coincidencias se encontraron en cada etapa.
- **Resumen Analítico**: Genera un reporte cualitativo sobre el estado de la conciliación.
- **Exportación**: Genera un archivo Excel con los resultados clasificados por nivel de confianza.

### 2. Auditoría GSM
- **Indexación de PDFs**: Busca montos específicos dentro de un conjunto de archivos PDF.
- **Marcado Automático**: Subraya en verde los montos encontrados en los estados de cuenta y añade una referencia cruzada.
- **Reporte de Faltantes**: Genera un archivo de texto con los movimientos que no se localizaron en los PDFs.

## 📁 Estructura del Proyecto

```
.
├── app.py                # Servidor Flask principal
├── modules/              # Lógica de negocio
│   ├── modulo_auditoria.py
│   └── modulo_conciliacion.py
├── training/             # Entrenamiento del modelo
│   ├── train_model.py
│   └── entrenamiento.csv
├── models/               # Modelos de IA guardados
├── templates/            # Vistas HTML (Flask)
├── uploads/              # Carpeta temporal de subida
├── outputs/              # Carpeta de resultados procesados
└── requirements.txt      # Dependencias del proyecto
```

## 🛠️ Instalación

1. Clona el repositorio.
2. Crea un entorno virtual: `python -m venv venv`.
3. Activa el entorno virtual.
4. Instala las dependencias: `pip install -r requirements.txt`.

## 🖥️ Uso

Inicia el servidor con:
```bash
python app.py
```
Accede a `http://localhost:5001`. El PIN de acceso predeterminado es `190805`.

## 📄 Licencia
Privado - Todos los derechos reservados.
