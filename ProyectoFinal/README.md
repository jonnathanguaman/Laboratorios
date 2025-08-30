# COVID-19 Data Pipeline con Dagster

Pipeline de análisis epidemiológico que compara datos COVID-19 entre Ecuador y España utilizando Dagster para la orquestación de datos.

## 🚀 Ejecución en GitHub Codespaces

### Opción 1: Ejecución Automática
1. Abre el proyecto en Codespaces
2. Espera a que se complete la instalación automática
3. Ejecuta: `dagster dev`
4. Abre el puerto 3000 para acceder a la interfaz web

### Opción 2: Instalación Manual
```bash
# Instalar dependencias
pip install -e .

# Iniciar Dagster
dagster dev
```

## 🏗️ Estructura del Pipeline

### Assets Principales
1. **leer_datos** - Descarga datos COVID-19 desde OWID
2. **tabla_perfilado** - Genera análisis exploratorio automático  
3. **datos_procesados** - Limpia y filtra datos de Ecuador/España
4. **metrica_incidencia_7d** - Calcula incidencia por 100k habitantes
5. **metrica_factor_crec_7d** - Calcula factor de crecimiento semanal
6. **resumen_chequeos_calidad** - Resumen de validaciones
7. **chequeos_salida** - Asset del Paso 5 con validaciones finales
8. **reporte_excel_covid** - Exporta resultados a Excel y CSV

### Asset Checks
- Validación de fechas futuras
- Verificación de columnas críticas
- Controles de calidad epidemiológica
- Validación de rangos y consistencia

## 📊 Resultados Generados

Al ejecutar el pipeline completo se generan:
- `reporte_final_covid_ecuador_spain.xlsx` - Reporte Excel con 4 hojas
- `datos_procesados_final.csv` - Datos limpios Ecuador/España
- `metrica_incidencia_7d_final.csv` - Incidencia semanal
- `metrica_factor_crec_7d_final.csv` - Factor de crecimiento
- `resumen_ejecutivo_final.csv` - Resumen con estadísticas clave
- `tabla_perfilado.csv` - Análisis exploratorio

## 🔧 Comandos Útiles

```bash
# Iniciar interfaz web
dagster dev

# Materializar asset específico  
dagster asset materialize --select leer_datos

# Materializar todo el pipeline
dagster asset materialize --select "*"

# Ver logs
dagster asset materialize --select "*" --tags-file tags.yaml
```

## 📁 Estructura del Proyecto

```
covid-pipeline/
├── .devcontainer/          # Configuración Codespaces
├── covid_pipeline/         # Código principal del pipeline
│   ├── assets.py          # Assets principales y checks
│   ├── asset_chequeos_salida.py  # Asset resumen Paso 5
│   └── asset_reporte_excel.py    # Asset exportación Paso 6
├── definitions.py         # Configuración Dagster
├── covid.csv             # Dataset local (fallback)
├── pyproject.toml        # Dependencias Python
└── README.md            # Esta documentación
```

## 🎯 Pasos Implementados

✅ **Paso 1**: EDA - Análisis exploratorio automático  
✅ **Paso 2**: Pipeline Dagster con lectura y validaciones  
✅ **Paso 3**: Procesamiento y limpieza de datos  
✅ **Paso 4**: Métricas epidemiológicas (incidencia + crecimiento)  
✅ **Paso 5**: Chequeos de calidad y validaciones  
✅ **Paso 6**: Exportación de resultados (Excel + CSV)

## 🌐 Acceso en Codespaces

1. La interfaz de Dagster estará disponible en el puerto 3000
2. Los archivos se generarán en el directorio raíz del proyecto
3. Todos los assets son regenerables ejecutando el pipeline

## ⚡ Desarrollo

Para desarrollo local:
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows
pip install -e .
dagster dev
```
