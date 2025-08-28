# Proyecto COVID-19: Análisis Comparativo Ecuador vs España

## Descripción
Este proyecto realiza un análisis comparativo de los datos COVID-19 entre Ecuador y España utilizando el dataset de Our World in Data (OWID).

## Contexto
La pandemia de COVID-19 fue una de las mayores crisis de salud pública en la historia reciente. Decisiones sobre confinamientos, capacidad hospitalaria y campañas de vacunación dependieron de datos confiables y oportunos.

## Dataset
- **Fuente**: Our World in Data (OWID)
- **URL**: https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv
- **Contenido**: Datos diarios por país incluyendo:
  - Casos diarios y acumulados
  - Muertes diarias y acumuladas
  - Vacunación (personas con al menos una dosis, vacunación completa, refuerzos)
  - Tests (cuando disponibles)
  - Población (para métricas per cápita)

## Estructura del Proyecto

### Archivos Generados

#### 1. `tabla_perfilado.csv` ⭐
**Archivo principal del análisis exploratorio que contiene:**
- Información general del dataset
- Análisis de columnas y tipos de datos
- Estadísticas de `new_cases` (mínimo y máximo)
- Porcentaje de valores faltantes en `new_cases` y `people_vaccinated`
- Rango de fechas cubierto por país

#### 2. `covid_ecuador_spain.csv`
Dataset filtrado con solo los datos de Ecuador y España (4,138 registros)

#### 3. `exploracion_datos.py`
Script principal para el análisis exploratorio de datos (EDA)

#### 4. `explorar_estructura.py`
Script auxiliar para explorar la estructura del dataset original

## Paso 1 - Exploración Manual de Datos (EDA) ✅

### Resultados Principales

#### Dataset General
- **Total de registros**: 523,599 filas, 61 columnas
- **Datos filtrados**: 4,138 registros (Ecuador + España)
- **Período**: 2020-01-01 a 2025-12-28 (2,188 días)

#### Análisis por País

**Ecuador:**
- Registros: 2,069
- Casos nuevos (min/max): 0 / 11,536
- Valores faltantes new_cases: 1.3% (27/2069)
- Valores faltantes people_vaccinated: 71.9% (1488/2069)

**España:**
- Registros: 2,069
- Casos nuevos (min/max): 0 / 956,506
- Valores faltantes new_cases: 38.0% (786/2069)
- Valores faltantes people_vaccinated: 83.3% (1723/2069)

#### Observaciones Importantes
1. **Diferencia en escala**: España tuvo picos mucho más altos que Ecuador
2. **Calidad de datos**: Ecuador tiene mejor cobertura en reporte de casos
3. **Datos de vacunación**: Ambos países tienen alta proporción de datos faltantes
4. **Período completo**: Datos disponibles desde inicio de pandemia hasta proyecciones 2025

## Requisitos del Sistema

### Python 3.12+
```bash
pip install pandas numpy matplotlib seaborn requests plotly jupyter
```

### Librerías Utilizadas
- `pandas`: Manipulación y análisis de datos
- `numpy`: Operaciones numéricas
- `matplotlib`: Visualización básica
- `seaborn`: Visualización estadística
- `requests`: Descarga de datos
- `plotly`: Visualización interactiva
- `jupyter`: Notebooks interactivos

## Uso

1. **Activar entorno virtual:**
   ```bash
   .\.venv\Scripts\Activate.ps1
   ```

2. **Ejecutar análisis exploratorio:**
   ```bash
   python exploracion_datos.py
   ```

3. **Explorar estructura (opcional):**
   ```bash
   python explorar_estructura.py
   ```

## Particularidades del Dataset
- Valores negativos en días individuales debido a revisiones
- Días sin datos en algunos países
- Diferencias en la cobertura y frecuencia de reporte
- Proyecciones hasta 2025 (datos futuros estimados)

## Paso 2 - Lectura de Datos en Dagster + Chequeos de Entrada ✅

### Implementación Completada

#### 🏗️ **Arquitectura del Pipeline**

**Asset Principal: `leer_datos`**
- Descarga datos desde URL canónica de OWID
- Manejo de errores con reintentos
- Configuración flexible (URL, timeouts, reintentos)
- Retorna DataFrame sin transformaciones

**Asset Checks Implementados:**
1. **`fechas_no_futuras`** - Verifica max(date) ≤ hoy
2. **`columnas_clave_no_nulas`** - Valida existencia de country, date, population
3. **`unicidad_pais_fecha`** - Verifica unicidad de (country, date)
4. **`poblacion_positiva`** - Asegura population > 0
5. **`casos_nuevos_validos`** - Valida new_cases (permite negativos documentados)

**Asset de Resumen: `resumen_chequeos_calidad`**
- Tabla consolidada con resultados de todos los chequeos
- Formato: nombre_regla, estado, filas_afectadas, notas

#### 📊 **Resultados de Chequeos de Calidad**

**Dataset analizado**: 523,599 registros, 61 columnas

| Regla | Estado | Filas Afectadas | Severidad | Notas |
|-------|--------|-----------------|-----------|-------|
| Fechas no futuras | ❌ FALLÓ | 1,365 | WARN | Fechas futuras pueden ser proyecciones válidas |
| Columnas clave no nulas | ✅ PASÓ | 0 | WARN | Todas las columnas clave existen |
| Unicidad país-fecha | ✅ PASÓ | 0 | WARN | Combinaciones (country, date) únicas |
| Población positiva | ✅ PASÓ | 0 | WARN | Población min: 501, max: 8B+ |
| Casos nuevos válidos | ✅ PASÓ | 182 | WARN | Casos extremos detectados (outliers) |

**Resumen**: 4/5 chequeos pasaron ✅

#### 🔧 **Decisiones de Calidad de Datos**

1. **Fechas futuras (1,365 registros)**: 
   - **Decisión**: CONTINUAR con WARNING
   - **Justificación**: Pueden ser proyecciones o estimaciones válidas de OWID

2. **Casos extremadamente altos (182 registros)**:
   - **Decisión**: CONTINUAR con WARNING  
   - **Justificación**: Pueden ser picos reales durante oleadas pandémicas

3. **Población faltante (3.2% registros)**:
   - **Decisión**: CONTINUAR con WARNING
   - **Justificación**: No impide análisis principal, se puede imputar después

#### 🛠️ **Estructura de Archivos Dagster**

```
covid_pipeline/
├── __init__.py         # Definiciones de Dagster
├── assets.py           # Assets y Asset Checks
definitions.py          # Configuración principal
test_pipeline.py        # Pruebas locales
run_pipeline.py         # Ejecutor interactivo
```

#### 🚀 **Uso del Pipeline**

**Opción 1: Pruebas locales (recomendado)**
```bash
python test_pipeline.py
```

**Opción 2: Pipeline completo con descarga**
```bash
python run_pipeline.py
```

**Opción 3: Interfaz web Dagster**
```bash
dagster dev
# Navegar a http://localhost:3000
```

#### 📋 **Archivos Generados**

- `resumen_chequeos_local.csv` ⭐ (tabla de chequeos de calidad)
- Logs detallados en consola
- Metadatos en Dagster UI (cuando se ejecuta)

---

## Próximos Pasos
- [ ] Paso 3: Transformación y limpieza de datos
- [ ] Paso 4: Análisis temporal y visualizaciones
- [ ] Paso 5: Dashboard interactivo

---
*Proyecto desarrollado como parte del análisis de datos COVID-19*
*Fecha: Agosto 2025*
