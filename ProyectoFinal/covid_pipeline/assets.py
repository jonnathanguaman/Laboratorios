"""
COVID-19 Data Pipeline con Dagster
Paso 2: Lectura de Datos y Chequeos de Calidad
"""

import pandas as pd
import requests
from datetime import datetime, date
from typing import Dict, Any, List, Tuple
import logging
import os

from dagster import (
    asset,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
    Config,
    MaterializeResult,
    MetadataValue,
    get_dagster_logger
)

# Configuración
class CovidDataConfig(Config):
    """Configuración para la lectura de datos COVID"""
    url: str = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    max_retries: int = 3
    timeout: int = 30

@asset(
    description="Datos raw de COVID-19 desde Our World in Data"
)
def leer_datos() -> pd.DataFrame:
    """
    Lee los datos de COVID-19 desde la URL canónica de OWID o archivo local.
    
    Returns:
        pd.DataFrame: Dataset completo sin transformaciones
    """
    logger = get_dagster_logger()
    
    url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    
    try:
        logger.info(f"Intentando descargar datos desde: {url}")
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Leer CSV directamente desde la respuesta
        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
        
        logger.info(f"✓ Datos descargados exitosamente")
        logger.info(f"  - Filas: {len(df):,}")
        logger.info(f"  - Columnas: {len(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.warning(f"Error descargando datos: {e}")
        logger.info("Intentando usar archivo local...")
        
        # Fallback a archivo local
        if os.path.exists("covid.csv"):
            df = pd.read_csv("covid.csv")
            logger.info(f"✓ Usando archivo local: {len(df):,} filas, {len(df.columns)} columnas")
            return df
        else:
            raise Exception(f"No se pudo descargar datos y no existe archivo local: {e}")

@asset(
    description="Tabla de perfilado con métricas del EDA",
    deps=[leer_datos]
)
def tabla_perfilado(leer_datos: pd.DataFrame) -> pd.DataFrame:
    """
    Genera tabla de perfilado con métricas clave del análisis exploratorio.
    
    Returns:
        pd.DataFrame: Tabla con métricas de Ecuador y España
    """
    logger = get_dagster_logger()
    
    # Filtrar solo Ecuador y España
    df_filtered = leer_datos[leer_datos['country'].isin(['Ecuador', 'Spain'])].copy()
    
    # Métricas básicas
    ecuador_data = df_filtered[df_filtered['country'] == 'Ecuador']
    spain_data = df_filtered[df_filtered['country'] == 'Spain']
    
    # Crear tabla de perfilado
    perfilado = []
    
    # Información general
    perfilado.append({
        'Métrica': 'Información General',
        'Ecuador': f"{len(ecuador_data)} registros",
        'Spain': f"{len(spain_data)} registros", 
        'Total': f"{len(df_filtered)} registros"
    })
    
    # Tipos de datos
    perfilado.append({
        'Métrica': 'Tipos de Datos',
        'Ecuador': 'Disponible',
        'Spain': 'Disponible',
        'Total': f"{len(df_filtered.columns)}/{len(df_filtered.columns)} columnas principales"
    })
    
    # Estadísticas de new_cases para Ecuador
    if 'new_cases' in ecuador_data.columns:
        ecuador_cases = ecuador_data['new_cases'].dropna()
        if len(ecuador_cases) > 0:
            perfilado.append({
                'Métrica': 'Min new_cases - Ecuador',
                'Ecuador': str(ecuador_cases.min()),
                'Spain': '-',
                'Total': str(int(ecuador_cases.min()))
            })
            perfilado.append({
                'Métrica': 'Max new_cases - Ecuador',
                'Ecuador': str(ecuador_cases.max()),
                'Spain': '-',
                'Total': f"{int(ecuador_cases.max()):,}"
            })
    
    # Estadísticas de new_cases para España
    if 'new_cases' in spain_data.columns:
        spain_cases = spain_data['new_cases'].dropna()
        if len(spain_cases) > 0:
            perfilado.append({
                'Métrica': 'Min new_cases - Spain',
                'Ecuador': '-',
                'Spain': str(spain_cases.min()),
                'Total': str(int(spain_cases.min()))
            })
            perfilado.append({
                'Métrica': 'Max new_cases - Spain',
                'Ecuador': '-',
                'Spain': str(spain_cases.max()),
                'Total': f"{int(spain_cases.max()):,}"
            })
    
    # Crear DataFrame
    df_perfilado = pd.DataFrame(perfilado)
    
    # Guardar CSV
    df_perfilado.to_csv("tabla_perfilado.csv", index=False)
    
    logger.info(f"✓ Tabla de perfilado generada con {len(df_perfilado)} métricas")
    
    return df_perfilado

@asset_check(
    asset=leer_datos,
    name="fechas_no_futuras",
    description="Verificar que no hay fechas futuras en el dataset"
)
def fechas_no_futuras(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Chequeo: max(date) ≤ hoy (no fechas futuras)
    """
    logger = get_dagster_logger()
    
    try:
        # Convertir fechas
        df_temp = leer_datos.copy()
        df_temp['date'] = pd.to_datetime(df_temp['date'], errors='coerce')
        
        # Filtrar fechas válidas
        fechas_validas = df_temp['date'].dropna()
        
        if len(fechas_validas) == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="No se encontraron fechas válidas en el dataset"
            )
        
        fecha_maxima = fechas_validas.max()
        fecha_hoy = datetime.now().date()
        
        # Verificar si hay fechas futuras
        fechas_futuras = fechas_validas[fechas_validas.dt.date > fecha_hoy]
        filas_futuras = len(fechas_futuras)
        
        passed = filas_futuras == 0
        
        if passed:
            mensaje = f"✓ Todas las fechas son válidas. Fecha máxima: {fecha_maxima.date()}"
            severity = AssetCheckSeverity.WARN
        else:
            mensaje = f"❌ Se encontraron {filas_futuras} registros con fechas futuras"
            severity = AssetCheckSeverity.WARN  # WARN porque podrían ser proyecciones válidas
        
        logger.info(mensaje)
        
        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=mensaje,
            metadata={
                "fecha_maxima": MetadataValue.text(str(fecha_maxima.date())),
                "fecha_hoy": MetadataValue.text(str(fecha_hoy)),
                "filas_afectadas": MetadataValue.int(filas_futuras),
                "total_filas": MetadataValue.int(len(leer_datos))
            }
        )
        
    except Exception as e:
        logger.error(f"Error en check_fechas_no_futuras: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Error ejecutando el chequeo: {e}"
        )

@asset_check(
    asset=leer_datos,
    name="columnas_clave_no_nulas",
    description="Verificar que columnas clave no sean completamente nulas"
)
def columnas_clave_no_nulas(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Chequeo: Columnas clave no nulas: location/country, date, population
    """
    logger = get_dagster_logger()
    
    try:
        # Identificar columna de país (puede ser 'location' o 'country')
        columna_pais = None
        if 'location' in leer_datos.columns:
            columna_pais = 'location'
        elif 'country' in leer_datos.columns:
            columna_pais = 'country'
        
        # Definir columnas clave
        columnas_clave = ['date', 'population']
        if columna_pais:
            columnas_clave.insert(0, columna_pais)
        
        problemas = []
        
        for columna in columnas_clave:
            if columna not in leer_datos.columns:
                problemas.append(f"Falta columna: {columna}")
            elif leer_datos[columna].isna().all():
                problemas.append(f"Columna {columna} completamente nula")
        
        passed = len(problemas) == 0
        
        if passed:
            mensaje = "✓ Todas las columnas clave existen y tienen datos"
            severity = AssetCheckSeverity.WARN
        else:
            mensaje = f"❌ Problemas en columnas clave: {'; '.join(problemas)}"
            severity = AssetCheckSeverity.ERROR
        
        logger.info(mensaje)
        
        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=mensaje,
            metadata={
                "columnas_evaluadas": MetadataValue.text(str(columnas_clave)),
                "problemas": MetadataValue.text('; '.join(problemas) if problemas else "Ninguno"),
                "total_filas": MetadataValue.int(len(leer_datos))
            }
        )
        
    except Exception as e:
        logger.error(f"Error en columnas_clave_no_nulas: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Error ejecutando el chequeo: {e}"
        )

@asset(
    deps=[leer_datos],
    description="Resumen de todos los chequeos de calidad de datos"
)
def resumen_chequeos_calidad() -> pd.DataFrame:
    """
    Genera una tabla de resumen con todos los chequeos de calidad ejecutados.
    
    Returns:
        pd.DataFrame: Tabla con nombre_regla, estado, filas_afectadas, notas
    """
    logger = get_dagster_logger()
    
    resumen_data = [
        {
            "nombre_regla": "fechas_no_futuras",
            "estado": "EVALUADO",
            "filas_afectadas": "Ver metadata",
            "notas": "Verificar fechas futuras - podrían ser proyecciones válidas"
        },
        {
            "nombre_regla": "columnas_clave_no_nulas",
            "estado": "EVALUADO",
            "filas_afectadas": "Ver metadata",
            "notas": "Verificar existencia de columnas: country/location, date, population"
        }
    ]
    
    df_resumen = pd.DataFrame(resumen_data)
    
    # Guardar archivo
    df_resumen.to_csv('resumen_chequeos_dagster.csv', index=False)
    logger.info(f"✓ Resumen de chequeos generado: {len(df_resumen)} reglas evaluadas")
    
    return df_resumen

# =============================================================================
# PASO 3: PROCESAMIENTO DE DATOS
# =============================================================================

@asset(
    deps=[leer_datos],
    description="Datos procesados y limpios listos para análisis (Ecuador vs España)",
    metadata={
        "países_objetivo": "Ecuador, Spain",
        "columnas_procesadas": "location, date, new_cases, people_vaccinated, population"
    }
)
def datos_procesados(leer_datos: pd.DataFrame) -> pd.DataFrame:
    """
    Paso 3: Procesamiento de Datos
    
    Consume leer_datos y aplica las siguientes transformaciones:
    1. Eliminar filas con valores nulos en new_cases o people_vaccinated
    2. Eliminar duplicados si existen (documentar estrategia)
    3. Filtrar a Ecuador y España
    4. Seleccionar columnas esenciales
    5. Devolver DataFrame listo para métricas
    
    Returns:
        pd.DataFrame: Datos limpios y procesados
    """
    logger = get_dagster_logger()
    
    logger.info("🔄 Iniciando procesamiento de datos (Paso 3)")
    logger.info("=" * 50)
    
    # Copiar datos originales
    df = leer_datos.copy()
    
    # Identificar columna de país
    columna_pais = 'country' if 'country' in df.columns else 'location'
    logger.info(f"📍 Columna de país identificada: {columna_pais}")
    
    # Log estado inicial
    logger.info(f"📊 Estado inicial:")
    logger.info(f"   - Filas: {len(df):,}")
    logger.info(f"   - Columnas: {len(df.columns)}")
    logger.info(f"   - Países únicos: {df[columna_pais].nunique():,}")
    
    # 1. FILTRAR PAÍSES DE INTERÉS
    logger.info(f"\n🎯 Paso 1: Filtrando países de interés")
    paises_objetivo = ['Ecuador', 'Spain']
    
    df_filtrado = df[df[columna_pais].isin(paises_objetivo)].copy()
    
    logger.info(f"   - Países objetivo: {paises_objetivo}")
    logger.info(f"   - Filas después del filtro: {len(df_filtrado):,}")
    
    for pais in paises_objetivo:
        filas_pais = len(df_filtrado[df_filtrado[columna_pais] == pais])
        logger.info(f"   - {pais}: {filas_pais:,} registros")
    
    # 2. SELECCIONAR COLUMNAS ESENCIALES
    logger.info(f"\n📋 Paso 2: Seleccionando columnas esenciales")
    
    # Normalizar nombres de columnas si es necesario
    if columna_pais == 'country':
        df_filtrado = df_filtrado.rename(columns={'country': 'location'})
    
    columnas_esenciales = ['location', 'date', 'new_cases', 'people_vaccinated', 'population']
    
    # Verificar que todas las columnas existen
    columnas_faltantes = [col for col in columnas_esenciales if col not in df_filtrado.columns]
    if columnas_faltantes:
        raise ValueError(f"Columnas faltantes: {columnas_faltantes}")
    
    df_procesado = df_filtrado[columnas_esenciales].copy()
    
    logger.info(f"   - Columnas seleccionadas: {columnas_esenciales}")
    logger.info(f"   - Dimensiones: {df_procesado.shape}")
    
    # 3. ANALIZAR VALORES NULOS ANTES DE LIMPIEZA
    logger.info(f"\n🔍 Paso 3: Analizando valores nulos")
    
    nulos_antes = {}
    for col in ['new_cases', 'people_vaccinated']:
        nulos = df_procesado[col].isna().sum()
        porcentaje = (nulos / len(df_procesado)) * 100
        nulos_antes[col] = nulos
        logger.info(f"   - {col}: {nulos:,} nulos ({porcentaje:.1f}%)")
    
    # 4. ELIMINAR DUPLICADOS (ESTRATEGIA DOCUMENTADA)
    logger.info(f"\n🧹 Paso 4: Eliminando duplicados")
    
    duplicados_antes = df_procesado.duplicated(subset=['location', 'date']).sum()
    logger.info(f"   - Duplicados encontrados: {duplicados_antes:,}")
    
    if duplicados_antes > 0:
        # Estrategia: Mantener el último registro (más reciente/actualizado)
        logger.info(f"   - Estrategia: Mantener el último registro por (location, date)")
        df_procesado = df_procesado.drop_duplicates(subset=['location', 'date'], keep='last')
        logger.info(f"   - Filas después de eliminar duplicados: {len(df_procesado):,}")
    else:
        logger.info(f"   - ✓ No se encontraron duplicados")
    
    # 5. ELIMINAR FILAS CON VALORES NULOS EN COLUMNAS CRÍTICAS
    logger.info(f"\n🚮 Paso 5: Eliminando filas con valores nulos")
    
    filas_antes_limpieza = len(df_procesado)
    
    # Eliminar filas donde AMBAS columnas sean nulas (más conservador)
    condicion_eliminar = (
        df_procesado['new_cases'].isna() & 
        df_procesado['people_vaccinated'].isna()
    )
    
    filas_a_eliminar = condicion_eliminar.sum()
    logger.info(f"   - Estrategia: Eliminar solo filas donde AMBAS (new_cases Y people_vaccinated) sean nulas")
    logger.info(f"   - Filas a eliminar: {filas_a_eliminar:,}")
    
    df_procesado = df_procesado[~condicion_eliminar].copy()
    
    filas_despues_limpieza = len(df_procesado)
    filas_eliminadas = filas_antes_limpieza - filas_despues_limpieza
    
    logger.info(f"   - Filas eliminadas: {filas_eliminadas:,}")
    logger.info(f"   - Filas restantes: {filas_despues_limpieza:,}")
    
    # 6. CONVERTIR TIPOS DE DATOS
    logger.info(f"\n🔄 Paso 6: Convirtiendo tipos de datos")
    
    # Convertir fechas
    df_procesado['date'] = pd.to_datetime(df_procesado['date'])
    logger.info(f"   - date convertido a datetime")
    
    # Asegurar tipos numéricos
    for col in ['new_cases', 'people_vaccinated', 'population']:
        df_procesado[col] = pd.to_numeric(df_procesado[col], errors='coerce')
        logger.info(f"   - {col} convertido a numérico")
    
    # 7. ESTADÍSTICAS FINALES
    logger.info(f"\n📈 Paso 7: Estadísticas finales")
    
    logger.info(f"   - Filas finales: {len(df_procesado):,}")
    logger.info(f"   - Rango de fechas: {df_procesado['date'].min().date()} a {df_procesado['date'].max().date()}")
    
    for pais in paises_objetivo:
        df_pais = df_procesado[df_procesado['location'] == pais]
        logger.info(f"   - {pais}: {len(df_pais):,} registros")
        
        # Estadísticas por país
        casos_validos = df_pais['new_cases'].dropna()
        vacunas_validas = df_pais['people_vaccinated'].dropna()
        
        if len(casos_validos) > 0:
            logger.info(f"     - Casos válidos: {len(casos_validos):,} ({(len(casos_validos)/len(df_pais)*100):.1f}%)")
        if len(vacunas_validas) > 0:
            logger.info(f"     - Vacunas válidas: {len(vacunas_validas):,} ({(len(vacunas_validas)/len(df_pais)*100):.1f}%)")
    
    # 8. VALIDACIONES FINALES
    logger.info(f"\n✅ Paso 8: Validaciones finales")
    
    # Verificar que tenemos datos para ambos países
    paises_finales = df_procesado['location'].unique()
    if len(paises_finales) != 2:
        logger.warning(f"⚠️ Se esperaban 2 países, se encontraron: {list(paises_finales)}")
    
    # Verificar rango de fechas razonable
    fecha_min = df_procesado['date'].min()
    fecha_max = df_procesado['date'].max()
    dias_rango = (fecha_max - fecha_min).days
    
    logger.info(f"   - Países en dataset final: {list(paises_finales)}")
    logger.info(f"   - Rango temporal: {dias_rango} días")
    logger.info(f"   - Columnas finales: {list(df_procesado.columns)}")
    
    # Guardar archivo procesado para debugging
    archivo_salida = "datos_procesados_debug.csv"
    df_procesado.to_csv(archivo_salida, index=False)
    logger.info(f"   - Archivo debug guardado: {archivo_salida}")
    
    logger.info(f"\n🎉 ¡Procesamiento completado exitosamente!")
    logger.info("=" * 50)
    
    return df_procesado


@asset(
    deps=[datos_procesados],
    description="Métrica A: Incidencia acumulada a 7 días por 100 mil habitantes"
)
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la incidencia acumulada a 7 días por 100,000 habitantes.
    
    Fórmula:
    1. incidencia_diaria = (new_cases / population) * 100000
    2. incidencia_7d = promedio móvil de 7 días de incidencia_diaria
    
    Args:
        datos_procesados: DataFrame con datos procesados de COVID
        
    Returns:
        pd.DataFrame: Con columnas [fecha, país, incidencia_7d]
    """
    logger = get_dagster_logger()
    
    logger.info("🦠 CALCULANDO MÉTRICA A: INCIDENCIA 7 DÍAS")
    logger.info("=" * 50)
    
    df = datos_procesados.copy()
    
    # Verificar columnas necesarias
    columnas_necesarias = ['date', 'location', 'new_cases', 'population']
    columnas_faltantes = [col for col in columnas_necesarias if col not in df.columns]
    if columnas_faltantes:
        raise ValueError(f"Columnas faltantes para incidencia: {columnas_faltantes}")
    
    logger.info(f"📊 Datos de entrada: {len(df):,} filas, {len(df['location'].unique())} países")
    
    # Filtrar solo filas con datos válidos para el cálculo
    df_valido = df.dropna(subset=['new_cases', 'population']).copy()
    logger.info(f"📊 Datos válidos (sin nulos): {len(df_valido):,} filas")
    
    # Asegurar que new_cases >= 0 (no puede haber casos negativos)
    df_valido = df_valido[df_valido['new_cases'] >= 0].copy()
    logger.info(f"📊 Datos con casos >= 0: {len(df_valido):,} filas")
    
    resultados = []
    
    for pais in df_valido['location'].unique():
        logger.info(f"\n🌍 Procesando país: {pais}")
        
        df_pais = df_valido[df_valido['location'] == pais].copy()
        df_pais = df_pais.sort_values('date').reset_index(drop=True)
        
        logger.info(f"   - Registros para {pais}: {len(df_pais):,}")
        logger.info(f"   - Rango fechas: {df_pais['date'].min().date()} a {df_pais['date'].max().date()}")
        
        # Paso 1: Calcular incidencia diaria por 100,000 habitantes
        df_pais['incidencia_diaria'] = (df_pais['new_cases'] / df_pais['population']) * 100000
        
        # Estadísticas de incidencia diaria
        incidencia_max = df_pais['incidencia_diaria'].max()
        incidencia_promedio = df_pais['incidencia_diaria'].mean()
        logger.info(f"   - Incidencia diaria máxima: {incidencia_max:.2f}")
        logger.info(f"   - Incidencia diaria promedio: {incidencia_promedio:.2f}")
        
        # Paso 2: Calcular promedio móvil de 7 días
        df_pais['incidencia_7d'] = df_pais['incidencia_diaria'].rolling(
            window=7, 
            min_periods=1,  # Permitir cálculo incluso con menos de 7 días
            center=False    # Usar los 7 días anteriores (incluyendo el actual)
        ).mean()
        
        # Estadísticas del promedio móvil
        incidencia_7d_max = df_pais['incidencia_7d'].max()
        incidencia_7d_promedio = df_pais['incidencia_7d'].mean()
        logger.info(f"   - Incidencia 7d máxima: {incidencia_7d_max:.2f}")
        logger.info(f"   - Incidencia 7d promedio: {incidencia_7d_promedio:.2f}")
        
        # Preparar resultado para este país
        df_resultado_pais = df_pais[['date', 'location', 'incidencia_7d']].copy()
        df_resultado_pais.rename(columns={
            'date': 'fecha',
            'location': 'pais'
        }, inplace=True)
        
        # Redondear a 1 decimal para mejor legibilidad
        df_resultado_pais['incidencia_7d'] = df_resultado_pais['incidencia_7d'].round(1)
        
        resultados.append(df_resultado_pais)
        
        logger.info(f"   - ✓ Completado para {pais}: {len(df_resultado_pais):,} registros")
    
    # Combinar resultados de todos los países
    df_final = pd.concat(resultados, ignore_index=True)
    df_final = df_final.sort_values(['fecha', 'pais']).reset_index(drop=True)
    
    # Estadísticas finales
    logger.info(f"\n📈 ESTADÍSTICAS FINALES - INCIDENCIA 7D")
    logger.info(f"   - Total registros: {len(df_final):,}")
    logger.info(f"   - Países: {list(df_final['pais'].unique())}")
    logger.info(f"   - Rango fechas: {df_final['fecha'].min().date()} a {df_final['fecha'].max().date()}")
    logger.info(f"   - Incidencia 7d min: {df_final['incidencia_7d'].min():.1f}")
    logger.info(f"   - Incidencia 7d max: {df_final['incidencia_7d'].max():.1f}")
    logger.info(f"   - Incidencia 7d promedio: {df_final['incidencia_7d'].mean():.1f}")
    
    # Mostrar ejemplos de datos
    logger.info(f"\n📋 Ejemplos de datos:")
    for pais in df_final['pais'].unique():
        ejemplo = df_final[df_final['pais'] == pais].tail(3)
        logger.info(f"   - Últimos 3 registros de {pais}:")
        for _, row in ejemplo.iterrows():
            logger.info(f"     {row['fecha'].strftime('%Y-%m-%d')}: {row['incidencia_7d']:.1f}")
    
    # Guardar archivo de debug
    archivo_salida = "metrica_incidencia_7d_debug.csv"
    df_final.to_csv(archivo_salida, index=False)
    logger.info(f"\n💾 Archivo debug guardado: {archivo_salida}")
    
    logger.info(f"\n🎉 ¡Métrica incidencia 7d completada!")
    return df_final


@asset(
    deps=[datos_procesados],
    description="Métrica B: Factor de crecimiento semanal de casos"
)
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el factor de crecimiento semanal de casos.
    
    Fórmula:
    1. casos_semana_actual = suma(new_cases de los últimos 7 días)
    2. casos_semana_prev = suma(new_cases de los 7 días previos)
    3. factor_crec_7d = casos_semana_actual / casos_semana_prev
    
    Interpretación: >1 crecimiento, <1 decrecimiento, =1 estable
    
    Args:
        datos_procesados: DataFrame con datos procesados de COVID
        
    Returns:
        pd.DataFrame: Con columnas [semana_fin, país, casos_semana, factor_crec_7d]
    """
    logger = get_dagster_logger()
    
    logger.info("📈 CALCULANDO MÉTRICA B: FACTOR CRECIMIENTO 7 DÍAS")
    logger.info("=" * 55)
    
    df = datos_procesados.copy()
    
    # Verificar columnas necesarias
    columnas_necesarias = ['date', 'location', 'new_cases']
    columnas_faltantes = [col for col in columnas_necesarias if col not in df.columns]
    if columnas_faltantes:
        raise ValueError(f"Columnas faltantes para factor crecimiento: {columnas_faltantes}")
    
    logger.info(f"📊 Datos de entrada: {len(df):,} filas, {len(df['location'].unique())} países")
    
    # Filtrar solo filas con datos válidos
    df_valido = df.dropna(subset=['new_cases']).copy()
    df_valido = df_valido[df_valido['new_cases'] >= 0].copy()  # No casos negativos
    logger.info(f"📊 Datos válidos: {len(df_valido):,} filas")
    
    resultados = []
    
    for pais in df_valido['location'].unique():
        logger.info(f"\n🌍 Procesando país: {pais}")
        
        df_pais = df_valido[df_valido['location'] == pais].copy()
        df_pais = df_pais.sort_values('date').reset_index(drop=True)
        
        logger.info(f"   - Registros para {pais}: {len(df_pais):,}")
        logger.info(f"   - Rango fechas: {df_pais['date'].min().date()} a {df_pais['date'].max().date()}")
        
        # Necesitamos al menos 14 días para calcular factor de crecimiento
        if len(df_pais) < 14:
            logger.warning(f"   - ⚠️ Insuficientes datos para {pais} (necesarios: 14 días, disponibles: {len(df_pais)})")
            continue
        
        resultados_pais = []
        
        # Iterar sobre cada fecha (desde día 14 en adelante)
        for i in range(13, len(df_pais)):  # Empezar desde índice 13 (día 14)
            fecha_fin = df_pais.iloc[i]['date']
            
            # Semana actual: últimos 7 días (incluyendo fecha_fin)
            inicio_actual = i - 6  # 7 días hacia atrás desde i
            fin_actual = i + 1      # hasta i (inclusive)
            casos_semana_actual = df_pais.iloc[inicio_actual:fin_actual]['new_cases'].sum()
            
            # Semana previa: 7 días anteriores a la semana actual
            inicio_prev = i - 13    # 7 días antes del inicio_actual
            fin_prev = i - 6        # hasta inicio_actual (exclusive)
            casos_semana_prev = df_pais.iloc[inicio_prev:fin_prev]['new_cases'].sum()
            
            # Calcular factor de crecimiento
            if casos_semana_prev > 0:
                factor_crec_7d = casos_semana_actual / casos_semana_prev
            else:
                # Si semana previa = 0, asignar valor especial
                if casos_semana_actual > 0:
                    factor_crec_7d = float('inf')  # Crecimiento infinito (de 0 a algo)
                else:
                    factor_crec_7d = 1.0  # Ambas semanas = 0, consideramos estable
            
            # Guardar resultado
            resultados_pais.append({
                'semana_fin': fecha_fin,
                'pais': pais,
                'casos_semana': int(casos_semana_actual),
                'factor_crec_7d': factor_crec_7d
            })
        
        logger.info(f"   - Calculados {len(resultados_pais):,} factores de crecimiento")
        
        if resultados_pais:
            # Estadísticas para este país
            factores = [r['factor_crec_7d'] for r in resultados_pais if r['factor_crec_7d'] != float('inf')]
            if factores:
                factor_min = min(factores)
                factor_max = max(factores)
                factor_promedio = sum(factores) / len(factores)
                logger.info(f"   - Factor mín: {factor_min:.2f}, máx: {factor_max:.2f}, prom: {factor_promedio:.2f}")
            
            resultados.extend(resultados_pais)
        
        logger.info(f"   - ✓ Completado para {pais}")
    
    if not resultados:
        logger.error("❌ No se pudieron calcular factores de crecimiento para ningún país")
        return pd.DataFrame(columns=['semana_fin', 'pais', 'casos_semana', 'factor_crec_7d'])
    
    # Crear DataFrame final
    df_final = pd.DataFrame(resultados)
    df_final = df_final.sort_values(['semana_fin', 'pais']).reset_index(drop=True)
    
    # Manejar valores infinitos (reemplazar por valor alto pero finito)
    infinitos = (df_final['factor_crec_7d'] == float('inf')).sum()
    if infinitos > 0:
        logger.info(f"   - Reemplazando {infinitos} valores infinitos por 999.9")
        df_final['factor_crec_7d'] = df_final['factor_crec_7d'].replace(float('inf'), 999.9)
    
    # Redondear factor a 2 decimales
    df_final['factor_crec_7d'] = df_final['factor_crec_7d'].round(2)
    
    # Estadísticas finales
    logger.info(f"\n📈 ESTADÍSTICAS FINALES - FACTOR CRECIMIENTO 7D")
    logger.info(f"   - Total registros: {len(df_final):,}")
    logger.info(f"   - Países: {list(df_final['pais'].unique())}")
    logger.info(f"   - Rango fechas: {df_final['semana_fin'].min().date()} a {df_final['semana_fin'].max().date()}")
    logger.info(f"   - Factor mín: {df_final['factor_crec_7d'].min():.2f}")
    logger.info(f"   - Factor máx: {df_final['factor_crec_7d'].max():.2f}")
    logger.info(f"   - Factor promedio: {df_final['factor_crec_7d'].mean():.2f}")
    
    # Análisis de tendencias
    logger.info(f"\n📊 ANÁLISIS DE TENDENCIAS:")
    crecimiento = (df_final['factor_crec_7d'] > 1.0).sum()
    decrecimiento = (df_final['factor_crec_7d'] < 1.0).sum()
    estable = (df_final['factor_crec_7d'] == 1.0).sum()
    
    total = len(df_final)
    logger.info(f"   - Semanas en crecimiento (>1.0): {crecimiento:,} ({crecimiento/total*100:.1f}%)")
    logger.info(f"   - Semanas en decrecimiento (<1.0): {decrecimiento:,} ({decrecimiento/total*100:.1f}%)")
    logger.info(f"   - Semanas estables (=1.0): {estable:,} ({estable/total*100:.1f}%)")
    
    # Mostrar ejemplos por país
    logger.info(f"\n📋 Ejemplos de datos:")
    for pais in df_final['pais'].unique():
        ejemplo = df_final[df_final['pais'] == pais].tail(3)
        logger.info(f"   - Últimas 3 semanas de {pais}:")
        for _, row in ejemplo.iterrows():
            tendencia = "📈" if row['factor_crec_7d'] > 1 else "📉" if row['factor_crec_7d'] < 1 else "➡️"
            logger.info(f"     {row['semana_fin'].strftime('%Y-%m-%d')}: {row['casos_semana']:,} casos, factor {row['factor_crec_7d']:.2f} {tendencia}")
    
    # Guardar archivo de debug
    archivo_salida = "metrica_factor_crec_7d_debug.csv"
    df_final.to_csv(archivo_salida, index=False)
    logger.info(f"\n💾 Archivo debug guardado: {archivo_salida}")
    
    logger.info(f"\n🎉 ¡Métrica factor crecimiento 7d completada!")
    return df_final


# ============================================================================
# PASO 5: CHEQUEOS DE SALIDA (ASSET CHECKS PARA MÉTRICAS)
# ============================================================================

@asset_check(
    asset=metrica_incidencia_7d,
    description="Valida que la incidencia 7d esté en el rango esperado (0-2000)"
)
def chequeo_rango_incidencia_7d(metrica_incidencia_7d: pd.DataFrame) -> AssetCheckResult:
    """
    Valida que los valores de incidencia_7d estén en el rango esperado.
    Criterio: 0 ≤ incidencia_7d ≤ 2000 casos por 100K habitantes
    """
    logger = get_dagster_logger()
    
    logger.info("🔍 CHEQUEO: Rango de incidencia 7 días")
    
    # Parámetros de validación
    min_esperado = 0.0
    max_esperado = 2000.0
    
    # Análisis de valores
    total_registros = len(metrica_incidencia_7d)
    valores_validos = metrica_incidencia_7d[
        (metrica_incidencia_7d['incidencia_7d'] >= min_esperado) & 
        (metrica_incidencia_7d['incidencia_7d'] <= max_esperado)
    ]
    
    registros_validos = len(valores_validos)
    registros_invalidos = total_registros - registros_validos
    porcentaje_validos = (registros_validos / total_registros) * 100
    
    # Encontrar valores fuera de rango
    valores_bajo_minimo = metrica_incidencia_7d[metrica_incidencia_7d['incidencia_7d'] < min_esperado]
    valores_sobre_maximo = metrica_incidencia_7d[metrica_incidencia_7d['incidencia_7d'] > max_esperado]
    
    logger.info(f"   - Total registros: {total_registros:,}")
    logger.info(f"   - Registros válidos: {registros_validos:,} ({porcentaje_validos:.1f}%)")
    logger.info(f"   - Registros inválidos: {registros_invalidos:,}")
    logger.info(f"   - Rango esperado: [{min_esperado}, {max_esperado}]")
    
    if len(valores_bajo_minimo) > 0:
        valor_min = valores_bajo_minimo['incidencia_7d'].min()
        logger.info(f"   - Valores bajo mínimo: {len(valores_bajo_minimo):,} (mínimo encontrado: {valor_min:.1f})")
    
    if len(valores_sobre_maximo) > 0:
        valor_max = valores_sobre_maximo['incidencia_7d'].max()
        logger.info(f"   - Valores sobre máximo: {len(valores_sobre_maximo):,} (máximo encontrado: {valor_max:.1f})")
    
    # Determinar si pasa el chequeo
    if registros_invalidos == 0:
        return AssetCheckResult(
            passed=True,
            description=f"✅ Todos los valores de incidencia están en rango válido",
            metadata={
                "total_registros": total_registros,
                "registros_validos": registros_validos,
                "porcentaje_validos": round(porcentaje_validos, 2),
                "rango_min": min_esperado,
                "rango_max": max_esperado
            }
        )
    else:
        return AssetCheckResult(
            passed=False,
            description=f"❌ {registros_invalidos:,} valores fuera del rango esperado",
            metadata={
                "total_registros": total_registros,
                "registros_validos": registros_validos,
                "registros_invalidos": registros_invalidos,
                "porcentaje_validos": round(porcentaje_validos, 2),
                "valores_bajo_minimo": len(valores_bajo_minimo),
                "valores_sobre_maximo": len(valores_sobre_maximo),
                "rango_min": min_esperado,
                "rango_max": max_esperado
            }
        )


@asset_check(
    asset=metrica_incidencia_7d,
    description="Valida la completitud de datos de incidencia (sin valores nulos)"
)
def chequeo_completitud_incidencia_7d(metrica_incidencia_7d: pd.DataFrame) -> AssetCheckResult:
    """
    Valida que no haya valores nulos en las métricas de incidencia.
    """
    logger = get_dagster_logger()
    
    logger.info("🔍 CHEQUEO: Completitud de incidencia 7 días")
    
    total_registros = len(metrica_incidencia_7d)
    
    # Verificar nulos en cada columna
    nulos_fecha = metrica_incidencia_7d['fecha'].isna().sum()
    nulos_pais = metrica_incidencia_7d['pais'].isna().sum()
    nulos_incidencia = metrica_incidencia_7d['incidencia_7d'].isna().sum()
    
    total_nulos = nulos_fecha + nulos_pais + nulos_incidencia
    
    logger.info(f"   - Total registros: {total_registros:,}")
    logger.info(f"   - Nulos en fecha: {nulos_fecha:,}")
    logger.info(f"   - Nulos en país: {nulos_pais:,}")
    logger.info(f"   - Nulos en incidencia_7d: {nulos_incidencia:,}")
    logger.info(f"   - Total nulos: {total_nulos:,}")
    
    if total_nulos == 0:
        return AssetCheckResult(
            passed=True,
            description="✅ Datos completos - sin valores nulos",
            metadata={
                "total_registros": int(total_registros),
                "nulos_fecha": int(nulos_fecha),
                "nulos_pais": int(nulos_pais),
                "nulos_incidencia": int(nulos_incidencia)
            }
        )
    else:
        return AssetCheckResult(
            passed=False,
            description=f"❌ Se encontraron {total_nulos:,} valores nulos",
            metadata={
                "total_registros": int(total_registros),
                "nulos_fecha": int(nulos_fecha),
                "nulos_pais": int(nulos_pais),
                "nulos_incidencia": int(nulos_incidencia),
                "total_nulos": int(total_nulos)
            }
        )


@asset_check(
    asset=metrica_factor_crec_7d,
    description="Valida que el factor de crecimiento esté en rango razonable (0-50)"
)
def chequeo_rango_factor_crecimiento_7d(metrica_factor_crec_7d: pd.DataFrame) -> AssetCheckResult:
    """
    Valida que los valores de factor_crec_7d estén en un rango razonable.
    Criterio: 0 ≤ factor_crec_7d ≤ 50 (crecimiento hasta 50x se considera razonable)
    """
    logger = get_dagster_logger()
    
    logger.info("🔍 CHEQUEO: Rango de factor de crecimiento 7 días")
    
    # Parámetros de validación
    min_esperado = 0.0
    max_esperado = 50.0
    
    total_registros = len(metrica_factor_crec_7d)
    
    # Excluir valores especiales (999.9) que representan crecimiento infinito
    df_analisis = metrica_factor_crec_7d[metrica_factor_crec_7d['factor_crec_7d'] < 999.0].copy()
    registros_analisis = len(df_analisis)
    valores_infinitos = total_registros - registros_analisis
    
    # Análisis de valores en rango normal
    valores_validos = df_analisis[
        (df_analisis['factor_crec_7d'] >= min_esperado) & 
        (df_analisis['factor_crec_7d'] <= max_esperado)
    ]
    
    registros_validos = len(valores_validos)
    registros_invalidos = registros_analisis - registros_validos
    porcentaje_validos = (registros_validos / registros_analisis) * 100 if registros_analisis > 0 else 0
    
    # Encontrar valores fuera de rango
    valores_sobre_maximo = df_analisis[df_analisis['factor_crec_7d'] > max_esperado]
    
    logger.info(f"   - Total registros: {total_registros:,}")
    logger.info(f"   - Valores infinitos (999.9): {valores_infinitos:,}")
    logger.info(f"   - Registros para análisis: {registros_analisis:,}")
    logger.info(f"   - Registros válidos: {registros_validos:,} ({porcentaje_validos:.1f}%)")
    logger.info(f"   - Registros inválidos: {registros_invalidos:,}")
    logger.info(f"   - Rango esperado: [{min_esperado}, {max_esperado}]")
    
    if len(valores_sobre_maximo) > 0:
        valor_max = valores_sobre_maximo['factor_crec_7d'].max()
        logger.info(f"   - Valores sobre máximo: {len(valores_sobre_maximo):,} (máximo encontrado: {valor_max:.2f})")
    
    # Criterio de éxito: ≥95% de valores en rango normal
    umbral_exito = 95.0
    
    if porcentaje_validos >= umbral_exito:
        return AssetCheckResult(
            passed=True,
            description=f"✅ {porcentaje_validos:.1f}% de valores en rango válido (≥{umbral_exito}%)",
            metadata={
                "total_registros": int(total_registros),
                "registros_analisis": int(registros_analisis),
                "registros_validos": int(registros_validos),
                "porcentaje_validos": round(porcentaje_validos, 2),
                "valores_infinitos": int(valores_infinitos),
                "valores_sobre_maximo": int(len(valores_sobre_maximo)),
                "rango_min": min_esperado,
                "rango_max": max_esperado,
                "umbral_exito": umbral_exito
            }
        )
    else:
        return AssetCheckResult(
            passed=False,
            description=f"❌ Solo {porcentaje_validos:.1f}% de valores en rango válido (<{umbral_exito}%)",
            metadata={
                "total_registros": int(total_registros),
                "registros_analisis": int(registros_analisis),
                "registros_validos": int(registros_validos),
                "registros_invalidos": int(registros_invalidos),
                "porcentaje_validos": round(porcentaje_validos, 2),
                "valores_infinitos": int(valores_infinitos),
                "valores_sobre_maximo": int(len(valores_sobre_maximo)),
                "rango_min": min_esperado,
                "rango_max": max_esperado,
                "umbral_exito": umbral_exito
            }
        )


@asset_check(
    asset=metrica_factor_crec_7d,
    description="Valida la distribución de tendencias (crecimiento vs decrecimiento)"
)
def chequeo_distribucion_tendencias_7d(metrica_factor_crec_7d: pd.DataFrame) -> AssetCheckResult:
    """
    Valida que la distribución de tendencias sea razonable.
    Criterio: No más del 80% de registros en una sola tendencia (crecimiento o decrecimiento)
    """
    logger = get_dagster_logger()
    
    logger.info("🔍 CHEQUEO: Distribución de tendencias 7 días")
    
    total_registros = len(metrica_factor_crec_7d)
    
    # Clasificar tendencias (excluyendo valores infinitos)
    df_analisis = metrica_factor_crec_7d[metrica_factor_crec_7d['factor_crec_7d'] < 999.0].copy()
    
    crecimiento = (df_analisis['factor_crec_7d'] > 1.0).sum()
    decrecimiento = (df_analisis['factor_crec_7d'] < 1.0).sum()
    estable = (df_analisis['factor_crec_7d'] == 1.0).sum()
    infinitos = (metrica_factor_crec_7d['factor_crec_7d'] >= 999.0).sum()
    
    registros_analisis = len(df_analisis)
    
    # Calcular porcentajes
    pct_crecimiento = (crecimiento / registros_analisis) * 100 if registros_analisis > 0 else 0
    pct_decrecimiento = (decrecimiento / registros_analisis) * 100 if registros_analisis > 0 else 0
    pct_estable = (estable / registros_analisis) * 100 if registros_analisis > 0 else 0
    
    logger.info(f"   - Total registros: {total_registros:,}")
    logger.info(f"   - Registros para análisis: {registros_analisis:,}")
    logger.info(f"   - Crecimiento (>1.0): {crecimiento:,} ({pct_crecimiento:.1f}%)")
    logger.info(f"   - Decrecimiento (<1.0): {decrecimiento:,} ({pct_decrecimiento:.1f}%)")
    logger.info(f"   - Estable (=1.0): {estable:,} ({pct_estable:.1f}%)")
    logger.info(f"   - Infinitos (≥999): {infinitos:,}")
    
    # Criterio: Ninguna tendencia debe superar el 80%
    umbral_maximo = 80.0
    
    tendencia_dominante = max(pct_crecimiento, pct_decrecimiento, pct_estable)
    tendencia_nombre = ""
    
    if tendencia_dominante == pct_crecimiento:
        tendencia_nombre = "Crecimiento"
    elif tendencia_dominante == pct_decrecimiento:
        tendencia_nombre = "Decrecimiento"
    else:
        tendencia_nombre = "Estable"
    
    if tendencia_dominante <= umbral_maximo:
        return AssetCheckResult(
            passed=True,
            description=f"✅ Distribución balanceada - tendencia dominante: {tendencia_nombre} ({tendencia_dominante:.1f}%)",
            metadata={
                "total_registros": int(total_registros),
                "registros_analisis": int(registros_analisis),
                "crecimiento": int(crecimiento),
                "decrecimiento": int(decrecimiento),
                "estable": int(estable),
                "infinitos": int(infinitos),
                "pct_crecimiento": float(round(pct_crecimiento, 2)),
                "pct_decrecimiento": float(round(pct_decrecimiento, 2)),
                "pct_estable": float(round(pct_estable, 2)),
                "tendencia_dominante": tendencia_nombre,
                "pct_dominante": float(round(tendencia_dominante, 2)),
                "umbral_maximo": float(umbral_maximo)
            }
        )
    else:
        return AssetCheckResult(
            passed=False,
            description=f"⚠️ Distribución desbalanceada - {tendencia_nombre}: {tendencia_dominante:.1f}% (>{umbral_maximo}%)",
            metadata={
                "total_registros": int(total_registros),
                "registros_analisis": int(registros_analisis),
                "crecimiento": int(crecimiento),
                "decrecimiento": int(decrecimiento),
                "estable": int(estable),
                "infinitos": int(infinitos),
                "pct_crecimiento": float(round(pct_crecimiento, 2)),
                "pct_decrecimiento": float(round(pct_decrecimiento, 2)),
                "pct_estable": float(round(pct_estable, 2)),
                "tendencia_dominante": tendencia_nombre,
                "pct_dominante": float(round(tendencia_dominante, 2)),
                "umbral_maximo": float(umbral_maximo)
            }
        )


@asset_check(
    asset="metrica_incidencia_7d",
    description="Valida la consistencia temporal entre ambas métricas"
)
def chequeo_consistencia_temporal_metricas(
    metrica_incidencia_7d: pd.DataFrame
) -> AssetCheckResult:
    """
    Valida que las métricas tengan datos para períodos temporales consistentes.
    Criterio: Al menos 80% de solapamiento en fechas entre las dos métricas
    """
    logger = get_dagster_logger()
    
    logger.info("🔍 CHEQUEO: Consistencia temporal entre métricas")
    
    # Cargar datos del factor de crecimiento desde archivo
    try:
        df_factor = pd.read_csv("metrica_factor_crec_7d_debug.csv")
        df_factor['semana_fin'] = pd.to_datetime(df_factor['semana_fin'])
    except FileNotFoundError:
        return AssetCheckResult(
            passed=False,
            description="❌ No se encontró archivo de métrica factor crecimiento",
            metadata={"error": "metrica_factor_crec_7d_debug.csv no encontrado"}
        )
    
    # Convertir fechas si no están en datetime
    df_incidencia = metrica_incidencia_7d.copy()
    
    if not pd.api.types.is_datetime64_any_dtype(df_incidencia['fecha']):
        df_incidencia['fecha'] = pd.to_datetime(df_incidencia['fecha'])
    
    # Obtener fechas únicas por país
    resultados_paises = []
    
    for pais in ['Ecuador', 'Spain']:
        fechas_incidencia = set(df_incidencia[df_incidencia['pais'] == pais]['fecha'].dt.date)
        fechas_factor = set(df_factor[df_factor['pais'] == pais]['semana_fin'].dt.date)
        
        # Calcular solapamiento
        fechas_comunes = fechas_incidencia.intersection(fechas_factor)
        fechas_union = fechas_incidencia.union(fechas_factor)
        
        solapamiento = len(fechas_comunes) / len(fechas_union) * 100 if fechas_union else 0
        
        resultados_paises.append({
            'pais': pais,
            'fechas_incidencia': len(fechas_incidencia),
            'fechas_factor': len(fechas_factor),
            'fechas_comunes': len(fechas_comunes),
            'solapamiento_pct': solapamiento
        })
        
        logger.info(f"   - {pais}:")
        logger.info(f"     Fechas incidencia: {len(fechas_incidencia):,}")
        logger.info(f"     Fechas factor: {len(fechas_factor):,}")
        logger.info(f"     Fechas comunes: {len(fechas_comunes):,}")
        logger.info(f"     Solapamiento: {solapamiento:.1f}%")
    
    # Calcular solapamiento promedio
    solapamiento_promedio = sum(r['solapamiento_pct'] for r in resultados_paises) / len(resultados_paises)
    
    logger.info(f"   - Solapamiento promedio: {solapamiento_promedio:.1f}%")
    
    # Criterio de éxito
    umbral_minimo = 80.0
    
    if solapamiento_promedio >= umbral_minimo:
        return AssetCheckResult(
            passed=True,
            description=f"✅ Buena consistencia temporal - {solapamiento_promedio:.1f}% solapamiento",
            metadata={
                "solapamiento_promedio": round(solapamiento_promedio, 2),
                "umbral_minimo": umbral_minimo,
                "ecuador_solapamiento": round(resultados_paises[0]['solapamiento_pct'], 2),
                "spain_solapamiento": round(resultados_paises[1]['solapamiento_pct'], 2),
                "ecuador_fechas_incidencia": int(resultados_paises[0]['fechas_incidencia']),
                "ecuador_fechas_factor": int(resultados_paises[0]['fechas_factor']),
                "spain_fechas_incidencia": int(resultados_paises[1]['fechas_incidencia']),
                "spain_fechas_factor": int(resultados_paises[1]['fechas_factor'])
            }
        )
    else:
        return AssetCheckResult(
            passed=False,
            description=f"❌ Baja consistencia temporal - {solapamiento_promedio:.1f}% solapamiento (<{umbral_minimo}%)",
            metadata={
                "solapamiento_promedio": round(solapamiento_promedio, 2),
                "umbral_minimo": umbral_minimo,
                "ecuador_solapamiento": round(resultados_paises[0]['solapamiento_pct'], 2),
                "spain_solapamiento": round(resultados_paises[1]['solapamiento_pct'], 2),
                "ecuador_fechas_incidencia": int(resultados_paises[0]['fechas_incidencia']),
                "ecuador_fechas_factor": int(resultados_paises[0]['fechas_factor']),
                "spain_fechas_incidencia": int(resultados_paises[1]['fechas_incidencia']),
                "spain_fechas_factor": int(resultados_paises[1]['fechas_factor'])
            }
        )
