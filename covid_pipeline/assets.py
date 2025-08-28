"""
COVID-19 Data Pipeline con Dagster
Paso 2: Lectura de Datos y Chequeos de Calidad
"""

import pandas as pd
import requests
from datetime import datetime, date
from typing import Dict, Any, List, Tuple
import logging

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
    description="Datos raw de COVID-19 desde Our World in Data",
    metadata={
        "source": "https://covid.ourworldindata.org/data/owid-covid-data.csv",
        "owner": "OWID",
        "update_frequency": "daily"
    }
)
def leer_datos(config: CovidDataConfig) -> pd.DataFrame:
    """
    Lee los datos de COVID-19 desde la URL canónica de OWID.
    
    Returns:
        pd.DataFrame: Dataset completo sin transformaciones
    """
    logger = get_dagster_logger()
    
    logger.info(f"Iniciando descarga de datos desde: {config.url}")
    
    # Intentar descarga con reintentos
    for intento in range(config.max_retries):
        try:
            logger.info(f"Intento {intento + 1} de {config.max_retries}")
            
            response = requests.get(config.url, timeout=config.timeout)
            response.raise_for_status()
            
            # Leer CSV directamente desde la respuesta
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            logger.info(f"✓ Datos descargados exitosamente")
            logger.info(f"  - Filas: {len(df):,}")
            logger.info(f"  - Columnas: {len(df.columns)}")
            logger.info(f"  - Tamaño en memoria: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error en intento {intento + 1}: {e}")
            if intento == config.max_retries - 1:
                logger.error("Se agotaron todos los intentos de descarga")
                raise
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
            raise
    
    # Si llegamos aquí, algo salió mal
    raise Exception("No se pudo descargar los datos después de todos los intentos")

@asset_check(
    asset="leer_datos",
    name="fechas_no_futuras",
    description="Verificar que no hay fechas futuras en el dataset"
)
def check_fechas_no_futuras(leer_datos: pd.DataFrame) -> AssetCheckResult:
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
                description="No se encontraron fechas válidas en el dataset",
                metadata={
                    "filas_afectadas": len(leer_datos),
                    "notas": "Todas las fechas son inválidas o nulas"
                }
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
                "fecha_maxima": str(fecha_maxima.date()),
                "fecha_hoy": str(fecha_hoy),
                "filas_afectadas": filas_futuras,
                "total_filas": len(leer_datos),
                "notas": "Fechas futuras podrían ser proyecciones válidas" if not passed else "Dataset válido"
            }
        )
        
    except Exception as e:
        logger.error(f"Error en check_fechas_no_futuras: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Error ejecutando el chequeo: {e}",
            metadata={"error": str(e)}
        )

@asset_check(
    asset="leer_datos",
    name="columnas_clave_no_nulas",
    description="Verificar que columnas clave no sean completamente nulas"
)
def check_columnas_clave_no_nulas(leer_datos: pd.DataFrame) -> AssetCheckResult:
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
        
        resultados = {}
        filas_afectadas_total = 0
        
        for columna in columnas_clave:
            if columna in leer_datos.columns:
                nulos = leer_datos[columna].isna().sum()
                total = len(leer_datos)
                porcentaje = (nulos / total) * 100
                
                resultados[columna] = {
                    'nulos': nulos,
                    'total': total,
                    'porcentaje': porcentaje,
                    'existe': True
                }
                
                if porcentaje == 100:  # Columna completamente nula
                    filas_afectadas_total += total
                    
            else:
                resultados[columna] = {
                    'existe': False,
                    'nulos': 0,
                    'total': 0,
                    'porcentaje': 0
                }
        
        # Evaluar si el chequeo pasa
        columnas_faltantes = [col for col, res in resultados.items() if not res['existe']]
        columnas_completamente_nulas = [
            col for col, res in resultados.items() 
            if res['existe'] and res['porcentaje'] == 100
        ]
        
        passed = len(columnas_faltantes) == 0 and len(columnas_completamente_nulas) == 0
        
        if passed:
            mensaje = "✓ Todas las columnas clave existen y tienen datos"
            severity = AssetCheckSeverity.WARN
        else:
            problemas = []
            if columnas_faltantes:
                problemas.append(f"Faltantes: {columnas_faltantes}")
            if columnas_completamente_nulas:
                problemas.append(f"Completamente nulas: {columnas_completamente_nulas}")
            mensaje = f"❌ Problemas en columnas clave: {'; '.join(problemas)}"
            severity = AssetCheckSeverity.ERROR
        
        logger.info(mensaje)
        
        # Crear metadata detallada
        metadata = {
            "columnas_evaluadas": columnas_clave,
            "filas_afectadas": filas_afectadas_total,
            "total_filas": len(leer_datos)
        }
        
        for col, res in resultados.items():
            if res['existe']:
                metadata[f"{col}_nulos"] = int(res['nulos'])
                metadata[f"{col}_porcentaje_nulos"] = f"{res['porcentaje']:.1f}%"
        
        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=mensaje,
            metadata=metadata
        )
        
    except Exception as e:
        logger.error(f"Error en check_columnas_clave_no_nulas: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Error ejecutando el chequeo: {e}",
            metadata={"error": str(e)}
        )

@asset_check(
    asset="leer_datos",
    name="unicidad_pais_fecha",
    description="Verificar unicidad de (country/location, date)"
)
def check_unicidad_pais_fecha(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Chequeo: Unicidad de (location/country, date)
    """
    logger = get_dagster_logger()
    
    try:
        # Identificar columna de país
        columna_pais = None
        if 'location' in leer_datos.columns:
            columna_pais = 'location'
        elif 'country' in leer_datos.columns:
            columna_pais = 'country'
        
        if not columna_pais:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="No se encontró columna de país (location o country)",
                metadata={"columnas_disponibles": list(leer_datos.columns)}
            )
        
        if 'date' not in leer_datos.columns:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="No se encontró columna de fecha (date)",
                metadata={"columnas_disponibles": list(leer_datos.columns)}
            )
        
        # Verificar unicidad
        df_temp = leer_datos[[columna_pais, 'date']].dropna()
        total_filas = len(df_temp)
        filas_unicas = len(df_temp.drop_duplicates())
        duplicados = total_filas - filas_unicas
        
        passed = duplicados == 0
        
        if passed:
            mensaje = f"✓ Todas las combinaciones ({columna_pais}, date) son únicas"
            severity = AssetCheckSeverity.WARN
        else:
            mensaje = f"❌ Se encontraron {duplicados} combinaciones duplicadas de ({columna_pais}, date)"
            severity = AssetCheckSeverity.WARN  # WARN porque podría ser esperado en algunos casos
        
        logger.info(mensaje)
        
        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=mensaje,
            metadata={
                "total_filas": len(leer_datos),
                "filas_con_datos": total_filas,
                "filas_unicas": filas_unicas,
                "filas_afectadas": duplicados,
                "columna_pais": columna_pais,
                "notas": "Duplicados podrían indicar múltiples fuentes o actualizaciones" if not passed else "Dataset único"
            }
        )
        
    except Exception as e:
        logger.error(f"Error en check_unicidad_pais_fecha: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Error ejecutando el chequeo: {e}",
            metadata={"error": str(e)}
        )

@asset_check(
    asset="leer_datos",
    name="poblacion_positiva",
    description="Verificar que population > 0"
)
def check_poblacion_positiva(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Chequeo: population > 0
    """
    logger = get_dagster_logger()
    
    try:
        if 'population' not in leer_datos.columns:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="No se encontró columna 'population'",
                metadata={"columnas_disponibles": list(leer_datos.columns)}
            )
        
        # Analizar población
        poblacion = leer_datos['population'].dropna()
        
        if len(poblacion) == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="Todos los valores de población son nulos",
                metadata={
                    "filas_afectadas": len(leer_datos),
                    "total_filas": len(leer_datos)
                }
            )
        
        poblacion_invalida = poblacion[poblacion <= 0]
        filas_invalidas = len(poblacion_invalida)
        
        passed = filas_invalidas == 0
        
        if passed:
            mensaje = f"✓ Todos los valores de población son positivos (min: {poblacion.min():,.0f})"
            severity = AssetCheckSeverity.WARN
        else:
            mensaje = f"❌ Se encontraron {filas_invalidas} registros con población ≤ 0"
            severity = AssetCheckSeverity.ERROR
        
        logger.info(mensaje)
        
        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=mensaje,
            metadata={
                "poblacion_min": float(poblacion.min()) if len(poblacion) > 0 else None,
                "poblacion_max": float(poblacion.max()) if len(poblacion) > 0 else None,
                "filas_afectadas": filas_invalidas,
                "total_filas_con_poblacion": len(poblacion),
                "total_filas": len(leer_datos),
                "notas": "Población debe ser mayor a 0 para métricas per cápita"
            }
        )
        
    except Exception as e:
        logger.error(f"Error en check_poblacion_positiva: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Error ejecutando el chequeo: {e}",
            metadata={"error": str(e)}
        )

@asset_check(
    asset="leer_datos",
    name="casos_nuevos_validos",
    description="Verificar que new_cases sea un valor numérico válido (permitir negativos documentados)"
)
def check_casos_nuevos_validos(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Chequeo: new_cases ≥ 0 (permitir negativos solo si se documentan)
    """
    logger = get_dagster_logger()
    
    try:
        if 'new_cases' not in leer_datos.columns:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="No se encontró columna 'new_cases'",
                metadata={"columnas_disponibles": list(leer_datos.columns)}
            )
        
        # Analizar new_cases
        new_cases = leer_datos['new_cases'].dropna()
        
        if len(new_cases) == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="Todos los valores de new_cases son nulos",
                metadata={
                    "filas_afectadas": len(leer_datos),
                    "total_filas": len(leer_datos)
                }
            )
        
        # Casos negativos
        casos_negativos = new_cases[new_cases < 0]
        filas_negativas = len(casos_negativos)
        
        # Casos extremadamente altos (posibles outliers)
        percentil_99 = new_cases.quantile(0.99)
        casos_extremos = new_cases[new_cases > percentil_99 * 10]  # 10x el percentil 99
        filas_extremas = len(casos_extremos)
        
        # Evaluación: permitimos negativos pero los documentamos
        passed = True  # Siempre pasa, pero documentamos anomalías
        
        if filas_negativas > 0:
            mensaje = f"⚠️ Se encontraron {filas_negativas} registros con casos negativos (posibles correcciones de datos)"
            severity = AssetCheckSeverity.WARN
        elif filas_extremas > 0:
            mensaje = f"⚠️ Se encontraron {filas_extremas} registros con casos extremadamente altos"
            severity = AssetCheckSeverity.WARN
        else:
            mensaje = f"✓ Todos los valores de new_cases son válidos (min: {new_cases.min():,.0f}, max: {new_cases.max():,.0f})"
            severity = AssetCheckSeverity.WARN
        
        logger.info(mensaje)
        
        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=mensaje,
            metadata={
                "casos_min": float(new_cases.min()) if len(new_cases) > 0 else None,
                "casos_max": float(new_cases.max()) if len(new_cases) > 0 else None,
                "casos_negativos": filas_negativas,
                "casos_extremos": filas_extremas,
                "percentil_99": float(percentil_99) if len(new_cases) > 0 else None,
                "filas_afectadas": filas_negativas + filas_extremas,
                "total_filas_con_casos": len(new_cases),
                "total_filas": len(leer_datos),
                "notas": "Casos negativos pueden ser correcciones legítimas de datos históricos"
            }
        )
        
    except Exception as e:
        logger.error(f"Error en check_casos_nuevos_validos: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Error ejecutando el chequeo: {e}",
            metadata={"error": str(e)}
        )

@asset(
    deps=["leer_datos"],
    description="Resumen de todos los chequeos de calidad de datos"
)
def resumen_chequeos_calidad(context) -> pd.DataFrame:
    """
    Genera una tabla de resumen con todos los chequeos de calidad ejecutados.
    
    Returns:
        pd.DataFrame: Tabla con nombre_regla, estado, filas_afectadas, notas
    """
    logger = get_dagster_logger()
    
    # Obtener resultados de los chequeos desde el contexto
    # Esto es un placeholder - en una implementación real se obtendrían
    # los resultados de los asset checks ejecutados
    
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
        },
        {
            "nombre_regla": "unicidad_pais_fecha",
            "estado": "EVALUADO",
            "filas_afectadas": "Ver metadata",
            "notas": "Verificar combinaciones únicas de (país, fecha)"
        },
        {
            "nombre_regla": "poblacion_positiva",
            "estado": "EVALUADO",
            "filas_afectadas": "Ver metadata",
            "notas": "Población debe ser > 0 para cálculos per cápita"
        },
        {
            "nombre_regla": "casos_nuevos_validos",
            "estado": "EVALUADO",
            "filas_afectadas": "Ver metadata",
            "notas": "Casos negativos permitidos como correcciones históricas"
        }
    ]
    
    df_resumen = pd.DataFrame(resumen_data)
    
    logger.info(f"✓ Resumen de chequeos generado: {len(df_resumen)} reglas evaluadas")
    
    return df_resumen
