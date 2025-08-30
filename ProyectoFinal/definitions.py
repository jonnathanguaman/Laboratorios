"""
Configuración simplificada de Dagster para mostrar asset checks
"""

from dagster import Definitions
from covid_pipeline.assets import (
    leer_datos,
    tabla_perfilado,
    datos_procesados, 
    metrica_incidencia_7d,
    metrica_factor_crec_7d,
    resumen_chequeos_calidad,
    # Asset checks del paso 2
    fechas_no_futuras,
    columnas_clave_no_nulas,
    # Asset checks del paso 5
    chequeo_rango_incidencia_7d,
    chequeo_completitud_incidencia_7d,
    chequeo_rango_factor_crecimiento_7d,
    chequeo_distribucion_tendencias_7d,
    chequeo_consistencia_temporal_metricas,
)

from covid_pipeline.asset_chequeos_salida import chequeos_salida
from covid_pipeline.asset_reporte_excel import reporte_excel_covid

# Definición explícita de todos los assets y checks
defs = Definitions(
    assets=[
        leer_datos,
        tabla_perfilado,
        datos_procesados,
        metrica_incidencia_7d, 
        metrica_factor_crec_7d,
        resumen_chequeos_calidad,
        chequeos_salida,  # Asset resumen del Paso 5
        reporte_excel_covid,  # Asset exportación del Paso 6
    ],
    asset_checks=[
        # Checks paso 2
        fechas_no_futuras,
        columnas_clave_no_nulas,
        # Checks paso 5 
        chequeo_rango_incidencia_7d,
        chequeo_completitud_incidencia_7d,
        chequeo_rango_factor_crecimiento_7d,
        chequeo_distribucion_tendencias_7d,
        chequeo_consistencia_temporal_metricas,
    ]
)
