"""
Módulo de assets para el pipeline de COVID-19
"""

# Solo importar los assets, no definir Definitions aquí
from .assets import (
    leer_datos,
    resumen_chequeos_calidad,
    datos_procesados,  # Nuevo asset del Paso 3
    metrica_incidencia_7d,  # Nuevo asset del Paso 4
    metrica_factor_crec_7d,  # Nuevo asset del Paso 4
    fechas_no_futuras,
    columnas_clave_no_nulas,
    # Asset checks del Paso 5
    chequeo_rango_incidencia_7d,
    chequeo_completitud_incidencia_7d,
    chequeo_rango_factor_crecimiento_7d,
    chequeo_distribucion_tendencias_7d,
    chequeo_consistencia_temporal_metricas,
)

__all__ = [
    "leer_datos",
    "resumen_chequeos_calidad",
    "datos_procesados",  # Nuevo asset del Paso 3
    "metrica_incidencia_7d",  # Nuevo asset del Paso 4
    "metrica_factor_crec_7d",  # Nuevo asset del Paso 4
    "fechas_no_futuras",
    "columnas_clave_no_nulas",
    # Asset checks del Paso 5
    "chequeo_rango_incidencia_7d",
    "chequeo_completitud_incidencia_7d",
    "chequeo_rango_factor_crecimiento_7d",
    "chequeo_distribucion_tendencias_7d",
    "chequeo_consistencia_temporal_metricas",
]
