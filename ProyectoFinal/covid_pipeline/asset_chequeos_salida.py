"""
Asset adicional para el resumen de chequeos de salida del Paso 5
"""

from dagster import asset, get_dagster_logger
import pandas as pd
from datetime import datetime

@asset(
    description="Resumen de todos los chequeos de salida (Paso 5)"
)
def chequeos_salida(
    metrica_incidencia_7d: pd.DataFrame, 
    metrica_factor_crec_7d: pd.DataFrame
) -> pd.DataFrame:
    """
    Genera un resumen consolidado de todos los asset checks del Paso 5.
    
    Args:
        metrica_incidencia_7d: DataFrame con métricas de incidencia
        metrica_factor_crec_7d: DataFrame con métricas de factor de crecimiento
        
    Returns:
        pd.DataFrame: Resumen de todos los chequeos realizados
    """
    logger = get_dagster_logger()
    
    logger.info("📋 GENERANDO RESUMEN DE CHEQUEOS DE SALIDA")
    logger.info("=" * 50)
    
    # Lista para almacenar resultados de todos los checks
    resultados_checks = []
    
    # === CHECK 1: Rango de incidencia 7d ===
    logger.info("\n🔍 Ejecutando Check 1: Rango incidencia 7d")
    
    df_incidencia = metrica_incidencia_7d.copy()
    total_inc = len(df_incidencia)
    fuera_rango_inc = ((df_incidencia['incidencia_7d'] < 0) | (df_incidencia['incidencia_7d'] > 2000)).sum()
    
    check1_resultado = {
        'check_nombre': 'rango_incidencia_7d',
        'asset_objetivo': 'metrica_incidencia_7d',
        'descripcion': 'Validar que incidencia esté en rango [0, 2000]',
        'total_registros': total_inc,
        'registros_invalidos': int(fuera_rango_inc),
        'porcentaje_validos': round((1 - fuera_rango_inc/total_inc) * 100, 2),
        'resultado': 'PASÓ' if fuera_rango_inc == 0 else 'FALLÓ',
        'timestamp': datetime.now().isoformat()
    }
    resultados_checks.append(check1_resultado)
    
    logger.info(f"   - Total registros: {total_inc:,}")
    logger.info(f"   - Fuera de rango: {fuera_rango_inc:,}")
    logger.info(f"   - Resultado: {check1_resultado['resultado']}")
    
    # === CHECK 2: Completitud de incidencia ===
    logger.info("\n🔍 Ejecutando Check 2: Completitud incidencia")
    
    nulos_inc = df_incidencia['incidencia_7d'].isna().sum()
    
    check2_resultado = {
        'check_nombre': 'completitud_incidencia_7d',
        'asset_objetivo': 'metrica_incidencia_7d',
        'descripcion': 'Validar ausencia de valores nulos',
        'total_registros': total_inc,
        'registros_invalidos': int(nulos_inc),
        'porcentaje_validos': round((1 - nulos_inc/total_inc) * 100, 2),
        'resultado': 'PASÓ' if nulos_inc == 0 else 'FALLÓ',
        'timestamp': datetime.now().isoformat()
    }
    resultados_checks.append(check2_resultado)
    
    logger.info(f"   - Valores nulos: {nulos_inc:,}")
    logger.info(f"   - Resultado: {check2_resultado['resultado']}")
    
    # === CHECK 3: Rango factor crecimiento ===
    logger.info("\n🔍 Ejecutando Check 3: Rango factor crecimiento")
    
    df_factor = metrica_factor_crec_7d.copy()
    # Excluir valores infinitos para el análisis
    df_factor_clean = df_factor[df_factor['factor_crec_7d'] < 999.0].copy()
    
    total_factor = len(df_factor_clean)
    fuera_rango_factor = ((df_factor_clean['factor_crec_7d'] < 0) | (df_factor_clean['factor_crec_7d'] > 50)).sum()
    
    check3_resultado = {
        'check_nombre': 'rango_factor_crecimiento_7d',
        'asset_objetivo': 'metrica_factor_crec_7d',
        'descripcion': 'Validar factor crecimiento en rango [0, 50]',
        'total_registros': total_factor,
        'registros_invalidos': int(fuera_rango_factor),
        'porcentaje_validos': round((1 - fuera_rango_factor/total_factor) * 100, 2) if total_factor > 0 else 0,
        'resultado': 'PASÓ' if fuera_rango_factor == 0 else 'FALLÓ',
        'timestamp': datetime.now().isoformat()
    }
    resultados_checks.append(check3_resultado)
    
    logger.info(f"   - Total registros analizados: {total_factor:,}")
    logger.info(f"   - Fuera de rango: {fuera_rango_factor:,}")
    logger.info(f"   - Resultado: {check3_resultado['resultado']}")
    
    # === CHECK 4: Distribución de tendencias ===
    logger.info("\n🔍 Ejecutando Check 4: Distribución tendencias")
    
    crecimiento = (df_factor_clean['factor_crec_7d'] > 1.0).sum()
    decrecimiento = (df_factor_clean['factor_crec_7d'] < 1.0).sum()
    estable = (df_factor_clean['factor_crec_7d'] == 1.0).sum()
    
    if total_factor > 0:
        pct_crecimiento = (crecimiento / total_factor) * 100
        pct_decrecimiento = (decrecimiento / total_factor) * 100
        pct_estable = (estable / total_factor) * 100
        
        # Verificar que ninguna tendencia domine más del 80%
        tendencia_dominante = max(pct_crecimiento, pct_decrecimiento, pct_estable)
        distribucion_ok = tendencia_dominante <= 80.0
    else:
        distribucion_ok = False
        tendencia_dominante = 0
    
    check4_resultado = {
        'check_nombre': 'distribucion_tendencias_7d',
        'asset_objetivo': 'metrica_factor_crec_7d',
        'descripcion': 'Validar distribución balanceada de tendencias',
        'total_registros': total_factor,
        'registros_invalidos': 0 if distribucion_ok else 1,
        'porcentaje_validos': round(tendencia_dominante, 2),
        'resultado': 'PASÓ' if distribucion_ok else 'FALLÓ',
        'timestamp': datetime.now().isoformat()
    }
    resultados_checks.append(check4_resultado)
    
    logger.info(f"   - Crecimiento: {crecimiento:,} ({pct_crecimiento:.1f}%)")
    logger.info(f"   - Decrecimiento: {decrecimiento:,} ({pct_decrecimiento:.1f}%)")
    logger.info(f"   - Tendencia dominante: {tendencia_dominante:.1f}%")
    logger.info(f"   - Resultado: {check4_resultado['resultado']}")
    
    # === CHECK 5: Consistencia temporal ===
    logger.info("\n🔍 Ejecutando Check 5: Consistencia temporal")
    
    # Convertir fechas para análisis
    df_inc_temporal = df_incidencia.copy()
    df_factor_temporal = df_factor.copy()
    
    if not pd.api.types.is_datetime64_any_dtype(df_inc_temporal['fecha']):
        df_inc_temporal['fecha'] = pd.to_datetime(df_inc_temporal['fecha'])
    if not pd.api.types.is_datetime64_any_dtype(df_factor_temporal['semana_fin']):
        df_factor_temporal['semana_fin'] = pd.to_datetime(df_factor_temporal['semana_fin'])
    
    # Calcular solapamiento temporal por país
    solapamientos = []
    
    for pais in ['Ecuador', 'Spain']:
        fechas_inc = set(df_inc_temporal[df_inc_temporal['pais'] == pais]['fecha'].dt.date)
        fechas_factor = set(df_factor_temporal[df_factor_temporal['pais'] == pais]['semana_fin'].dt.date)
        
        if fechas_inc and fechas_factor:
            fechas_comunes = fechas_inc.intersection(fechas_factor)
            fechas_union = fechas_inc.union(fechas_factor)
            solapamiento = len(fechas_comunes) / len(fechas_union) * 100
            solapamientos.append(solapamiento)
    
    solapamiento_promedio = sum(solapamientos) / len(solapamientos) if solapamientos else 0
    consistencia_ok = solapamiento_promedio >= 80.0
    
    check5_resultado = {
        'check_nombre': 'consistencia_temporal_metricas',
        'asset_objetivo': 'metrica_incidencia_7d',
        'descripcion': 'Validar consistencia temporal entre métricas',
        'total_registros': len(solapamientos),
        'registros_invalidos': 0 if consistencia_ok else 1,
        'porcentaje_validos': round(solapamiento_promedio, 2),
        'resultado': 'PASÓ' if consistencia_ok else 'FALLÓ',
        'timestamp': datetime.now().isoformat()
    }
    resultados_checks.append(check5_resultado)
    
    logger.info(f"   - Solapamiento promedio: {solapamiento_promedio:.1f}%")
    logger.info(f"   - Resultado: {check5_resultado['resultado']}")
    
    # === CREAR DATAFRAME RESUMEN ===
    logger.info("\n📊 CREANDO RESUMEN FINAL")
    
    df_resumen = pd.DataFrame(resultados_checks)
    
    # Estadísticas generales
    total_checks = len(resultados_checks)
    checks_pasados = sum(1 for check in resultados_checks if check['resultado'] == 'PASÓ')
    tasa_exito = (checks_pasados / total_checks) * 100
    
    logger.info(f"   - Total checks ejecutados: {total_checks}")
    logger.info(f"   - Checks que pasaron: {checks_pasados}")
    logger.info(f"   - Tasa de éxito: {tasa_exito:.1f}%")
    
    # Agregar fila resumen
    resumen_general = {
        'check_nombre': 'RESUMEN_GENERAL',
        'asset_objetivo': 'TODOS',
        'descripcion': f'Resumen de {total_checks} asset checks ejecutados',
        'total_registros': total_checks,
        'registros_invalidos': total_checks - checks_pasados,
        'porcentaje_validos': round(tasa_exito, 2),
        'resultado': 'PASÓ' if tasa_exito >= 80.0 else 'FALLÓ',
        'timestamp': datetime.now().isoformat()
    }
    
    df_resumen = pd.concat([df_resumen, pd.DataFrame([resumen_general])], ignore_index=True)
    
    # Guardar archivo de resumen
    archivo_salida = "chequeos_salida_resumen.csv"
    df_resumen.to_csv(archivo_salida, index=False)
    logger.info(f"   - Archivo guardado: {archivo_salida}")
    
    # Log final
    logger.info(f"\n🎉 CHEQUEOS DE SALIDA COMPLETADOS")
    logger.info(f"   - Tasa de éxito general: {tasa_exito:.1f}%")
    logger.info(f"   - Estado general: {resumen_general['resultado']}")
    logger.info("=" * 50)
    
    return df_resumen
