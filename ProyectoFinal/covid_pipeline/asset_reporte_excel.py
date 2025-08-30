"""
Paso 6 — Exportación de Resultados
Asset para exportar resultados finales a Excel y CSV
"""

from dagster import asset, get_dagster_logger
import pandas as pd
import os
from datetime import datetime

@asset(
    description="Exportación de resultados finales a Excel y CSV (Paso 6)"
)
def reporte_excel_covid(
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame, 
    metrica_factor_crec_7d: pd.DataFrame
) -> str:
    """
    Exporta los resultados finales del pipeline a un archivo Excel con múltiples hojas
    y archivos CSV individuales.
    
    Args:
        datos_procesados: DataFrame con datos limpios procesados
        metrica_incidencia_7d: DataFrame con métricas de incidencia a 7 días
        metrica_factor_crec_7d: DataFrame con métricas de factor de crecimiento a 7 días
        
    Returns:
        str: Ruta del archivo Excel generado
    """
    logger = get_dagster_logger()
    
    logger.info("📊 INICIANDO EXPORTACIÓN DE RESULTADOS FINALES")
    logger.info("=" * 60)
    
    # Crear timestamp para los archivos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # === PREPARAR DATOS PARA EXPORTACIÓN ===
    logger.info("\n📋 Preparando datos para exportación...")
    
    # Datos procesados - solo Ecuador y España
    df_datos = datos_procesados.copy()
    logger.info(f"   - Datos procesados: {len(df_datos):,} registros")
    logger.info(f"   - Países: {sorted(df_datos['location'].unique())}")
    logger.info(f"   - Período: {df_datos['date'].min()} a {df_datos['date'].max()}")
    
    # Métricas de incidencia
    df_incidencia = metrica_incidencia_7d.copy()
    logger.info(f"   - Métrica incidencia: {len(df_incidencia):,} registros")
    
    # Métricas de factor de crecimiento
    df_factor = metrica_factor_crec_7d.copy()
    logger.info(f"   - Métrica factor crecimiento: {len(df_factor):,} registros")
    
    # === EXPORTAR A EXCEL ===
    logger.info("\n📁 Creando archivo Excel...")
    
    archivo_excel = f"reporte_covid_ecuador_spain_{timestamp}.xlsx"
    
    try:
        with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
            # Hoja 1: Datos procesados
            df_datos.to_excel(
                writer, 
                sheet_name='datos_procesados', 
                index=False,
                freeze_panes=(1, 0)  # Congelar primera fila
            )
            logger.info(f"   ✅ Hoja 'datos_procesados' creada ({len(df_datos):,} filas)")
            
            # Hoja 2: Métrica incidencia 7d
            df_incidencia.to_excel(
                writer, 
                sheet_name='metrica_incidencia_7d', 
                index=False,
                freeze_panes=(1, 0)
            )
            logger.info(f"   ✅ Hoja 'metrica_incidencia_7d' creada ({len(df_incidencia):,} filas)")
            
            # Hoja 3: Métrica factor crecimiento 7d  
            df_factor.to_excel(
                writer, 
                sheet_name='metrica_factor_crec_7d', 
                index=False,
                freeze_panes=(1, 0)
            )
            logger.info(f"   ✅ Hoja 'metrica_factor_crec_7d' creada ({len(df_factor):,} filas)")
            
            # Hoja 4: Resumen ejecutivo
            resumen_data = []
            
            # Estadísticas generales
            for pais in ['Ecuador', 'Spain']:
                datos_pais = df_datos[df_datos['location'] == pais]
                inc_pais = df_incidencia[df_incidencia['pais'] == pais]
                factor_pais = df_factor[df_factor['pais'] == pais]
                
                if not datos_pais.empty:
                    resumen_data.append({
                        'Indicador': f'{pais} - Total registros',
                        'Valor': len(datos_pais),
                        'Descripción': 'Número total de registros procesados'
                    })
                    
                    resumen_data.append({
                        'Indicador': f'{pais} - Fecha inicio',
                        'Valor': datos_pais['date'].min().strftime('%Y-%m-%d') if not datos_pais.empty else 'N/A',
                        'Descripción': 'Primera fecha con datos'
                    })
                    
                    resumen_data.append({
                        'Indicador': f'{pais} - Fecha fin',
                        'Valor': datos_pais['date'].max().strftime('%Y-%m-%d') if not datos_pais.empty else 'N/A',
                        'Descripción': 'Última fecha con datos'
                    })
                
                if not inc_pais.empty:
                    resumen_data.append({
                        'Indicador': f'{pais} - Incidencia máxima 7d',
                        'Valor': round(inc_pais['incidencia_7d'].max(), 2),
                        'Descripción': 'Máxima incidencia semanal registrada'
                    })
                    
                    resumen_data.append({
                        'Indicador': f'{pais} - Incidencia promedio 7d',
                        'Valor': round(inc_pais['incidencia_7d'].mean(), 2),
                        'Descripción': 'Incidencia semanal promedio'
                    })
                
                if not factor_pais.empty:
                    factor_clean = factor_pais[factor_pais['factor_crec_7d'] < 999.0]
                    if not factor_clean.empty:
                        resumen_data.append({
                            'Indicador': f'{pais} - Factor crecimiento máximo 7d',
                            'Valor': round(factor_clean['factor_crec_7d'].max(), 2),
                            'Descripción': 'Máximo factor de crecimiento semanal'
                        })
                        
                        resumen_data.append({
                            'Indicador': f'{pais} - Factor crecimiento promedio 7d',
                            'Valor': round(factor_clean['factor_crec_7d'].mean(), 2),
                            'Descripción': 'Factor de crecimiento semanal promedio'
                        })
            
            # Metadatos del reporte
            resumen_data.extend([
                {
                    'Indicador': 'Fecha generación reporte',
                    'Valor': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Descripción': 'Timestamp de generación del reporte'
                },
                {
                    'Indicador': 'Total países analizados',
                    'Valor': len(df_datos['location'].unique()),
                    'Descripción': 'Número de países incluidos en el análisis'
                },
                {
                    'Indicador': 'Total registros procesados',
                    'Valor': len(df_datos),
                    'Descripción': 'Número total de registros después del procesamiento'
                }
            ])
            
            df_resumen = pd.DataFrame(resumen_data)
            df_resumen.to_excel(
                writer, 
                sheet_name='resumen_ejecutivo', 
                index=False,
                freeze_panes=(1, 0)
            )
            logger.info(f"   ✅ Hoja 'resumen_ejecutivo' creada ({len(df_resumen):,} filas)")
        
        logger.info(f"   📊 Archivo Excel creado: {archivo_excel}")
        
    except Exception as e:
        logger.error(f"   ❌ Error creando Excel: {str(e)}")
        raise
    
    # === EXPORTAR ARCHIVOS CSV INDIVIDUALES ===
    logger.info("\n📁 Creando archivos CSV individuales...")
    
    archivos_csv = []
    
    try:
        # CSV 1: Datos procesados
        csv_datos = f"datos_procesados_{timestamp}.csv"
        df_datos.to_csv(csv_datos, index=False, encoding='utf-8')
        archivos_csv.append(csv_datos)
        logger.info(f"   ✅ CSV creado: {csv_datos}")
        
        # CSV 2: Métrica incidencia
        csv_incidencia = f"metrica_incidencia_7d_{timestamp}.csv"
        df_incidencia.to_csv(csv_incidencia, index=False, encoding='utf-8')
        archivos_csv.append(csv_incidencia)
        logger.info(f"   ✅ CSV creado: {csv_incidencia}")
        
        # CSV 3: Métrica factor crecimiento
        csv_factor = f"metrica_factor_crec_7d_{timestamp}.csv"
        df_factor.to_csv(csv_factor, index=False, encoding='utf-8')
        archivos_csv.append(csv_factor)
        logger.info(f"   ✅ CSV creado: {csv_factor}")
        
        # CSV 4: Resumen ejecutivo
        csv_resumen = f"resumen_ejecutivo_{timestamp}.csv"
        df_resumen.to_csv(csv_resumen, index=False, encoding='utf-8')
        archivos_csv.append(csv_resumen)
        logger.info(f"   ✅ CSV creado: {csv_resumen}")
        
    except Exception as e:
        logger.error(f"   ❌ Error creando CSVs: {str(e)}")
        raise
    
    # === VERIFICAR ARCHIVOS GENERADOS ===
    logger.info("\n🔍 Verificando archivos generados...")
    
    # Verificar Excel
    if os.path.exists(archivo_excel):
        size_excel = os.path.getsize(archivo_excel) / 1024  # KB
        logger.info(f"   ✅ {archivo_excel} - {size_excel:.1f} KB")
    else:
        logger.error(f"   ❌ {archivo_excel} no encontrado")
    
    # Verificar CSVs
    for csv_file in archivos_csv:
        if os.path.exists(csv_file):
            size_csv = os.path.getsize(csv_file) / 1024  # KB
            logger.info(f"   ✅ {csv_file} - {size_csv:.1f} KB")
        else:
            logger.error(f"   ❌ {csv_file} no encontrado")
    
    # === RESUMEN FINAL ===
    logger.info("\n🎉 EXPORTACIÓN COMPLETADA")
    logger.info("=" * 60)
    logger.info(f"📊 Archivo principal: {archivo_excel}")
    logger.info(f"📁 Archivos CSV: {len(archivos_csv)} archivos")
    logger.info(f"📈 Datos exportados:")
    logger.info(f"   - Registros procesados: {len(df_datos):,}")
    logger.info(f"   - Métricas incidencia: {len(df_incidencia):,}")
    logger.info(f"   - Métricas factor crecimiento: {len(df_factor):,}")
    logger.info(f"   - Países analizados: {', '.join(sorted(df_datos['location'].unique()))}")
    logger.info("=" * 60)
    
    return archivo_excel
