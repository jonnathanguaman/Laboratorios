#!/usr/bin/env python3
"""
Script para probar el pipeline de Dagster con datos locales
"""

import pandas as pd
from covid_pipeline.assets import (
    check_fechas_no_futuras,
    check_columnas_clave_no_nulas,
    check_unicidad_pais_fecha,
    check_poblacion_positiva,
    check_casos_nuevos_validos,
    resumen_chequeos_calidad
)

def test_pipeline_local():
    """Prueba el pipeline usando los datos locales de covid.csv"""
    print("🧪 PROBANDO PIPELINE DAGSTER CON DATOS LOCALES")
    print("=" * 55)
    
    try:
        # Cargar datos locales
        print("\n📂 Cargando datos locales...")
        df = pd.read_csv('covid.csv')
        print(f"✓ Datos cargados: {len(df):,} filas, {len(df.columns)} columnas")
        
        # Ejecutar chequeos uno por uno
        chequeos = [
            ("Fechas no futuras", check_fechas_no_futuras),
            ("Columnas clave no nulas", check_columnas_clave_no_nulas),
            ("Unicidad país-fecha", check_unicidad_pais_fecha),
            ("Población positiva", check_poblacion_positiva),
            ("Casos nuevos válidos", check_casos_nuevos_validos)
        ]
        
        resultados = []
        
        print(f"\n🔍 EJECUTANDO CHEQUEOS DE CALIDAD")
        print("-" * 40)
        
        for nombre, funcion_chequeo in chequeos:
            try:
                print(f"\n▶️ Ejecutando: {nombre}")
                resultado = funcion_chequeo(df)
                
                estado = "✅ PASÓ" if resultado.passed else "❌ FALLÓ"
                severidad = resultado.severity.value
                
                print(f"   Estado: {estado}")
                print(f"   Severidad: {severidad}")
                print(f"   Descripción: {resultado.description}")
                
                if resultado.metadata:
                    print("   Metadata:")
                    # Manejar metadatos de Dagster correctamente
                    try:
                        for key, value in resultado.metadata.items():
                            # Extraer el valor real del MetadataValue
                            if hasattr(value, 'value'):
                                actual_value = value.value
                            elif hasattr(value, 'text'):
                                actual_value = value.text
                            else:
                                actual_value = str(value)
                            print(f"     - {key}: {actual_value}")
                    except Exception as e:
                        print(f"     - Error mostrando metadata: {e}")
                
                # Extraer filas_afectadas para el resumen
                filas_afectadas = "N/A"
                if resultado.metadata:
                    try:
                        if "filas_afectadas" in resultado.metadata:
                            filas_afectadas_meta = resultado.metadata["filas_afectadas"]
                            if hasattr(filas_afectadas_meta, 'value'):
                                filas_afectadas = filas_afectadas_meta.value
                            else:
                                filas_afectadas = str(filas_afectadas_meta)
                    except:
                        filas_afectadas = "Error"
                
                resultados.append({
                    "nombre_regla": nombre,
                    "estado": estado,
                    "severidad": severidad,
                    "descripcion": resultado.description,
                    "passed": resultado.passed,
                    "filas_afectadas": filas_afectadas,
                    "metadata": resultado.metadata
                })
                
            except Exception as e:
                print(f"   ❌ ERROR: {e}")
                resultados.append({
                    "nombre_regla": nombre,
                    "estado": "❌ ERROR",
                    "severidad": "ERROR",
                    "descripcion": f"Error ejecutando chequeo: {e}",
                    "passed": False,
                    "filas_afectadas": "Error",
                    "metadata": {}
                })
        
        # Mostrar resumen
        print(f"\n📊 RESUMEN DE CHEQUEOS")
        print("=" * 30)
        
        total_chequeos = len(resultados)
        chequeos_pasados = sum(1 for r in resultados if r["passed"])
        chequeos_fallados = total_chequeos - chequeos_pasados
        
        print(f"Total de chequeos: {total_chequeos}")
        print(f"✅ Pasados: {chequeos_pasados}")
        print(f"❌ Fallados: {chequeos_fallados}")
        
        # Crear tabla de resumen
        df_resumen = pd.DataFrame([
            {
                "nombre_regla": r["nombre_regla"],
                "estado": r["estado"],
                "filas_afectadas": r["filas_afectadas"],
                "notas": r["descripcion"][:100] + "..." if len(r["descripcion"]) > 100 else r["descripcion"]
            }
            for r in resultados
        ])
        
        # Guardar resumen
        df_resumen.to_csv('resumen_chequeos_local.csv', index=False)
        print(f"\n💾 Resumen guardado en: resumen_chequeos_local.csv")
        
        # Mostrar tabla de resumen
        print(f"\n📋 TABLA DE RESUMEN")
        print("-" * 50)
        print(df_resumen.to_string(index=False))
        
        return df_resumen, resultados
        
    except Exception as e:
        print(f"❌ Error ejecutando prueba: {e}")
        return None, None

if __name__ == "__main__":
    test_pipeline_local()
