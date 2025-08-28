#!/usr/bin/env python3
"""
Ejecutor del pipeline COVID-19 con Dagster
"""

import os
import subprocess
import sys
from pathlib import Path

def verificar_instalacion():
    """Verificar que Dagster esté instalado"""
    try:
        import dagster
        print(f"✓ Dagster {dagster.__version__} instalado")
        return True
    except ImportError:
        print("❌ Dagster no está instalado")
        return False

def ejecutar_pipeline():
    """Ejecutar el pipeline completo"""
    print("🚀 EJECUTANDO PIPELINE COVID-19 CON DAGSTER")
    print("=" * 50)
    
    if not verificar_instalacion():
        return False
    
    try:
        # Cambiar al directorio del proyecto
        os.chdir(Path(__file__).parent)
        
        print("\n📂 Directorio actual:", os.getcwd())
        print("📁 Archivos en directorio:")
        for f in os.listdir("."):
            if f.endswith(('.py', '.toml')):
                print(f"   - {f}")
        
        # Ejecutar materialization de assets
        print(f"\n🔧 Ejecutando materialización de assets...")
        
        from dagster import materialize
        from covid_pipeline.assets import leer_datos, resumen_chequeos_calidad
        from covid_pipeline.assets import CovidDataConfig
        
        # Configuración
        config = CovidDataConfig(
            url="https://covid.ourworldindata.org/data/owid-covid-data.csv",
            max_retries=2,
            timeout=60
        )
        
        # Materializar assets
        result = materialize(
            [leer_datos, resumen_chequeos_calidad],
            resources={"config": config}
        )
        
        if result.success:
            print("✅ Pipeline ejecutado exitosamente")
            return True
        else:
            print("❌ Pipeline falló")
            return False
            
    except Exception as e:
        print(f"❌ Error ejecutando pipeline: {e}")
        return False

def iniciar_dagster_ui():
    """Iniciar la interfaz web de Dagster"""
    print(f"\n🌐 INICIANDO INTERFAZ WEB DE DAGSTER")
    print("-" * 40)
    
    try:
        print("🔄 Ejecutando: dagster dev")
        print("📱 La interfaz estará disponible en: http://localhost:3000")
        print("⏹️  Presiona Ctrl+C para detener")
        
        # Ejecutar dagster dev
        result = subprocess.run([
            sys.executable, "-m", "dagster", "dev"
        ], check=True)
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error iniciando Dagster UI: {e}")
    except KeyboardInterrupt:
        print(f"\n🛑 Dagster UI detenido por el usuario")

def main():
    """Función principal"""
    print("COVID-19 PIPELINE MANAGER")
    print("=" * 30)
    
    while True:
        print(f"\n📋 OPCIONES:")
        print("1. 🧪 Probar pipeline localmente")
        print("2. 🚀 Ejecutar pipeline completo")
        print("3. 🌐 Iniciar interfaz web Dagster")
        print("4. 📊 Ver resultados existentes")
        print("5. 🚪 Salir")
        
        opcion = input(f"\n👉 Selecciona una opción (1-5): ").strip()
        
        if opcion == "1":
            print(f"\n🧪 Ejecutando pruebas locales...")
            os.system(f'"{sys.executable}" test_pipeline.py')
            
        elif opcion == "2":
            ejecutar_pipeline()
            
        elif opcion == "3":
            iniciar_dagster_ui()
            
        elif opcion == "4":
            print(f"\n📊 Archivos de resultados:")
            archivos_resultados = [
                "tabla_perfilado.csv",
                "resumen_chequeos_local.csv",
                "covid_ecuador_spain.csv"
            ]
            
            for archivo in archivos_resultados:
                if os.path.exists(archivo):
                    print(f"✓ {archivo}")
                else:
                    print(f"❌ {archivo} (no encontrado)")
                    
        elif opcion == "5":
            print(f"\n👋 ¡Hasta luego!")
            break
            
        else:
            print(f"\n❌ Opción inválida. Intenta de nuevo.")

if __name__ == "__main__":
    main()
