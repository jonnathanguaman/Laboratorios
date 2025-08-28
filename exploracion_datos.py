#!/usr/bin/env python3
"""
Exploración Manual de Datos (EDA) - COVID-19
Dataset: Our World in Data (OWID)
Enfoque: Ecuador y país comparativo (España)
"""

import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime

def verificar_datos():
    """Verifica que el archivo covid.csv existe"""
    if os.path.exists('covid.csv'):
        print("✓ Archivo covid.csv encontrado")
        return True
    else:
        print("❌ Archivo covid.csv no encontrado")
        return False

def cargar_y_filtrar_datos():
    """Carga los datos y filtra por Ecuador y España"""
    print("\nCargando y filtrando datos...")
    
    # Cargar datos desde covid.csv
    df = pd.read_csv('covid.csv')
    print(f"Dataset completo: {df.shape[0]:,} filas, {df.shape[1]} columnas")
    
    # Filtrar por Ecuador y España
    paises_interes = ['Ecuador', 'Spain']
    df_filtrado = df[df['country'].isin(paises_interes)].copy()
    
    print(f"Datos filtrados: {df_filtrado.shape[0]:,} filas para {paises_interes}")
    
    # Guardar datos filtrados
    df_filtrado.to_csv('covid_ecuador_spain.csv', index=False)
    
    return df_filtrado

def perfilado_basico(df):
    """Realiza el perfilado básico de los datos"""
    print("\n" + "="*60)
    print("PERFILADO BÁSICO DE DATOS COVID-19")
    print("="*60)
    
    perfilado = []
    
    # 1. Información general del dataset
    print("\n1. INFORMACIÓN GENERAL")
    print("-" * 30)
    info_general = {
        'Métrica': 'Información General',
        'Ecuador': f"{len(df[df['country'] == 'Ecuador'])} registros",
        'Spain': f"{len(df[df['country'] == 'Spain'])} registros",
        'Total': f"{len(df)} registros"
    }
    perfilado.append(info_general)
    print(f"Registros Ecuador: {len(df[df['country'] == 'Ecuador']):,}")
    print(f"Registros España: {len(df[df['country'] == 'Spain']):,}")
    print(f"Total columnas: {df.shape[1]}")
    
    # Columnas y tipos de datos principales
    print("\n2. COLUMNAS PRINCIPALES Y TIPOS DE DATOS")
    print("-" * 45)
    columnas_principales = [
        'date', 'country', 'new_cases', 'total_cases', 
        'new_deaths', 'total_deaths', 'people_vaccinated', 
        'people_fully_vaccinated', 'total_tests'
    ]
    
    tipos_info = {
        'Métrica': 'Tipos de Datos',
        'Ecuador': 'Disponible',
        'Spain': 'Disponible',
        'Total': f"{len([col for col in columnas_principales if col in df.columns])}/{len(columnas_principales)} columnas principales"
    }
    perfilado.append(tipos_info)
    
    for col in columnas_principales:
        if col in df.columns:
            print(f"{col:25}: {df[col].dtype}")
    
    # 3. Análisis de new_cases por país
    print("\n3. ANÁLISIS DE NEW_CASES")
    print("-" * 30)
    
    for pais in ['Ecuador', 'Spain']:
        df_pais = df[df['country'] == pais]
        new_cases = df_pais['new_cases'].dropna()
        
        if len(new_cases) > 0:
            min_cases = new_cases.min()
            max_cases = new_cases.max()
            
            perfilado.append({
                'Métrica': f'Min new_cases - {pais}',
                'Ecuador': min_cases if pais == 'Ecuador' else '-',
                'Spain': min_cases if pais == 'Spain' else '-',
                'Total': f"{min_cases:,.0f}"
            })
            
            perfilado.append({
                'Métrica': f'Max new_cases - {pais}',
                'Ecuador': max_cases if pais == 'Ecuador' else '-',
                'Spain': max_cases if pais == 'Spain' else '-',
                'Total': f"{max_cases:,.0f}"
            })
            
            print(f"{pais}:")
            print(f"  Mínimo new_cases: {min_cases:,.0f}")
            print(f"  Máximo new_cases: {max_cases:,.0f}")
    
    # 4. Porcentaje de valores faltantes
    print("\n4. VALORES FALTANTES")
    print("-" * 25)
    
    columnas_analisis = ['new_cases', 'people_vaccinated']
    
    for col in columnas_analisis:
        if col in df.columns:
            print(f"\n{col}:")
            for pais in ['Ecuador', 'Spain']:
                df_pais = df[df['country'] == pais]
                total_registros = len(df_pais)
                valores_faltantes = df_pais[col].isna().sum()
                porcentaje_faltante = (valores_faltantes / total_registros) * 100
                
                perfilado.append({
                    'Métrica': f'% Faltantes {col} - {pais}',
                    'Ecuador': f"{porcentaje_faltante:.1f}%" if pais == 'Ecuador' else '-',
                    'Spain': f"{porcentaje_faltante:.1f}%" if pais == 'Spain' else '-',
                    'Total': f"{valores_faltantes}/{total_registros}"
                })
                
                print(f"  {pais}: {porcentaje_faltante:.1f}% ({valores_faltantes}/{total_registros})")
    
    # 5. Rango de fechas
    print("\n5. RANGO DE FECHAS")
    print("-" * 20)
    
    df['date'] = pd.to_datetime(df['date'])
    
    for pais in ['Ecuador', 'Spain']:
        df_pais = df[df['country'] == pais]
        fecha_min = df_pais['date'].min()
        fecha_max = df_pais['date'].max()
        
        perfilado.append({
            'Métrica': f'Fecha inicio - {pais}',
            'Ecuador': fecha_min.strftime('%Y-%m-%d') if pais == 'Ecuador' else '-',
            'Spain': fecha_min.strftime('%Y-%m-%d') if pais == 'Spain' else '-',
            'Total': fecha_min.strftime('%Y-%m-%d')
        })
        
        perfilado.append({
            'Métrica': f'Fecha fin - {pais}',
            'Ecuador': fecha_max.strftime('%Y-%m-%d') if pais == 'Ecuador' else '-',
            'Spain': fecha_max.strftime('%Y-%m-%d') if pais == 'Spain' else '-',
            'Total': fecha_max.strftime('%Y-%m-%d')
        })
        
        print(f"{pais}:")
        print(f"  Desde: {fecha_min.strftime('%Y-%m-%d')}")
        print(f"  Hasta: {fecha_max.strftime('%Y-%m-%d')}")
        print(f"  Días: {(fecha_max - fecha_min).days}")
    
    return perfilado

def guardar_perfilado(perfilado_data):
    """Guarda la tabla de perfilado como CSV"""
    df_perfilado = pd.DataFrame(perfilado_data)
    df_perfilado.to_csv('tabla_perfilado.csv', index=False)
    print(f"\n✓ Tabla de perfilado guardada como 'tabla_perfilado.csv'")
    print(f"✓ {len(perfilado_data)} métricas incluidas")

def main():
    """Función principal"""
    print("EXPLORACIÓN DE DATOS COVID-19 - ECUADOR vs ESPAÑA")
    print("=" * 55)
    
    # Verificar que existe el archivo
    if not verificar_datos():
        return
    
    # Cargar y filtrar datos
    df_filtrado = cargar_y_filtrar_datos()
    
    # Realizar perfilado básico
    perfilado_data = perfilado_basico(df_filtrado)
    
    # Guardar tabla de perfilado
    guardar_perfilado(perfilado_data)
    
    print(f"\n{'='*60}")
    print("PROCESO COMPLETADO EXITOSAMENTE")
    print("Archivos generados:")
    print("- covid_ecuador_spain.csv (datos filtrados)")
    print("- tabla_perfilado.csv (métricas de perfilado)")
    print("="*60)

if __name__ == "__main__":
    main()
