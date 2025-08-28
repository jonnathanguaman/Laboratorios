#!/usr/bin/env python3
"""
Script para explorar la estructura del archivo covid.csv
"""

import pandas as pd
import numpy as np

def explorar_estructura():
    """Explora la estructura básica del archivo covid.csv"""
    print("EXPLORACIÓN DE ESTRUCTURA - covid.csv")
    print("=" * 40)
    
    try:
        # Leer solo las primeras 1000 filas para análisis rápido
        df = pd.read_csv('covid.csv', nrows=1000)
        
        print(f"Dimensiones (primeras 1000 filas): {df.shape}")
        print(f"\nColumnas ({len(df.columns)} total):")
        print("-" * 30)
        
        for i, col in enumerate(df.columns):
            print(f"{i+1:2d}. {col}")
        
        print(f"\nTipos de datos:")
        print("-" * 20)
        for col in df.columns[:10]:  # Mostrar primeras 10 columnas
            print(f"{col:20}: {df[col].dtype}")
        
        print(f"\nPrimeras 3 filas:")
        print("-" * 20)
        print(df.head(3).to_string())
        
        # Buscar columnas que puedan contener países
        print(f"\nBuscando columnas de países...")
        print("-" * 30)
        
        columnas_pais = []
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['country', 'location', 'nation', 'pais']):
                columnas_pais.append(col)
                print(f"Posible columna de país: {col}")
                # Mostrar algunos valores únicos
                valores_unicos = df[col].unique()[:10]
                print(f"  Primeros valores: {valores_unicos}")
        
        if not columnas_pais:
            print("No se encontraron columnas obvias de países.")
            print("Explorando valores únicos en las primeras columnas:")
            for col in df.columns[:5]:
                if df[col].dtype == 'object':
                    valores_unicos = df[col].unique()
                    if len(valores_unicos) < 50:  # Si tiene pocos valores únicos, podría ser países
                        print(f"\n{col} ({len(valores_unicos)} valores únicos):")
                        print(f"  {valores_unicos[:10]}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    explorar_estructura()
