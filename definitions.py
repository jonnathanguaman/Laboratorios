"""
Definiciones principales de Dagster para el proyecto COVID-19
"""

from dagster import Definitions
from covid_pipeline import defs

# Exportar las definiciones para Dagster
definitions = defs
