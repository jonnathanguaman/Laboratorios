"""
Definiciones de Dagster para el pipeline de COVID-19
"""

from dagster import Definitions, load_assets_from_modules

from . import assets

# Cargar todos los assets desde el módulo assets
covid_assets = load_assets_from_modules([assets])

# Definir el proyecto Dagster
defs = Definitions(
    assets=covid_assets,
)
