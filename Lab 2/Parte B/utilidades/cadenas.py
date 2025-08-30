import re

def normalizar(texto):
    """Convierte a minúsculas y elimina espacios extra."""
    return ' '.join(texto.lower().split())

def es_email(texto):
    """Valida si el texto tiene formato de email simple."""
    return bool(re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", texto))
