from .cadenas import normalizar  # Importación relativa

def suma_segura(a, b):
    """Suma dos números, validando tipos."""
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Ambos argumentos deben ser numéricos.")
    return a + b

def es_positivo(n):
    """Valida si un número es positivo (>0)."""
    if not isinstance(n, (int, float)):
        raise TypeError("El argumento debe ser numérico.")
    return n > 0
