def suma_segura(a, b):
    """Suma dos números, validando que ambos sean numéricos."""
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Ambos argumentos deben ser numéricos.")
    return a + b

def division_segura(a, b):
    """Divide a entre b, validando que b no sea cero."""
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Ambos argumentos deben ser numéricos.")
    if b == 0:
        raise ZeroDivisionError("El divisor no puede ser cero.")
    return a / b

def es_positivo(n):
    """Valida si un número es positivo (>0)."""
    if not isinstance(n, (int, float)):
        raise TypeError("El argumento debe ser numérico.")
    return n > 0

def promedio(lista):
    """Calcula el promedio de una lista de números."""
    if not lista:
        raise ValueError("La lista no puede estar vacía.")
    if not all(isinstance(x, (int, float)) for x in lista):
        raise TypeError("Todos los elementos deben ser numéricos.")
    return sum(lista) / len(lista)
