def requiere_positivos(*args):
    for arg in args:
        if not isinstance(arg, (int, float)):
            continue
        if arg <= 0:
            raise ValueError(f"El argumento '{arg}' debe ser mayor que cero.")

def calcular_descuento(precio, porcentaje):
    requiere_positivos(precio, porcentaje)
    return precio * (1 - porcentaje)

def escala(valor, factor):
    requiere_positivos(valor, factor)
    return valor * factor

if __name__ == "__main__":
    # Pruebas
    try:
        print(calcular_descuento(100, 0.2))  # 80.0
    except ValueError as e:
        print(f"Error: {e}")
    try:
        print(calcular_descuento(-1, 0.2))  # Error
    except ValueError as e:
        print(f"Error: {e}")
    try:
        print(escala(5, 3))  # 15
    except ValueError as e:
        print(f"Error: {e}")
    try:
        print(escala(0, 3))  # Error
    except ValueError as e:
        print(f"Error: {e}")
