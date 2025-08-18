class CantidadInvalida(Exception):
    pass

def calcular_total(precio_unitario, cantidad):
    if cantidad <= 0:
        raise CantidadInvalida("La cantidad debe ser mayor que cero.")
    if precio_unitario < 0:
        raise ValueError("El precio unitario no puede ser negativo.")
    return precio_unitario * cantidad

if __name__ == "__main__":
    pruebas = [
        (10, 3),      # válido
        (10, 0),      # cantidad inválida
        (-5, 2),      # precio inválido
    ]
    for precio, cantidad in pruebas:
        try:
            total = calcular_total(precio, cantidad)
            print(f"Total para {cantidad} unidades a ${precio} c/u: {total}")
        except CantidadInvalida as e:
            print(f"CantidadInvalida: {e}")
        except ValueError as e:
            print(f"ValueError: {e}")
