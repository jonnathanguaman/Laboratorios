from utilidades_numericas import suma_segura, division_segura, es_positivo, promedio

if __name__ == "__main__":
    # Casos representativos
    print("Suma segura:", suma_segura(5, 3))  # 8
    print("División segura:", division_segura(10, 2))  # 5.0
    print("¿Es positivo?:", es_positivo(-1))  # False
    print("Promedio:", promedio([1, 2, 3, 4]))  # 2.5

    # Casos límite y errores controlados
    try:
        print("División por cero:", division_segura(5, 0))
    except ZeroDivisionError as e:
        print(f"Error controlado: {e}")
    try:
        print("Promedio de lista vacía:", promedio([]))
    except ValueError as e:
        print(f"Error controlado: {e}")
    try:
        print("Suma con string:", suma_segura(5, "a"))
    except TypeError as e:
        print(f"Error controlado: {e}")
