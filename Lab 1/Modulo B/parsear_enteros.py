def parsear_enteros(entradas):
    valores = []
    errores = []
    for entrada in entradas:
        try:
            valores.append(int(entrada))
        except ValueError:
            errores.append(f"Error: '{entrada}' no es un entero válido.")
    return valores, errores

# Pruebas
if __name__ == "__main__":
    datos = ["10", "x", "3"]
    valores, errores = parsear_enteros(datos)
    print("Valores convertidos:", valores)  # [10, 3]
    print("Errores:", errores)  # Error para "x"
