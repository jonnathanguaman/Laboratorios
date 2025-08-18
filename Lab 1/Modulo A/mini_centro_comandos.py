def saludar(nombre):
    return f"Hola, {nombre}"

def despedir(nombre):
    return f"Adiós, {nombre}"

def aplaudir(nombre):
    return f"¡Bravo, {nombre}!"

acciones = {
    "saludar": saludar,
    "despedir": despedir,
    "aplaudir": aplaudir
}

def ejecutar(accion, *args, **kwargs):
    if accion not in acciones:
        raise ValueError(f"Acción '{accion}' no reconocida.")
    return acciones[accion](*args, **kwargs)

if __name__ == "__main__":
    print(ejecutar("saludar", "Ana"))  
    print(ejecutar("despedir", "Luis"))
    print(ejecutar("aplaudir", "María"))  
    try:
        print(ejecutar("bailar", "Pedro"))
    except ValueError as e:
        print(f"Error: {e}")
