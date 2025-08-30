from utilidades.cadenas import normalizar, es_email
from utilidades.numeros import suma_segura, es_positivo

if __name__ == "__main__":
    print(normalizar("  Hola Mundo  "))  # "hola mundo"
    print(es_email("correo@dominio.com"))  # True
    print(suma_segura(5, 7))  # 12
    print(es_positivo(-3))  # False
    # Caso límite
    try:
        print(suma_segura(5, "a"))
    except TypeError as e:
        print(f"Error controlado: {e}")
