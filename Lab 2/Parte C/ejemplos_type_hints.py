from typing import List, Union, Optional

def suma(a: int, b: int) -> int:
    return a + b

def nombre_completo(nombre: str, apellido: Optional[str] = None) -> str:
    if apellido:
        return f"{nombre} {apellido}"
    return nombre

def procesar_lista(valores: List[Union[int, str]]) -> List[str]:
    return [str(v) for v in valores]

# Ejemplos de uso
if __name__ == "__main__":
    print(suma(2, 3))  # 5
    print(nombre_completo("Ana"))  # "Ana"
    print(nombre_completo("Ana", "García"))  # "Ana García"
    print(procesar_lista([1, "dos", 3]))  # ['1', 'dos', '3']
