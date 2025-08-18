def crear_descuento(porcentaje):
    def aplicar_descuento(precio):
        return precio * (1 - porcentaje)
    return aplicar_descuento

descuento10 = crear_descuento(0.10)
descuento25 = crear_descuento(0.25)

if __name__ == "__main__":
    print(descuento10(100))  
    print(descuento25(80))  
