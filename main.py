import sys
from src.main_process import Main
from src.helpers import Helper

if __name__ == "__main__":
    ## Comporbar archivo config.json
    if not Helper.file_exists('config.json'):
        Helper.printr("*** Archivo de configuracion no encontrado ***")
        input("Presiona Enter para cerrar...")  # Espera a que el usuario presione Enter antes de cerrar
        sys.exit(1)  # Salir del programa
    
    # Cargar configuracion
    config = Helper.load_config('config.json')
    urls = config.get("urls", [])

    # Obtener modo de ejecución
    mode = Helper.get_option("Seleccione el modo de ejecución", [('Actualizar', 'U'), ('Buscar información', 'I')])

    Main = Main(urls, mode)
    Main.main()

    input("Presiona Enter para cerrar...")  # Espera a que el usuario presione Enter antes de cerrar
