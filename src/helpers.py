import os
import os.path as path
from openpyxl import Workbook

class Helper:

    @staticmethod
    def get_cpu_count():
        """
        Obtiene el numero total de hilos de la maquina.

        Parámetros:
        None

        Devuelve:
        int: Numero total de hilos.
        """
        return  os.cpu_count()

    @staticmethod
    def create_excel_if_not_exists(file_name, directory, sheet_name="Sheet1", get_path=True):
        """
        Verifica si un archivo Excel existe en la ruta especificada; si no existe, lo crea.

        Parámetros:
        file_name (str): Nombre del archivo a verificar o crear.
        directory (str): Ruta del directorio donde se verificará o creará el archivo.
        sheet_name (str): Nombre de la hoja de cálculo a crear o modificar. Por defecto es "Sheet1".
        get_path (bool): Si es True, devuelve la ruta del archivo además de la indicación de creación.


        Devuelve:
        tuple o bool: Si get_path es True, devuelve una tupla (ruta del archivo, bool),
                      si es False, devuelve un bool indicando si el archivo fue creado.
        """

        # Asegurar que el directorio existe
        os.makedirs(directory, exist_ok=True)

        # Ruta completa del archivo
        file_path = path.join(directory, file_name)

        # Verificar si el archivo existe
        if not path.exists(file_path):
            # Crear un nuevo libro de Excel
            wb = Workbook()
            
            # Modificar el nombre de la hoja predeterminada
            ws = wb.active
            ws.title = sheet_name
            
            # Guardar el libro de Excel en la ruta especificada
            wb.save(file_path)
            
            print(f"Archivo '{file_name}' creado en '{directory}'.")

            if get_path:
                return (file_path, True)
            return True
        else:
            if get_path:
                return (file_path, True)
            return True