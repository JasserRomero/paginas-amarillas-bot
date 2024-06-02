import os
import json
import sys
import os.path as path
from pathlib import Path
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
    def printr(message: str, spacing: str = 'none'):
        """
        Imprime un mensaje con un espaciado opcional.
        
        :param message: str, el mensaje a imprimir
        :param spacing: str, puede ser 'above', 'below', 'both' o 'none' para controlar el espaciado
                        'above' - Añade una línea en blanco antes del mensaje
                        'below' - Añade una línea en blanco después del mensaje
                        'both'  - Añade una línea en blanco antes y después del mensaje
                        'none'  - No añade líneas en blanco
        """
        if spacing == 'above':
            print("\n" + message)  # Añade una línea en blanco antes del mensaje
        elif spacing == 'below':
            print(message + "\n")  # Añade una línea en blanco después del mensaje
        elif spacing == 'both':
            print("\n" + message + "\n")  # Añade una línea en blanco antes y después del mensaje
        else:
            print(message)  # Imprime el mensaje sin espaciado adicional

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
        
            if get_path:
                return (file_path, True)
            return True
        else:
            if get_path:
                return (file_path, True)
            return True
    
    @staticmethod
    def file_exists(file_path):
        """
        Comprueba si el archivo existe en la ruta dada.

        :param file_path: str o Path, ruta del archivo a comprobar
        :return: bool, True si el archivo existe, False en caso contrario
        """
        return Path(file_path).is_file()

    @staticmethod
    def load_config(config_path):
        """
        Carga la configuración desde un archivo JSON ubicado al mismo nivel que el ejecutable.

        :param config_path: str, ruta relativa del archivo de configuración
        :return: dict, contenido del archivo de configuración
        """
        # Determina la ruta al archivo de configuración
        if hasattr(sys, '_MEIPASS'):
            # Directorio temporal creado por PyInstaller
            base_path = Path(sys._MEIPASS)
        else:
            # Directorio del script principal (subiendo desde src a la raíz)
            base_path = Path(__file__).resolve().parent.parent
        
        json_file_path = base_path / config_path
        
        if not json_file_path.is_file():
            raise FileNotFoundError(f"El archivo {json_file_path} no existe.")
        
        with open(json_file_path, 'r') as file:
            config = json.load(file)
        
        return config