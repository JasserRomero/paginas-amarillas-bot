from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import requests
import numpy as np
import time
import gspread
import os

actividades=[
    'https://www.paginasamarillas.es/a/mercado-inmobiliario/',
    'https://www.paginasamarillas.es/a/servicio-inmobiliario/',
    'https://www.paginasamarillas.es/a/abogados/',
    'https://www.paginasamarillas.es/a/abogada/',
    'https://www.paginasamarillas.es/a/gestores/',
    'https://www.paginasamarillas.es/a/gestoria-administrativa/',
    ## Agregar a la lista paginas que se desean scrapear 
]

# Obtiene el número de núcleos de CPU disponibles

# Función para realizar solicitudes y obtener el número de resultados y páginas
def fetch_initial_data(url):
    headers = {'User-agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        results = int(soup.find('span', {'class': 'h1'}).text[1:-1].replace(".", ""))
        pages = np.ceil(results / 30)
    except Exception as e:
        print(f"Error processing {url}: {e}")
        results = 0
        pages = 0
    return [url, results, pages]

# Uso de ThreadPoolExecutor para procesar todas las URLs de actividades
def scrape_initial_data(urls):
    base_act = []
    with ThreadPoolExecutor(max_workers=num_cores) as executor:
        future_to_url = {executor.submit(fetch_initial_data, url): url for url in urls}
        for future in as_completed(future_to_url):
            base_act.append(future.result())
    return base_act

# Obtener los datos iniciales de forma paralela
base_act = scrape_initial_data(actividades)
nombre_archivo = './paginasamarillas_filtrado.xlsx'
df_act = pd.DataFrame(base_act, columns=["actividades", "resultados", "paginas"])

df_existente = pd.read_excel(nombre_archivo, sheet_name='Actividades')
df_actualizado = pd.concat([df_existente, df_act], ignore_index=True)

nombre_archivo = './paginasamarillas_filtrado.xlsx'     
with pd.ExcelWriter(
    nombre_archivo,
    mode="a",
    engine="openpyxl",
    if_sheet_exists="replace",
) as writer:
    df_actualizado.to_excel(writer, sheet_name='Actividades', index=False)


## Procesar las paginas y extraer datos
