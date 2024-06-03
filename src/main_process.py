from src.helpers import Helper
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import requests
import numpy as np
import time
import gspread
import re
import threading
import sys
import json
import os

class Main:
    def __init__(self, urls = [], mode = None):
        self.mode = mode
        self.urls = urls
        self.config_path = "config.json"
        self.name_file = 'paginasamarillas_filtrado.xlsx'
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'es-ES,es;q=0.6',
            'cache-control': 'max-age=0',
            # 'cookie': 'oldSearch=ca07fe3feb5056fd63f7bb2c4c298226; oldIDSearch=paol206143738Xef88b5803a94d035; paol_searchs=a%3AGestoria+administrativa%7C; seedCookie=8754; uidAnalytics=923749512; ID_BUSQUEDA=paol2061232856X7a4850303a94d02d; visid_incap_861374=ip31tcAEROWMlzNEneBsIsJjRWYAAAAAQUIPAAAAAAC52ErNvqdEBkwF38stzP7w; visid_incap_2735099=7/TTGzEoQnqShIwSxUFogsNjRWYAAAAAQUIPAAAAAADSdLzvzYOD350Vt223Qo0k; check=true; _vfa=www%2Epaginasamarillas%2Ees.00000000-0000-4000-8000-091c972c717a.ee3fd02f-2eb0-4acd-ac18-0aae318a6423.1717245768.1717245768.1717250223.2; incap_ses_9129_861374=vTjwMHusjCdOzbg8MrmwfstwW2YAAAAA7XwamyzgM+tDNL8Xh/RtKA==; incap_ses_9219_2735099=u+FHPBeZ90QGJ0UwuHfwfzR5W2YAAAAAr0ubdxZYnovlEU+FsET/sg==; incap_ses_1661_861374=tMe6J3uG3UY0/jdnyg4NF497W2YAAAAAKlwzt1BOg0MkgNtuc7FxnA==; incap_ses_1352_2735099=L9KgRBVhHm9VJiSX5kTDEpB7W2YAAAAAqhaSNl47wvTSLvJjWwJyfg==; uidAnalytics=155663935; incap_ses_2224_2735099=L+VQLu86DmjNKj6nRTzdHqqCW2YAAAAAiPE2YIGODuK5j6TOqnP8bQ==; incap_ses_7228_861374=kaiYLTY8MA0nW05iwgNPZPKIW2YAAAAAKq0+Grya/PL0QF4CNwHjVg==; incap_ses_536_861374=HzRkL2aEkRkPDcJyOUFwBx6JW2YAAAAAjB1mtSp0wSyRaipIXKgx7A==; incap_ses_9029_861374=5pcsHVUgQy6Oct6IvnNNfbqQW2YAAAAArqpau3Nbl2X+/Abooesxog==; mbox=session#6e56a385875048899a837221af25d8ac#1717278721; incap_ses_1397_2735099=uUp5NUfR6nMpy+E0JyRjE7yQW2YAAAAAqaPnjKeMFsXE5es9I2Rcsg==; incap_ses_882_861374=Zug7JUXs4m4vnOUOZH49DJaSW2YAAAAAPuwrjkz9JHd7eDR0BPaY3w==; ___utmvc=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; incap_ses_1516_2735099=K3ovMl0Ttysdxl3MBeoJFZmSW2YAAAAAw84OYX4K42E/dZlCrjw1dw==',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Brave";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        }

    def fetch_initial_data(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status() # Lanzamos excepcion si la respuesta no es correcta
            soup = BeautifulSoup(response.text, 'html.parser')
            results = int(soup.find('span', {'class': 'h1'}).text[1:-1].replace(".", ""))
            pages = np.ceil(results / 30)
        except Exception as e:
            print(f"Error processing {url}: {e}")
            results = 0
            pages = 0
        return [url, results, pages]

    def fetch_page_data(self, url, retries=2, backoff_factor=2):
        for attempt in range(retries + 1):
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                return soup
            except (requests.Timeout, requests.RequestException):
                if attempt < retries:
                    time.sleep(backoff_factor * (attempt + 1))
        
        return None

    def process_soup(self, soup, url_base, data):
        boxes = soup.find_all("div", {"class": "box"})
        web, desc_web, logo_web = "", "", ""
        
        for box in boxes[2:]:

            try:
                web = box.find("a", {"class": "web"}).get('href')
                l = requests.get(web, headers=self.headers)
                loup = BeautifulSoup(l.text, 'html.parser')
                desc_web = loup.find("meta", {"property": "og:description"})
                logo_web = loup.find("img").get("src")
            except:
                web, desc_web, logo_web = "", "", ""
            
            name = box.find("span", {"itemprop": "name"}).text if box.find("span", {"itemprop": "name"}) else ""
            categ = box.find("p", {"class": "categ"}).text if box.find("p", {"class": "categ"}) else ""
            desc = box.find("div", {"itemprop": "description"}).text if box.find("div", {"itemprop": "description"}) else ""
            address = box.find("span", {"itemprop": "streetAddress"}).text if box.find("span", {"itemprop": "streetAddress"}) else ""
            postal = box.find("span", {"itemprop": "postalCode"}).text if box.find("span", {"itemprop": "postalCode"}) else ""
            locality = box.find("span", {"itemprop": "addressLocality"}).text if box.find("span", {"itemprop": "addressLocality"}) else ""
            region  = box.find("span", {"itemprop": "addressRegion"}).text if box.find("span", {"itemprop": "addressRegion"}) else locality
            tel = box.find("span", {"itemprop": "telephone"}).text if box.find("span", {"itemprop": "telephone"}) else ""
            logo = box.find("img", {"itemprop": "image"}).get("src") if box.find("img", {"itemprop": "image"}) else ""
            link = box.find("a").get('href') if box.find("a") else "" 

            horario=""
            customer_mail=""
            try:
                l=requests.get(link, headers=self.headers)
                loup = BeautifulSoup(l.text, 'html.parser')

                # Añadir mas descripcion
                try:desc += " | " + loup.find("p",{"class":"line-fluid"}) 
                except:pass

                # Obtener Horarios divididos por |
                try: 
                    horario_div = loup.find('div', {'id': 'horario'})
                    horarios = [] # Inicializar una lista para guardar cada entrada de horario
                    if horario_div:
                        for p in horario_div.find_all('p'): # Iterar sobre cada <p> en el div
                            dia = p.find('b').text if p.find('b') else ''  # Obtener el nombre del día (contenido en <b>)
                            times = p.find_all('time')  # Obtener todos los elementos <time> y sus horarios
                            if times: # Extraer los horarios y unirlos con ' y '
                                horario_texto = ' y '.join(time.get_text(strip=True) for time in times)
                                horarios.append(f"{dia} {horario_texto}")
                            else:
                                horarios.append(f"{dia} Cerrado") # Manejar los días cerrados
                        
                        horario = " | ".join(horarios)
                except:pass

                # Obtener email
                try:
                    contenedor_div = loup.find("div", class_="contenedor")
                    if contenedor_div and 'data-business' in contenedor_div.attrs:
                        data_business = contenedor_div.attrs['data-business']
                        data_business_json = json.loads(data_business)

                        customer_mail = data_business_json.get('customerMail', "")
                except:pass
            except:pass

            data.append([link,name,customer_mail,categ,desc,web,tel,address,postal,locality,region,logo,logo_web,horario,url_base])

    def scrape_page_range(self, url_base, page_range, data):
        for page in page_range:
            url = f"{url_base}{page}"
            soup = self.fetch_page_data(url)
            if soup:
                self.process_soup(soup, url_base, data)

    def scrape_data(self, url_base, num_pages, num_threads=None):
        data = []
        threads = []

        if num_threads is None:
            num_threads = Helper.get_cpu_count()

        # # Crear particiones de las páginas para cada hilo
        # pages_per_thread = num_pages // num_threads
        # page_ranges = [range(i * pages_per_thread + 1, (i + 1) * pages_per_thread + 1) for i in range(num_threads)]
        # if num_pages % num_threads != 0:
        #     page_ranges.append(range(num_threads * pages_per_thread + 1, num_pages + 1))

        # for page_range in page_ranges:
        #     thread = threading.Thread(target=self.scrape_page_range, args=(url_base, page_range, data))
        #     thread.start()
        #     threads.append(thread)
        
        # for thread in tqdm(threads, desc=f"Scraping {url_base}"):
        #     thread.join()

        for page in tqdm(range(int(num_pages))):
            if page == 0: continue
            if page == num_pages: break
            url = f"{url_base}{page}"
            soup = self.fetch_page_data(url)
            if soup:
                self.process_soup(soup, url, data)

        return data

    def process_update_files(self, update_folder):
        """
        Procesar archivos de actualización desde la carpeta 'update'.
        """
        if not os.path.exists(update_folder):
            print(f"Error: la carpeta '{update_folder}' no existe.")
            option = Helper.get_option("¿Desea crear la carpeta?", [('Si', 'T'), ('No', 'F')])
            if option == "F":
                return
            if option == "T":
                os.makedirs(update_folder)
        
        if len(os.listdir(update_folder)) == 0:
            print(f"Error: la carpeta '{update_folder}' esta vacía. No hay xlsx a procesar")
            return
        
        for file_name in os.listdir(update_folder):
            if file_name.endswith('.xlsx'):
                file_path = os.path.join(update_folder, file_name)
                print(f"Procesando archivo: {file_name}")
                try:
                    df = pd.read_excel(file_path)

                    # Verificar que la columna 'email' y 'horario' exista exista
                    if 'email' and 'horario' not in df.columns:
                        print("La columna 'email' o 'horario' no existe en el archivo Excel.")
                        return
                    else:
                        df['email'] = df['email'].astype(str)
                        df['horario'] = df['email'].astype(str)

                    for index, row in tqdm(df.iterrows(), desc="Actualizando", total=df.shape[0]):
                        link = row['link']
                        try:
                            soup = self.fetch_page_data(link)
                            if soup is None: continue
                            

                            ## Obtener el email
                            customer_mail = ""
                            contenedor_div = soup.find("div", class_="contenedor")
                            if contenedor_div and 'data-business' in contenedor_div.attrs:
                                data_business = contenedor_div.attrs['data-business']
                                data_business_json = json.loads(data_business)
                                customer_mail = data_business_json.get('customerMail', "")

                            ## Obtener horarios
                            horario=""
                            horario_div = soup.find('div', {'id': 'horario'})
                            horarios = [] # Inicializar una lista para guardar cada entrada de horario
                            if horario_div:
                                for p in horario_div.find_all('p'): # Iterar sobre cada <p> en el div
                                    dia = p.find('b').text if p.find('b') else ''  # Obtener el nombre del día (contenido en <b>)
                                    times = p.find_all('time')  # Obtener todos los elementos <time> y sus horarios
                                    if times: # Extraer los horarios y unirlos con ' y '
                                        horario_texto = ' y '.join(time.get_text(strip=True) for time in times)
                                        horarios.append(f"{dia} {horario_texto}")
                                    else:
                                        horarios.append(f"{dia} Cerrado") # Manejar los días cerrados
                                horario = " | ".join(horarios)


                            df.at[index, 'email'] = customer_mail
                            df.at[index, 'horario'] = horario                  

                            time.sleep(2) ## Espremos 2seg para la siguiente
                        except requests.RequestException as e:
                            continue
                        except Exception as e:
                            continue

                    # Reemplazar NaN con cadenas vacías
                    #df.fillna("", inplace=True)

                    ## Guardar el DataFrame actualizado
                    df.to_excel(file_path, index=False)
                except Exception as e:
                    print(f"Error al procesar {file_name}: {e}")

    def procces_scrape_data(self):
        ## Obtener cantidad de reultados y páginas por actividad
        base_act = []
        for url in tqdm(self.urls, desc="Creando archivo base"):
            base_act.append(self.fetch_initial_data(url))

        ## Crear archivo donde se guardará la informacion
        file_path, isExist = Helper.create_excel_if_not_exists(self.name_file, "./data", "Actividades") # Creamos xlsx de guia
        df_act = pd.DataFrame(base_act, columns=["actividades", "resultados", "paginas"])

        df_existente = pd.read_excel(file_path, sheet_name='Actividades')
        df_actualizado = pd.concat([df_existente, df_act], ignore_index=True).drop_duplicates(subset=['actividades'], keep='last')

        with pd.ExcelWriter(file_path, mode="a", engine="openpyxl",if_sheet_exists="replace", ) as writer:
            df_actualizado.to_excel(writer, sheet_name='Actividades', index=False)

        ## Procesar las paginas y extraer datos
        for url_base in self.urls:
            Helper.printr(f"Raspando pagina - {url_base}")
            pages = int(df_actualizado.loc[df_actualizado["actividades"] == url_base, "paginas"].to_list()[0])
            if pages > 0:
                results = self.scrape_data(url_base, pages)
            
                # Creamos un xlsx por cada actividad
                match = re.search(r'/([^/]+)/$', url_base)
                sheet_name = match.group(1)

                file_path, isExist = Helper.create_excel_if_not_exists(f"{sheet_name}.xlsx", "./data", sheet_name)
                df_final = pd.DataFrame(results, columns=['link','nombre', 'email', 'categoria','descripcion','web','phone','direccion','cod_postal','locality','region', 'logo','logo_web','horario','actividad'])
                
                df_existente_final = pd.read_excel(file_path, sheet_name=sheet_name)
                df_actualizado_final = pd.concat([df_existente_final, df_final], ignore_index=True)
                
                with pd.ExcelWriter(file_path, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
                    df_actualizado_final.to_excel(writer, sheet_name=sheet_name, index=False)

    def main(self):
        Helper.clear_console()
        Helper.printr("*** INICIO ***")

        info_mode = "Actualizar" if self.mode == "U" else "Buscar información"
        Helper.printr(f"*** Modo: {info_mode} ***", 'below')

        if self.mode == "I":
            self.procces_scrape_data()
        if self.mode == "U":
            self.process_update_files("update")
            
        Helper.printr("*** FIN ***", 'above')

