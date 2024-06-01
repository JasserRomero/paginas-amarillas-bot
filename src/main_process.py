from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import requests
import numpy as np
import time
import gspread
from helpers import Helper
import re


class Main:
    def __init__(self, max_workers=5, batch_size=3):
        self.proxie = {"https": 'http://brd-auth-token:F4p85TcsFzK7AbHfBynjN2hCaJbJ5AGc@pmgr-customer-hl_9b4af93c.brd.superproxy.io:24005' }
        self.headers = {'User-agent': 'Mozilla/5.0'}
        self.name_file = 'paginasamarillas_filtrado.xlsx'
        self.max_workers = max_workers
        self.batch_size = batch_size

    def get_activities(self):
        activities_list = [
            #'https://www.paginasamarillas.es/a/mercado-inmobiliario/',
            #'https://www.paginasamarillas.es/a/servicio-inmobiliario/',
            #'https://www.paginasamarillas.es/a/abogados/',
            #'https://www.paginasamarillas.es/a/abogada/',
            #'https://www.paginasamarillas.es/a/gestores/',
            'https://www.paginasamarillas.es/a/gestoria-administrativa/',
            ## Agregar a la lista paginas que se desean scrapear 
        ]
        return activities_list

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


    def fetch_page_data(self, url):
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup
        except requests.Timeout:
            print(f"Tiempo de espera excedido para la URL: {url}")
            return None
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

    def process_soup(self, soup, url_base):
        data = []
        boxes = soup.find_all("div", {"class": "box"})
        link=""
        web=""
        name=""
        categ=""
        desc=""
        address=""
        postal=""
        locality=""
        logo=""
        logo_web=""
        horario=""
        region=""
        for box in boxes[2:]:
            time.sleep(2)
            try:
                web = box.find("a", {"class": "web"}).get('href')
                l = requests.get(web, headers=self.headers)
                loup = BeautifulSoup(l.text, 'html.parser')
                desc_web = loup.find("meta", {"property": "og:description"})
                logo_web = loup.find("img").get("src")
            except:
                web, desc_web, logo_web = "", "", ""
            
            try: name=box.find("span",{"itemprop":"name"}).text
            except: name = ""

            try:categ=box.find("p",{"class":"categ"}).text
            except:categ=""

            try:desc=box.find("div",{"itemprop":"description"}).text
            except:desc=""

            try:address=box.find("span",{"itemprop":"streetAddress"}).text
            except:address=""

            try:postal=box.find("span",{"itemprop":"postalCode"}).text
            except:postal=""

            try:locality=box.find("span",{"itemprop":"addressLocality"}).text
            except:locality=""

            try:region=box.find("span",{"itemprop":"addressRegion"}).text
            except:region=locality

            try:
                link = box.find("a").get('href')
                l=requests.get(link, headers=headers)
                loup = BeautifulSoup(l.text, 'html.parser')
                try:desc += " | " + loup.find("p",{"class":"line-fluid"}) 
                except: pass
                try: horario=loup.find("div",{"id":"horario"}).text 
                except: pass
            except:link=""

            try:tel=box.find("span",{"itemprop":"telephone"}).text
            except:tel=""

            try:logo=box.find("img",{"itemprop":"image"}).get("src")
            except:logo=""

            data.append([link,name,categ,desc,web,tel,address,postal,locality,region,logo,logo_web,horario,url_base])
        return data

    def scrape_data(self, url_base, num_pages):
        data = []
        for page in tqdm(range(int(num_pages))):
            print(page)
            print(f"{url_base}{page}")
            page = page + 0
            if page == num_pages: break
            url = f"{url_base}{page}"
            soup = self.fetch_page_data(url)
            if soup:
                data.extend(self.process_soup(soup, url_base))

        return data

    def main(self):
        print("*** INICIO ***")
        ## Obtener cantidad de reultados y páginas por actividad
        base_act = []
        with ThreadPoolExecutor(max_workers=Helper.get_cpu_count()) as executor:
            future_to_url = { executor.submit(self.fetch_initial_data, url): url for url in self.get_activities() }
            for future in as_completed(future_to_url):
                base_act.append(future.result())

        ## Crear archivo donde se guardará la informacion
        file_path, isExist = Helper.create_excel_if_not_exists(self.name_file, "./data", "Actividades") # Creamos xlsx de guia
        df_act = pd.DataFrame(base_act, columns=["actividades", "resultados", "paginas"])

        df_existente = pd.read_excel(file_path, sheet_name='Actividades')
        df_actualizado = pd.concat([df_existente, df_act], ignore_index=True).drop_duplicates(subset=['actividades'], keep='last')

        with pd.ExcelWriter( file_path, mode="a", engine="openpyxl",if_sheet_exists="replace", ) as writer:
            df_actualizado.to_excel(writer, sheet_name='Actividades', index=False)

        ## Procesar las paginas y extraer datos
        for url_base in self.get_activities():
            all_data = []
            pages = int(df_actualizado.loc[df_actualizado["actividades"] == url_base, "paginas"].to_list()[0])
            
            if pages > 0:
                results = self.scrape_data(url_base, 2)
                all_data.extend(results)
            
            # Creamos un xlsx por cada actividad
            match = re.search(r'/([^/]+)/$', url_base)

            sheet_name = match.group(1)
            file_path, isExist = Helper.create_excel_if_not_exists(f"{sheet_name}.xlsx", "./data", sheet_name)
            df_final = pd.DataFrame(all_data, columns=['link','nombre','categoria','descripcion','web','phone','direccion','cod_postal','locality','region', 'logo','logo_web','horario','actividad'])
            
            df_existente = pd.read_excel(file_path, sheet_name=sheet_name)
            df_actualizado = pd.concat([df_existente, df_final], ignore_index=True)
            
            with pd.ExcelWriter(file_path, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
                df_actualizado.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print("*** FIN ***")

if __name__ == "__main__":
    Main = Main()
    Main.main()