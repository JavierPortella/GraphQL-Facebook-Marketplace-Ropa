from datetime import datetime, timedelta
from json import loads, JSONDecodeError
from os import _exit, getenv, makedirs, path
from time import localtime, sleep, strftime, time

from dotenv import load_dotenv
from openpyxl import load_workbook
import pandas as pd
from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, ElementNotInteractableException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

fb_mkp_ropa = {
    "Fecha Extraccion": [],
    "titulo_marketplace": [],
    "tiempo_creacion": [],
    "tipo_delivery": [],
    "delivery_data": [],
    "delivery_direccion": [],
    "descripcion": [],
    "disponible": [],
    "vendido": [],
    "fecha_union_vendedor": [],
    "cantidad": [],
    "precio": [],
    "tipo_moneda": [],
    "amount_with_concurrency": [],
    "latitud": [],
    "longitud": [],
    "locacion": [],
    "locacion_id": [],
    "name_vendedor": [],
    "tipo_vendedor": [],
    "id_vendedor": []
}

fb_mkp_ropa_time = {
    "Fecha" : None,
    "Hora Inicio": None,
    "Hora Termino": None,
    "Cantidad": None,
    "Tiempo(HHMMSS)": None,
    "Productos/min": None,
    "Enlace": None,
    "Observaciones": None,
}

class ScraperFb:
    """Representa a un bot para hacer web scarping en fb marketplace.

    Attributes:
        driver (Object): Maneja un navegador para hacer web scraping
        wait (Object): Maneja el Tiempo de espera durante la ejecución del bot
    """
    
    def __init__(self):
        """Inicializa un objeto de tipo ScraperFb.

        Args:
            driver (Object): [Driver]
            wait (Object): [Wait]
        """
        chrome_options = webdriver.ChromeOptions()
        prefs = {"profile.default_content_setting_values.notifications" : 2}
        chrome_options.add_experimental_option("prefs",prefs)
        self.driver = webdriver.Chrome(chrome_options=chrome_options,service=Service(ChromeDriverManager().install()))
        self.wait = WebDriverWait(self.driver, 10)

    def iniciar_sesion(self, url):
        """Inicia sesión en una página web usando un usuario y contraseña

        Args:
            url (str): [Url]
        """
        self.driver.get(url)
        self.driver.maximize_window()
        username = self.wait.until(EC.presence_of_element_located((By.ID, "email")))
        password = self.wait.until(EC.presence_of_element_located((By.ID, "pass")))
        username.clear()
        password.clear()
        username.send_keys(getenv('FB_USERNAME'))
        password.send_keys(getenv('FB_PASSWORD'))
        self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[name='login']"))).click()
    
    def mapear_datos(self, url):
        sleep(10)
        self.driver.execute_script("window.open('about:blank', 'newtab');")
        self.driver.switch_to.window("newtab")
        self.driver.get(url)
        
        sleep(8)        
        ropa = self.driver.find_elements(By.XPATH, '//*[@class="xt7dq6l xl1xv1r x6ikm8r x10wlt62 xh8yej3"]')
        fecha_publicacion = fecha_extraccion = int(datetime.strptime(fb_mkp_ropa_time["Fecha"],"%d/%m/%Y").timestamp())
        fecha_flag = fecha_extraccion + 86400
        i=0
        e=0
        while fecha_publicacion >= fecha_extraccion:
            print("Scrapeando item", i + 1)
            try:
                ropa[i].click()
                sleep(5)
                for request in self.driver.requests:
                    if not request.response or 'graphql' not in request.url:
                        continue
                    
                    body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                    decoded_body = body.decode('utf-8')
                    json_data = loads(decoded_body)
                    
                    if 'prefetch_uris_v2' not in json_data['extensions']:
                        continue

                    fecha_publicacion = json_data['data']['viewer']['marketplace_product_details_page']['target']['creation_time']
                    print(fecha_publicacion)
                    if fecha_publicacion < fecha_flag:
                        dato = json_data['data']['viewer']['marketplace_product_details_page']
                        print(dato["target"]["marketplace_listing_title"])
                        self.extraer_datos(dato, fb_mkp_ropa_time["Fecha"])
                    break
                self.driver.execute_script("window.history.go(-1)");
                
            except (NoSuchElementException, JSONDecodeError, StaleElementReferenceException) as error:
                print("Error:",error)
                print('No se hallo el item N '+str(i + 1)+'se pasará al siguiente')
                e=e+1
                
            except (KeyError, ElementNotInteractableException) as error:
                print("Error:",error)
                print('No se puede obtener la data del item N '+str(i + 1)+'se pasará al siguiente')
                e=e+1
                self.driver.execute_script("window.history.go(-1)")
                
            except Exception as error:
                print("Error:", error)
                e = e + 1
                self.guardar_datos()
                _exit(0)
            i = i + 1
            if i == len(ropa):
                self.driver.execute_script('window.scrollTo(0, document.body.scrollHeight)')
                sleep(7)
                ropa = self.driver.find_elements(By.XPATH, '//*[@class="xt7dq6l xl1xv1r x6ikm8r x10wlt62 xh8yej3"]')
            del self.driver.requests
            sleep(3)
            print('-------------------------------------------------------------------')
        fb_mkp_ropa_time["Cantidad"]= i - e
        print("Se halló", e, "errores")
        print('-------------------------------------------------------------------')
        print('Fin de la extraccion')
        print('-------------------------------------------------------------------')
    
    def extraer_datos(self, item, fecha_extraccion):
        fb_mkp_ropa["titulo_marketplace"].append(item['target'].get('marketplace_listing_title'))
        fb_mkp_ropa["tiempo_creacion"].append(item['target'].get('creation_time'))
        fb_mkp_ropa["disponible"].append(item['target'].get('is_live'))
        fb_mkp_ropa["vendido"].append(item['target'].get('is_sold'))
        fb_mkp_ropa["cantidad"].append(item['target'].get('listing_inventory_type'))
        fb_mkp_ropa["name_vendedor"].append(item['target'].get('story').get('actors')[0].get('name'))
        fb_mkp_ropa["tipo_vendedor"].append(item['target'].get('story').get('actors')[0]['__typename'])
        fb_mkp_ropa["id_vendedor"].append(item['target'].get('story').get('actors')[0]['id'])
        fb_mkp_ropa["locacion_id"].append(item['target'].get('location_vanity_or_id'))
        fb_mkp_ropa["latitud"].append(item['target'].get('location', {}).get('latitude'))
        fb_mkp_ropa["longitud"].append(item['target'].get('location', {}).get('longitude'))
        fb_mkp_ropa["precio"].append(item['target'].get('listing_price', {}).get('amount'))
        fb_mkp_ropa["tipo_moneda"].append(item['target'].get('listing_price', {}).get('currency'))
        fb_mkp_ropa["amount_with_concurrency"].append(item['target'].get('listing_price', {}).get('amount_with_offset_in_currency'))
        fb_mkp_ropa["tipo_delivery"].append(item['target'].get('delivery_types', [None])[0])
        fb_mkp_ropa["delivery_data"].append(item['target'].get("delivery_data", {}).get('carrier'))
        fb_mkp_ropa["delivery_direccion"].append(item['target'].get("delivery_data", {}).get('delivery_address'))
        fb_mkp_ropa["descripcion"].append(item['target'].get('redacted_description', {}).get('text'))
        fb_mkp_ropa["fecha_union_vendedor"].append(item['target'].get('marketplace_listing_seller', {}).get('join_time'))  
        data = item['target'].get('location_text', {})
        if data:
            data = data.get('text')
        fb_mkp_ropa["locacion"].append(data)
        fb_mkp_ropa["Fecha Extraccion"].append(fecha_extraccion)
    
    def guardar_datos(self):
        df_fb_mkp_ropa = pd.DataFrame(fb_mkp_ropa)
        df_fb_mkp_ropa.drop(len(df_fb_mkp_ropa)-1, axis=0, inplace=True)
        fb_mkp_ropa_time["Cantidad"] = len(df_fb_mkp_ropa)
        datetime_obj = datetime.strptime(fb_mkp_ropa_time["Fecha"],"%d/%m/%Y")
        filepath = "Data/" + datetime_obj.strftime('%d-%m-%Y') + "/"
        filename = "fb_ropa_" + datetime_obj.strftime('%d%m%Y') + "_" + str(fb_mkp_ropa_time["Cantidad"]) + ".xlsx"
        if not path.exists(filepath):
            makedirs(filepath)
        df_fb_mkp_ropa.to_excel(filepath + filename, index = False)
        print("Datos Guardados Correctamente")
        
    def guardar_tiempos(self, filename, sheet_name):
        tiempos = load_workbook(filename)
        header_exist = True
        if sheet_name not in [ws.title for ws in tiempos.worksheets]:
            tiempos.create_sheet(sheet_name)
            header_exist = False
        worksheet = tiempos[sheet_name]
        if not header_exist:
            worksheet.append(list(fb_mkp_ropa_time.keys()))
        worksheet.append(list(fb_mkp_ropa_time.values()))
        tiempos.save(filename)
        tiempos.close()
        print("Tiempos Guardados Correctamente")

def set_params_inicio():
    print("Estableciendo parámetros de inicio")
    fb_mkp_ropa_time["Fecha"] = (datetime.now().date() - timedelta(days=1)).strftime('%d/%m/%Y')
    start = time()
    fb_mkp_ropa_time["Hora Inicio"] = strftime("%H:%M:%S", localtime(start))
    print("Hora de inicio:",fb_mkp_ropa_time["Hora Inicio"])
    return start

def set_params_final(start):
    print("Estableciendo parámetros finales")
    end = time()
    fb_mkp_ropa_time["Hora Termino"] = strftime("%H:%M:%S", localtime(end))
    print("Hora Termino:",fb_mkp_ropa_time["Hora Termino"])
    total = end - start
    print("Duracion: ",total, 'seconds')
    fb_mkp_ropa_time["Tiempo(HHMMSS)"] = str(timedelta(seconds=total)).split(".")[0]
    fb_mkp_ropa_time["Productos/min"] = int(fb_mkp_ropa_time["Cantidad"]/(total / 60))

def main():
    # Cargar variables de entorno
    load_dotenv()
    
    # Estabbleciendo hora y fecha de inicio de la extracción
    start = set_params_inicio()
    
    # Url base a scrapear
    url_base = 'https://www.facebook.com/'
    url_ropa = '"https://www.facebook.com/marketplace/category/apparel/?sortBy=creation_time_descend&exact=false"'
    
    # Parámetros para guardar la medición de la ejecución del scraper
    filename_tiempos = 'Tiempos.xlsx'
    sheet_tiempos = "Ropa"
    
    scraper = ScraperFb()
    scraper.iniciar_sesion(url_base)
    scraper.mapear_datos(url_ropa)
    scraper.guardar_datos()
    
    set_params_final(start)
    scraper.guardar_tiempos(filename_tiempos, sheet_tiempos)

if __name__ == '__main__':
    main()