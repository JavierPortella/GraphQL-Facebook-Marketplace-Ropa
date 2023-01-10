from datetime import datetime, timedelta
from json import loads, JSONDecodeError
from logging import basicConfig, CRITICAL, ERROR, getLogger, INFO, log
from os import getenv, makedirs, path
from re import findall
from time import localtime, sleep, strftime, time
from traceback import TracebackException

from dotenv import load_dotenv
from openpyxl import load_workbook, Workbook
from pandas import DataFrame
from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    ElementNotInteractableException,
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.remote_connection import LOGGER as seleniumLogger
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from urllib3.connectionpool import log as urllibLogger
from webdriver_manager.chrome import ChromeDriverManager


class Errores:
    def __init__(self):
        self._errores = {
            "Clase": [],
            "Mensaje": [],
            "Linea de Error": [],
            "Codigo Error": [],
            "Publicacion": [],
        }

    def _get_errores(self):
        return self._errores

    def _append_error(self, error, enlace):
        traceback_error = TracebackException.from_exception(error)
        error_message = traceback_error._str
        error_stack = traceback_error.stack[0]
        log(ERROR, error_message)
        self._errores["Clase"].append(traceback_error.exc_type)
        self._errores["Mensaje"].append(error_message)
        self._errores["Linea de Error"].append(error_stack.lineno)
        self._errores["Codigo Error"].append(error_stack.line)
        self._errores["Publicacion"].append(enlace)


class Dataset:
    def __init__(self):
        self._dataset = {
            "Fecha Extraccion": [],
            "titulo_marketplace": [],
            "tiempo_creacion": [],
            "tipo_delivery": [],
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
            "id_vendedor": [],
            "enlace": [],
        }

    def _get_dataset(self):
        return self._dataset

    def _append_data(self, item, fecha_extraccion, enlace):
        self._dataset["titulo_marketplace"].append(
            item.get("marketplace_listing_title")
        )
        self._dataset["tiempo_creacion"].append(item.get("creation_time"))
        self._dataset["disponible"].append(item.get("is_live"))
        self._dataset["vendido"].append(item.get("is_sold"))
        self._dataset["cantidad"].append(item.get("listing_inventory_type"))
        self._dataset["name_vendedor"].append(
            item.get("story").get("actors")[0].get("name")
        )
        self._dataset["tipo_vendedor"].append(
            item.get("story").get("actors")[0]["__typename"]
        )
        self._dataset["id_vendedor"].append(item.get("story").get("actors")[0]["id"])
        self._dataset["locacion_id"].append(item.get("location_vanity_or_id"))
        self._dataset["latitud"].append(item.get("location", {}).get("latitude"))
        self._dataset["longitud"].append(item.get("location", {}).get("longitude"))
        self._dataset["precio"].append(item.get("listing_price", {}).get("amount"))
        self._dataset["tipo_moneda"].append(
            item.get("listing_price", {}).get("currency")
        )
        self._dataset["amount_with_concurrency"].append(
            item.get("listing_price", {}).get("amount_with_offset_in_currency")
        )
        self._dataset["tipo_delivery"].append(item.get("delivery_types", [None])[0])
        self._dataset["descripcion"].append(
            item.get("redacted_description", {}).get("text")
        )
        self._dataset["fecha_union_vendedor"].append(
            item.get("marketplace_listing_seller", {}).get("join_time")
        )
        data = item.get("location_text", {})
        if data:
            data = data.get("text")
        self._dataset["locacion"].append(data)
        self._dataset["Fecha Extraccion"].append(fecha_extraccion)
        self._dataset["enlace"].append(enlace)


class Tiempo:
    def __init__(self, start):
        self._hora_inicio = strftime("%H:%M:%S", localtime(start))
        log(INFO, f"Hora de inicio: {self._hora_inicio}")
        self._fecha = (datetime.now().date() - timedelta(days=1)).strftime("%d/%m/%Y")
        self._hora_fin = None
        self._cantidad = None
        self._tiempo = None
        self._productos_por_min = None
        self._enlace = None
        self._errores = None

    def _get_fecha(self):
        return self._fecha

    def _get_errores(self):
        return self._errores

    def _set_cantidad(self, cantidad):
        self._cantidad = cantidad

    def _set_errores(self, errores):
        self._errores = errores

    def _set_param_final(self, start):
        end = time()
        self._hora_fin = strftime("%H:%M:%S", localtime(end))
        log(INFO, f"Productos Extraídos: {self._cantidad}")
        log(INFO, f"Hora Fin: {self._hora_fin}")
        total = end - start
        self._tiempo = str(timedelta(seconds=total)).split(".")[0]
        self._productos_por_min = int(round(self._cantidad / (total / 60), 0))


class ScraperFb:
    """Representa a un bot para hacer web scarping en fb marketplace.

    Attributes:
        driver (Object): Maneja un navegador para hacer web scraping
        wait (Object): Maneja el Tiempo de espera durante la ejecución del bot
    """

    def __init__(self, start):
        """Inicializa un objeto de tipo ScraperFb.

        Args:
            driver (Object): [Driver]
            wait (Object): [Wait]
        """
        log(INFO, "Inicializando scraper")
        self._tiempo = Tiempo(start)
        chrome_options = webdriver.ChromeOptions()
        prefs = {"profile.default_content_setting_values.notifications": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(
            chrome_options=chrome_options,
            service=Service(ChromeDriverManager().install()),
        )
        self.wait = WebDriverWait(self.driver, 10)
        self._errores = Errores()
        self._data = Dataset()

    def _get_data(self):
        return self._data

    def _get_errores(self):
        return self._errores

    def iniciar_sesion(self, url):
        """Inicia sesión en una página web usando un usuario y contraseña

        Args:
            url (str): [Url]
        """
        log(INFO, "Iniciando sesión")
        self.driver.get(url)
        self.driver.maximize_window()
        username = self.wait.until(EC.presence_of_element_located((By.ID, "email")))
        password = self.wait.until(EC.presence_of_element_located((By.ID, "pass")))
        username.clear()
        password.clear()
        username.send_keys(getenv("FB_USERNAME"))
        password.send_keys(getenv("FB_PASSWORD"))
        self.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[name='login']"))
        ).click()
        log(INFO, "Inicio de sesión con éxito")

    def mapear_datos(self, url):
        sleep(10)
        log(INFO, "Accediendo a la URL")
        self.driver.execute_script("window.open('about:blank', 'newtab');")
        self.driver.switch_to.window("newtab")
        self.driver.get(url)

        sleep(8)
        log(INFO, "Mapeando Publicaciones")
        ropa = self.driver.find_elements(
            By.XPATH, '//*[@class="xt7dq6l xl1xv1r x6ikm8r x10wlt62 xh8yej3"]'
        )
        fecha_publicacion = fecha_extraccion = int(
            datetime.strptime(self._tiempo._get_fecha(), "%d/%m/%Y").timestamp()
        )
        fecha_flag = fecha_extraccion + 86400
        i = 0
        e = 0
        del self.driver.requests

        while fecha_publicacion >= fecha_extraccion:
            log(INFO, f"Scrapeando item {i + 1}")

            try:
                try:
                    enlace = findall(
                        "(.*)\/\?",
                        ropa[i]
                        .find_element(By.XPATH, ".//ancestor::a")
                        .get_attribute("href"),
                    )[0]
                except NoSuchElementException as error:
                    enlace = None
                    self._errores._append_error(error, enlace)
                ropa[i].click()
                sleep(5)
                for request in self.driver.requests:
                    if not request.response or "graphql" not in request.url:
                        continue

                    body = decode(
                        request.response.body,
                        request.response.headers.get("Content-Encoding", "identity"),
                    )
                    decoded_body = body.decode("utf-8")
                    json_data = loads(decoded_body)

                    if "prefetch_uris_v2" not in json_data["extensions"]:
                        continue

                    fecha_publicacion = json_data["data"]["viewer"][
                        "marketplace_product_details_page"
                    ]["target"]["creation_time"]
                    if fecha_publicacion < fecha_flag:
                        dato = json_data["data"]["viewer"][
                            "marketplace_product_details_page"
                        ]["target"]
                        log(INFO, f"{dato['marketplace_listing_title']}")
                        self._data._append_data(dato, self._tiempo._get_fecha(), enlace)
                        log(INFO, f"Item {i + 1} scrapeado con éxito")
                    break
                self.driver.execute_script("window.history.go(-1)")

            except (
                NoSuchElementException,
                ElementNotInteractableException,
                StaleElementReferenceException,
            ) as error:
                self._errores._append_error(error, enlace)
                e = e + 1

            except (KeyError, JSONDecodeError) as error:
                self._errores._append_error(error, enlace)
                e = e + 1
                self.driver.execute_script("window.history.go(-1)")

            except Exception as error:
                self._errores._append_error(error, enlace)
                e = e + 1
                log(CRITICAL, "Se detuvo inesperadamente el programa")
                log(CRITICAL, f"Causa:\n{error}")
                break
            i = i + 1
            if i == len(ropa):
                self.driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight)"
                )
                sleep(7)
                ropa = self.driver.find_elements(
                    By.XPATH, '//*[@class="xt7dq6l xl1xv1r x6ikm8r x10wlt62 xh8yej3"]'
                )

            del self.driver.requests
            log(
                INFO,
                "-------------------------------------------------------------------",
            )
            sleep(3)
        self._tiempo._set_errores(e)
        log(INFO, f"Se halló {e} errores")
        log(INFO, "Fin de la extraccion")

    def guardar_datos(
        self, dataset, filetype="Data", folder="Data", filename="fb_data"
    ):
        log(INFO, f"Guardando {filetype}")
        df_fb_mkp_ropa = DataFrame(dataset)
        if filetype == "Data":
            df_fb_mkp_ropa.drop(len(df_fb_mkp_ropa) - 1, axis=0, inplace=True)
            cantidad = len(df_fb_mkp_ropa)
            self._tiempo._set_cantidad(cantidad)
        elif filetype == "Error":
            cantidad = self._tiempo._get_errores()
        else:
            return

        datetime_obj = datetime.strptime(self._tiempo._get_fecha(), "%d/%m/%Y")
        filepath = folder + "/" + datetime_obj.strftime("%d-%m-%Y") + "/"
        filename = (
            filename
            + "_"
            + datetime_obj.strftime("%d%m%Y")
            + "_"
            + str(cantidad)
            + ".xlsx"
        )
        if not path.exists(filepath):
            makedirs(filepath)
        df_fb_mkp_ropa.to_excel(filepath + filename, index=False)
        log(INFO, f"{filetype} Guardados Correctamente")

    def guardar_tiempos(self, filename, sheet_name, start):
        log(INFO, "Guardando tiempos")
        self._tiempo._set_param_final(start)
        header_exist = True
        if path.isfile(filename):
            tiempos = load_workbook(filename)
            if sheet_name not in [ws.title for ws in tiempos.worksheets]:
                tiempos.create_sheet(sheet_name)
                header_exist = False
        else:
            tiempos = Workbook()
            tiempos.create_sheet(sheet_name)
            header_exist = False
        worksheet = tiempos[sheet_name]
        if not header_exist:
            worksheet.append(list(self._tiempo.__dict__.keys()))
        worksheet.append(list(self._tiempo.__dict__.values()))
        tiempos.save(filename)
        tiempos.close()
        log(INFO, "Tiempos Guardados Correctamente")


def config_log():
    seleniumLogger.setLevel(ERROR)
    urllibLogger.setLevel(ERROR)
    urllibLogger.propagate = False
    logger = getLogger("seleniumwire")
    logger.setLevel(ERROR)
    basicConfig(format="%(asctime)s %(message)s", level=INFO)


def validar_parametros(parametros):
    for parametro in parametros:
        if not parametro:
            log(ERROR, f"Parámetros incorrectos")
            return False
    log(INFO, "Parámetros válidos")


def main():
    # Formato para el debugger
    log(INFO, "Configurando Formato Básico del Debugger")
    config_log()

    # Cargar variables de entorno
    log(INFO, "Cargando Variables de entorno")
    load_dotenv()

    start = time()

    # Url base a scrapear
    url_base = getenv("URL_BASE")
    url_ropa = getenv("URL_CATEGORY")

    # Parámetros para guardar la data extraída por el scraper
    data_filename = getenv("DATA_FILENAME")
    data_folder = getenv("DATA_FOLDER")

    # Parámetros para guardar la medición de la ejecución del scraper
    filename_tiempos = getenv("FILENAME_TIEMPOS")
    sheet_tiempos = getenv("SHEET_TIEMPOS")

    # Parámetros para guardar los errores durante la ejecución por el scraper
    error_filename = getenv("ERROR_FILENAME")
    error_folder = getenv("ERROR_FOLDER")

    # Validar parámetros
    if not validar_parametros(
        [
            url_base,
            url_ropa,
            data_filename,
            data_folder,
            filename_tiempos,
            sheet_tiempos,
            error_filename,
            error_folder,
        ]
    ):
        return

    # Inicializar scrapper
    scraper = ScraperFb(start)

    # Iniciar sesión
    scraper.iniciar_sesion(url_base)

    # Extracción de datos
    scraper.mapear_datos(url_ropa)

    # Guardando la data extraída por el scraper
    scraper.guardar_datos(
        scraper._get_data()._get_dataset(), "Data", data_folder, data_filename
    )

    # Guardando los errores extraídos por el scraper
    scraper.guardar_datos(
        scraper._get_errores()._get_errores(), "Error", error_folder, error_filename
    )

    # Guardando los tiempos durante la ejecución del scraper
    scraper.guardar_tiempos(filename_tiempos, sheet_tiempos, start)
    log(INFO, "Programa ejecutado satisfactoriamente")


if __name__ == "__main__":
    main()
