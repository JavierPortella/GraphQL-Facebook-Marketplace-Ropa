from datetime import datetime, timedelta
from json import loads, JSONDecodeError
from logging import (
    basicConfig,
    CRITICAL,
    ERROR,
    FileHandler,
    getLogger,
    INFO,
    log,
    StreamHandler,
)
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
    """
    Representa a los errores ocurridos durante la ejecución de un scraper

    ...

    Attributes
    ----------
    errores : dict
        Conjunto de datos que contiene toda información de los errores ocurridos durante la ejecución del scraper

    Methods
    -------
    agregar_error(error, enlace):
        Agrega la información de un error al diccionario de datos errores
    """

    def __init__(self):
        """
        Genera todos los atributos para el objeto Errores
        """
        self._errores = {
            "Clase": [],
            "Mensaje": [],
            "Linea de Error": [],
            "Codigo Error": [],
            "Publicacion": [],
        }

    @property
    def errores(self):
        """Retorna el valor actual del diccionario de datos errores"""
        return self._errores

    def agregar_error(self, error, enlace):
        """
        Agrega la información de un error al diccionario de datos errores

        Parameters
        ----------
        error: Exception
            Objeto de tipo excepción ocurrida durante la ejecución del scraper
        enlace: str
            Enlace de la publicación de la página facebook marketplace

        Returns
        -------
        None
        """
        log(ERROR, error)
        traceback_error = TracebackException.from_exception(error)
        error_stack = traceback_error.stack[0]
        self._errores["Clase"].append(traceback_error.exc_type)
        self._errores["Mensaje"].append(traceback_error._str)
        self._errores["Linea de Error"].append(error_stack.lineno)
        self._errores["Codigo Error"].append(error_stack.line)
        self._errores["Publicacion"].append(enlace)


class Dataset:
    """
    Representa al conjunto de datos generado por el scraper

    ...

    Attributes
    ----------
    dataset : dict
        Conjunto de datos que contiene toda información extraída de una categoría de la página de facebook marketplace

    Methods
    -------
    agregar_data():
        Agrega la información de una publicación al diccionario de datos dataset
    """

    def __init__(self):
        """
        Genera todos los atributos para el objeto Dataset
        """
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

    @property
    def dataset(self):
        """Retorna el valor actual del diccionario de datos dataset"""
        return self._dataset

    def agregar_data(self, item, fecha_extraccion, enlace):
        """
        Agrega la información de una publicación al dataset

        Parameters
        ----------
        item: dict
            Conjunto de datos que contiene toda la información de una publicación
        fecha_extraccion: str
            Fecha actual en la que se creó una publicación en formato %d/%m/%Y
        enlace: str
            Enlace de la publicación de la página facebook marketplace

        Returns
        -------
        None
        """
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
    """
    Representa el tiempo que se demora el scraper en extraer la información

    ...

    Attributes
    ----------
    start : float
        Hora actual en segundos
    hora_inicio : str
        Hora de inicio de la ejecución del scraper en formato %H:%M:%S
    fecha : str
        Fecha de las publicaciones a extraer en formato %d/%m/%Y
    hora_fin : str
        Hora de término de la ejecución del scraper en formato %H:%M:%S
    cantidad : int
        Cantidad de publicaciones extraídas de la página de facebook marketplace
    cantidad_real: int
        Cantidad real de publicaciones extraídas de la página de facebook marketplace
    tiempo : str
        Tiempo de ejecución del scraper en formato %d days, %H:%M:%S
    productos_por_min : float
        Cantidad de publicaciones que puede extraer el scraper en un minuto
    productos_por_min_real : float
        Cantidad real de publicaciones que puede extraer el scraper en un minuto
    num_error : int
        Cantidad de errores ocurridos durante la ejecución del scraper

    Methods
    -------
    set_param_final():
        Establece los parámetros finales cuando se termina de ejecutar el scraper
    """

    def __init__(self, fecha_actual):
        """
        Genera todos los atributos para el objeto Tiempo

        Parameters
        ----------
        fecha_actual: str
            Fecha en la que se ejecuta el scraper
        """
        self._start = time()
        self._hora_inicio = strftime("%H:%M:%S", localtime(self._start))
        log(INFO, f"Hora de inicio: {self._hora_inicio}")
        self._fecha = fecha_actual.strftime("%d/%m/%Y")
        self._hora_fin = None
        self._cantidad = None
        self._cantidad_real = None
        self._tiempo = None
        self._productos_por_min = None
        self._productos_por_min_real = None
        self._num_error = None

    @property
    def cantidad(self):
        """Retorna el valor actual o asigna un nuevo valor del atributo cantidad"""
        return self._cantidad

    @property
    def cantidad_real(self):
        """Retorna el valor actual o asigna un nuevo valor del atributo cantidad_real"""
        return self._cantidad_real

    @property
    def fecha(self):
        """Retorna el valor actual del atributo fecha"""
        return self._fecha

    @property
    def num_error(self):
        """Retorna el valor actual o asigna un nuevo valor del atributo num_error"""
        return self._num_error

    @cantidad.setter
    def cantidad(self, cantidad):
        self._cantidad = cantidad

    @cantidad_real.setter
    def cantidad_real(self, cantidad_real):
        self._cantidad_real = cantidad_real

    @num_error.setter
    def num_error(self, num_error):
        self._num_error = num_error

    def set_param_final(self):
        """
        Establece parametros finales para medir el tiempo de ejecución del scraper

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        end = time()
        self._hora_fin = strftime("%H:%M:%S", localtime(end))
        log(INFO, f"Productos Extraídos: {self._cantidad}")
        log(INFO, f"Hora Fin: {self._hora_fin}")
        total = end - self._start
        self._tiempo = str(timedelta(seconds=total)).split(".")[0]
        self._productos_por_min = round(self._cantidad / (total / 60), 2)
        self._productos_por_min_real = round(self._cantidad_real / (total / 60), 2)


class ScraperFb:
    """
    Representa a un bot para hacer web scraping en fb marketplace

    ...

    Attributes
    ----------
    tiempo : Tiempo
        Objeto de la clase Tiempo que maneja información del tiempo de ejecución del scraper
    driver: webdriver.Chrome
        Objeto de la clase webdriver que maneja un navegador para hacer web scraping
    wait : WebDriverWait
        Objeto de la clase WebDriverWait que maneja el Tiempo de espera durante la ejecución del scraper
    errores : Errores
        Objeto de la clase Errores que maneja información de los errores ocurridos durante la ejecución del scraper
    data : Dataset
        Objeto de la clase Dataset que maneja información de las publicaciones extraídas por el scraper

    Methods
    -------
    iniciar_sesion():
        Iniciar sesión en facebook usando un usuario y contraseña
    mapear_datos(url):
        Mapea y extrae los datos de las publicaciones de una categoría
    guardar_datos(dataset, filetype, folder, filename):
        Guarda los datos o errores obtenidos durante la ejecución del scraper
    guardar_tiempos(filename, sheet_name):
        Guarda la información del tiempo de ejecución del scraper
    """

    def __init__(self, fecha_actual):
        """
        Genera todos los atributos para el objeto ScraperFb

        Parameters
        ----------
        fecha_actual: str
            Fecha en la que se ejecuta el scraper
        """
        log(INFO, "Inicializando scraper")
        self._tiempo = Tiempo(fecha_actual)
        chrome_options = webdriver.ChromeOptions()
        prefs = {"profile.default_content_setting_values.notifications": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        self._driver = webdriver.Chrome(
            chrome_options=chrome_options,
            service=Service(ChromeDriverManager().install()),
        )
        self._wait = WebDriverWait(self._driver, 10)
        self._errores = Errores()
        self._data = Dataset()

    @property
    def data(self):
        """Retorna el valor actual del atributo data"""
        return self._data

    @property
    def errores(self):
        """Retorna el valor actual del atributo errores"""
        return self._errores

    def iniciar_sesion(self):
        """
        Inicia sesión en una página web usando un usuario y contraseña

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        log(INFO, "Iniciando sesión")
        self._driver.get("https://www.facebook.com/")
        self._driver.maximize_window()
        username = self._wait.until(EC.presence_of_element_located((By.ID, "email")))
        password = self._wait.until(EC.presence_of_element_located((By.ID, "pass")))
        username.clear()
        password.clear()
        username.send_keys(getenv("FB_USERNAME"))
        password.send_keys(getenv("FB_PASSWORD"))
        self._wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[name='login']"))
        ).click()
        log(INFO, "Inicio de sesión con éxito")

    def mapear_datos(self, url):
        """
        Mapea y extrae los datos de las publicaciones de una categoría

        Parameters
        ----------
        url: str
            Link de la página de una categoría en facebook marketplace

        Returns
        -------
        None
        """
        sleep(10)
        log(INFO, "Accediendo a la URL")
        self._driver.execute_script("window.open('about:blank', 'newtab');")
        self._driver.switch_to.window("newtab")
        self._driver.get(url)

        sleep(8)
        log(INFO, "Mapeando Publicaciones")
        ropa = self._driver.find_elements(
            By.XPATH, '//*[@class="xt7dq6l xl1xv1r x6ikm8r x10wlt62 xh8yej3"]'
        )
        fecha_publicacion = fecha_extraccion = int(
            datetime.strptime(self._tiempo.fecha, "%d/%m/%Y").timestamp()
        )
        fecha_flag = fecha_extraccion + 86400
        i = 0
        e = 0
        del self._driver.requests

        while fecha_publicacion >= fecha_extraccion:
            try:
                log(INFO, f"Scrapeando item {i + 1}")
                try:
                    enlace = findall(
                        "(.*)\/\?",
                        ropa[i]
                        .find_element(By.XPATH, ".//ancestor::a")
                        .get_attribute("href"),
                    )[0]
                except NoSuchElementException as error:
                    enlace = None
                    self._errores.agregar_error(error, enlace)
                ropa[i].click()
                sleep(5)
                for request in self._driver.requests:
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
                        self._data.agregar_data(dato, self._tiempo.fecha, enlace)
                        log(INFO, f"Item {i + 1} scrapeado con éxito")
                    break
                self._driver.execute_script("window.history.go(-1)")

            except (
                NoSuchElementException,
                ElementNotInteractableException,
                StaleElementReferenceException,
            ) as error:
                self._errores.agregar_error(error, enlace)
                e += 1

            except (KeyError, JSONDecodeError) as error:
                self._errores.agregar_error(error, enlace)
                e += 1
                self._driver.execute_script("window.history.go(-1)")

            except Exception as error:
                self._errores.agregar_error(error, enlace)
                e += 1
                i += 1
                log(CRITICAL, "Se detuvo inesperadamente el programa")
                log(CRITICAL, f"Causa:\n{error}")
                break
            finally:
                i += 1
                if i == len(ropa):
                    self._driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight)"
                    )
                    sleep(7)
                    ropa = self._driver.find_elements(
                        By.XPATH,
                        '//*[@class="xt7dq6l xl1xv1r x6ikm8r x10wlt62 xh8yej3"]',
                    )
                sleep(2)
                del self._driver.requests
                log(
                    INFO,
                    "-------------------------------------------------------------------",
                )
        self._tiempo.cantidad_real = i - e
        self._tiempo.num_error = e
        log(INFO, f"Se halló {e} errores")
        log(INFO, "Fin de la extraccion")

    def guardar_datos(
        self,
        dataset,
        filetype="Data",
        folder="Data//datos_obtenidos",
        filename="fb_data",
    ):
        """
        Guarda los datos o errores obtenidos durante la ejecución del scraper

        Parameters
        ----------
        dataset: dict
            Conjunto de datos extraídos por el scraper
        filetype: str
            Indica si la información proviene de los datos o de los errores
        folder: str
            Ruta del archivo
        filename: str
            Nombre del archivo

        Returns
        -------
        None
        """
        log(INFO, f"Guardando {filetype}")
        df_fb_mkp_ropa = DataFrame(dataset)

        if len(df_fb_mkp_ropa) == 0:
            log(
                INFO,
                f"El archivo de tipo {filetype} no se va a guardar por no tener información",
            )
            return

        if filetype == "Data":
            df_fb_mkp_ropa.drop(len(df_fb_mkp_ropa) - 1, axis=0, inplace=True)
            cantidad = len(df_fb_mkp_ropa)
            self._tiempo.cantidad = cantidad
        elif filetype == "Error":
            cantidad = self._tiempo.num_error
        else:
            log(
                INFO,
                f"El archivo de tipo {filetype} no está admitido. Solo se aceptan los valores Data y Error",
            )
            return

        datetime_obj = datetime.strptime(self._tiempo.fecha, "%d/%m/%Y")
        filepath = path.join(folder, datetime_obj.strftime("%d-%m-%Y"))
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
        df_fb_mkp_ropa.to_excel(path.join(filepath, filename), index=False)
        log(INFO, f"{filetype} Guardados Correctamente")

    def guardar_tiempos(self, filename, sheet_name):
        """
        Guarda la información del tiempo de ejecución del scraper

        Parameters
        ----------
        filename: str
            Nombre del archivo
        sheet_name: str
            Nombre de la hoja de cálculo

        Returns
        -------
        None
        """
        log(INFO, "Guardando tiempos")
        self._tiempo.set_param_final()
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
            worksheet.append(list(self._tiempo.__dict__.keys())[1:])
        worksheet.append(list(self._tiempo.__dict__.values())[1:])
        tiempos.save(filename)
        tiempos.close()
        log(INFO, "Tiempos Guardados Correctamente")


def config_log(log_folder, log_filename, log_file_mode, log_file_encoding, fecha_actual):
    """
    Función que configura los logs para rastrear al programa
        Parameter:
                log_folder (str): Carpeta donde se va a generar el archivo log
                log_filename (str): Nombre del archivo log a ser generado
                fecha_actual (datetime): Fecha actual de la creación del archivo log
        Returns:
                None
    """
    seleniumLogger.setLevel(ERROR)
    urllibLogger.setLevel(ERROR)
    logger = getLogger("seleniumwire")
    logger.setLevel(ERROR)
    log_path = path.join(log_folder, fecha_actual.strftime("%d-%m-%Y"))
    log_filename = log_filename + "_" + fecha_actual.strftime("%d%m%Y") + ".log"
    if not path.exists(log_path):
        makedirs(log_path)
    basicConfig(
        format="%(asctime)s %(message)s",
        level=INFO,
        handlers=[StreamHandler(), FileHandler(path.join(log_path, log_filename), log_file_mode, log_file_encoding)],
    )


def validar_parametros(parametros):
    """
    Función que valida si los parámetros a usar están definidos
         Parameter:
                 parametros (list): Lista de parámetros

        Returns:
               None
    """
    for parametro in parametros:
        if not parametro:
            log(ERROR, "Parámetros incorrectos")
            return False
    log(INFO, "Parámetros válidos")
    return True


def main():
    # Formato para el debugger
    fecha_actual = datetime.now().date() - timedelta(days=1)
    config_log("Log", "fb_ropa_log", "w", "utf-8", fecha_actual)
    log(INFO, "Configurando Formato Básico del Debugger")

    # Cargar variables de entorno
    log(INFO, "Cargando Variables de entorno")
    load_dotenv()

    # Url de la categoría a scrapear
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
    scraper = ScraperFb(fecha_actual)

    # Iniciar sesión
    scraper.iniciar_sesion()

    # Extracción de datos
    scraper.mapear_datos(url_ropa)

    # Guardando la data extraída por el scraper
    scraper.guardar_datos(scraper.data.dataset, "Data", data_folder, data_filename)

    # Guardando los errores extraídos por el scraper
    scraper.guardar_datos(
        scraper.errores.errores, "Error", error_folder, error_filename
    )

    # Guardando los tiempos durante la ejecución del scraper
    scraper.guardar_tiempos(filename_tiempos, sheet_tiempos)
    log(INFO, "Programa finalizado")


if __name__ == "__main__":
    main()
