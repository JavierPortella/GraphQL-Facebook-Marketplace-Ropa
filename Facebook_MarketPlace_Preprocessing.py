from logging import (
    basicConfig,
    ERROR,
    INFO,
    log,
    StreamHandler,
)
from os import path
from pandas import read_excel, read_csv, DataFrame
from re import compile
from unidecode import unidecode


def read_data(data_filename, sep=";", encoding="utf-8"):
    """
    Función que lee un archivo y devuelve un DataFrame (pandas.core.frame.DataFrame)
        Parameter:
                filename (str): Ruta del archivo
                sep (str): Separador de las columnas
                encoding (str): Codificación en la que fue guardado el archivo
        Returns:
                pandas.core.frame.DataFrame
    """
    index = data_filename.rfind(".")
    ext = data_filename[index:]
    if ext == "csv":
        return read_csv(data_filename, sep=sep, encoding=encoding)
    elif ext == "xlsx":
        return read_excel(data_filename)
    else:
        return DataFrame()


def get_new_filename(filename, sufix="depurado"):
    """
    Función que retorna el nuevo nombre del archivo
        Parameter:
                filename (str): Ruta del archivo
                sufix (str): Identificador adicional que se agrega al final del nombre del archivo
        Returns:
                str
    """
    index = filename.rfind(".")
    return filename[:index] + "_" + sufix + filename[index:]


def eliminar_caracteres(df_data, na_action="ignore"):
    """
    Función que elimina caracteres no unicode de un DataFrame (pandas.core.frame.DataFrame)
        Parameters:
                df_data (pandas.core.frame.DataFrame): DataFrame
                na_action (str): Acción a realizar cuando se trabaja con valores faltantes o nulos
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data.applymap(lambda x: unidecode(str(x)), na_action=na_action)


def reemplazar_nulos(df_data, value_to_replace="n.d."):
    """
    Función que reemplaza los valores nulos existentes en el DataFrame (pandas.core.frame.DataFrame)
    por otro valor dado
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                value_to_replace (str): Valor a reemplazar
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data.fillna(value_to_replace)


def reemplazar_valores(df_data, pattern, new_value=None, regex=False):
    """
    Función que reemplaza la información contenida dentro del DataFrame (pandas.core.frame.DataFrame)
    por otro valor dado
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                pattern (str): Valor(es) a ser reemplazado(s) o un diccionario de valores
                new_value (str): Valor a reemplazar
                regex (bool): Indica si en el reemplazo se hace uso de expresiones regulares
        Returns:
                pandas.core.frame.DataFrame
    """
    patter_type = type(pattern)
    if patter_type == str or patter_type == list:
        if regex:
            pattern = compile(pattern)
        return df_data.replace(to_replace=pattern, value=new_value, regex=regex)
    else:
        return df_data.replace(to_replace=pattern)


def remover_publicaciones(
    df_data, column_1="descripcion", column_2="titulo_marketplace", pattern="#adi"
):
    """
    Función que elimina publicaciones que no corresponden a la categoría deseada
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                column_1 (str): Columna que hace referencia a la descripción de la publicación
                column_2 (str): Columna que hace referencia al título de la publicación
                pattern (str): Patrón que se va a utilizar para detectar las publicaciones a ser eliminadas
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data.drop(
        df_data[
            df_data[column_1].isna()
            & df_data[column_2].str.lower().str.contains(pattern)
        ].index
    )


def remover_duplicados(df_data, columns):
    """
    Función que elimina publicaciones duplicadas
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                columns (list): Lista de columnas que identifican de manera única a un registro
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data.drop(df_data[df_data[columns].duplicated()].index)


def cambiar_tipo_dato(df_data, datatype="str"):
    """
    Función que cambia el tipo de dato de de las columnas de un DataFrame (pandas.core.frame.DataFrame)
    por otro tipo de dato dado
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                datatype (str): Nombre del tipo de dato que se usa para la conversión
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data.astype(datatype)


def procesar_data(df_data):
    """
    Función que procesa toda la data contenida en el DataFrame (pandas.core.frame.DataFrame)
    que provenga de la página de facebook marketplace
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
        Returns:
                pandas.core.frame.DataFrame
    """
    # Columnas a trabajar
    cols_str = ["titulo_marketplace", "descripcion", "locacion"]
    cols_bool = ["disponible", "vendido"]
    log(INFO, "Eliminando caracteres no unicode")
    df_data[cols_str] = eliminar_caracteres(df_data[cols_str])
    log(INFO, "Eliminando caracteres de salto de línea")
    df_data[cols_str] = reemplazar_valores(df_data[cols_str], r"\r?\n", " ", regex=True)
    log(INFO, "Eliminando caracteres especiales repetidos")
    df_data[cols_str] = reemplazar_valores(
        df_data[cols_str], "[,.]{2,}(?![\sa-zA-Zá-úÁ-Ú])", "", regex=True
    )
    df_data[cols_str] = reemplazar_valores(
        df_data[cols_str], "[,.](?=[a-zA-Zá-úÁ-Ú])", " ", regex=True
    )

    log(INFO, "Eliminando publicaciones falsas")
    df_data = remover_publicaciones(df_data)
    df_data.reset_index(drop=True, inplace=True)

    log(INFO, "Reemplazando valores nulos y sus variantes por n.d.")
    null_values = ["undefined", "null", "-"]
    df_data = reemplazar_valores(df_data, null_values, "n.d.")
    df_data = reemplazar_nulos(df_data, "n.d.")

    log(INFO, "Cambiando tipo de dato de las columnas")
    df_data[cols_bool] = cambiar_tipo_dato(df_data[cols_bool])

    log(INFO, "Remover columnas duplicadas")
    df_data = remover_duplicados(df_data, ["id_vendedor", "titulo_marketplace"])
    return df_data


def config_log():
    """
    Función que configura los logs para rastrear al programa
        Parameter:
                None
        Returns:
                None
    """
    basicConfig(
        format="%(asctime)s %(message)s",
        level=INFO,
        handlers=[StreamHandler()],
    )


def main():
    try:
        # Formato para el debugger
        config_log()
        log(INFO, "Configurando Formato Básico del Debugger")

        # Variables
        log(INFO, "Configurando Variables de entorno")
        data_filename = r"archivo.xlsx"
        if not path.isfile(data_filename):
            log(ERROR, "El archivo especificado no existe o se encuentra en otra ruta")
            return
        filenameFixed = get_new_filename(data_filename)

        log(INFO, "Lectura del archivo")
        df_ropa = read_data(data_filename)
        log(INFO, "Archivo leído satisfactoriamente")

        if len(df_ropa) <= 0:
            log(
                ERROR,
                "La data no tiene información para ser procesada",
            )
            return

        log(INFO, "Procesando la data")
        df_ropa = procesar_data(df_ropa)
        log(INFO, "Data procesada satisfactoriamente")

        log(INFO, "Guardando la data limipia en un nuevo archivo csv")
        df_ropa.to_csv(filenameFixed, sep=";", index=False, encoding="utf-8-sig")
        log(INFO, "Datos guardados satisfactoriamente")
        log(INFO, "Programa ejecutado satisfactoriamente")

    except Exception as error:
        log(ERROR, f"Error: {error}")
        log(INFO, "Programa ejecutado con fallos")


if __name__ == "__main__":
    main()
