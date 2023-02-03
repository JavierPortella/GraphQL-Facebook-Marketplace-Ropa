# Importación de librerías
from logging import (
    basicConfig,
    ERROR,
    INFO,
    log,
    StreamHandler,
)
from os import path
from pandas import read_csv


def read_dataset(filename, sep=";", encoding="utf-8", decimal="."):
    """
    Función que lee un archivo y devuelve un DataFrame (pandas.core.frame.DataFrame)
        Parameter:
                filename (str): Ruta del archivo
                sep (str): Separador de las columnas
                encoding (str): Codificación en la que fue guardado el archivo
                decimal (str): Separador decimal
        Returns:
                pandas.core.frame.DataFrame
    """
    return read_csv(filename, sep=sep, encoding=encoding, decimal=decimal)


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


def drop_rows(df_data, index):
    """
    Función que elimina las filas de un DataFrame (pandas.core.frame.DataFrame) dado los índices
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                index (pandas.core.indexes.base.Index): Índice de las filas a eliminar
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data.drop(index)


def replace_values(df_data, old_value, new_value=None, regex=False):
    """
    Función que reemplaza la información contenida dentro del DataFrame (pandas.core.frame.DataFrame)
    por otro valor dado
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                old_value (str): Valor(es) a ser reemplazado(s)
                new_value (str): Valor a reemplazar
                regex (bool): Indica si en el reemplazo se hace uso de expresiones regulares
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data.replace(old_value, new_value, regex=regex)


def replace_null(df_data, new_value):
    """
    Función que reemplaza los valores nulos existentes en el DataFrame (pandas.core.frame.DataFrame)
    por otro valor dado
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                new_value (str): Valor a reemplazar
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data.fillna(new_value)


def change_datatype(df_data, datatype="int64"):
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


def remove_punctuation(df_data, columns, punctuation="."):
    """
    Función que elimina el separador de miles contenidas en las columnas dadas de un DataFrame
    (pandas.core.frame.DataFrame)
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                columns (list): Lista de columnas que contienen al separador de miles
                punctuation (str): Separador de miles a ser removido
        Returns:
                pandas.core.frame.DataFrame
    """
    for column in columns:
        index = df_data[
            df_data[column].str.contains(r"[{0}]+".format(punctuation)) == True
        ].index
        num_values = df_data.loc[index, column].str.split(punctuation).str
        df_data.loc[index, column] = num_values[0] + num_values[1].str.ljust(3, "0")

    return df_data


def fix_price(df_data, columns):
    """
    Función que corrige la información de algunos precios erróneos de los registros contenidos en el DataFrame
    (pandas.core.frame.DataFrame)
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                columns (list): Lista de columnas que contienen al separador de miles
                punctuation (str): Separador de miles a ser removido
        Returns:
                pandas.core.frame.DataFrame
    """
    for column in columns:
        index = df_data[df_data[column] < 10].index
        df_data.loc[index, column] = df_data.loc[index, column] * 1000
    return df_data


def get_final_price(df_data, price_name, tax_name):
    """
    Función que recalcula el precio final de todos los registros contenidos en el DataFrame
    (pandas.core.frame.DataFrame)
        Parameter:
                df_data (pandas.core.frame.DataFrame): DataFrame
                columns (list): Lista de columnas que contienen al separador de miles
                punctuation (str): Separador de miles a ser removido
        Returns:
                pandas.core.frame.DataFrame
    """
    return df_data[price_name] + df_data[tax_name]


def process_data_general(data):
    """
    Función que procesa toda la data contenida en el DataFrame (pandas.core.frame.DataFrame)
    sin importar de dónde proviene la data
        Parameter:
                data (pandas.core.frame.DataFrame): DataFrame
        Returns:
                pandas.core.frame.DataFrame
    """
    log(INFO, "Reemplazando valores nulos y sus variantes por n.d.")
    null_values = ["undefined", "null", "-"]
    data = replace_null(data, "n.d.")
    data = replace_values(data, null_values, "n.d.")
    return data


def process_data_despegar(data):
    """
    Función que procesa toda la data contenida en el DataFrame (pandas.core.frame.DataFrame)
    que provenga de la página de despegar.com
        Parameter:
                data (pandas.core.frame.DataFrame): DataFrame
        Returns:
                pandas.core.frame.DataFrame
    """
    # Columnas a trabajar
    stopover_cols = ["Escalas"]
    price_col = "Precio"
    price_cols = [
        price_col,
        "Impuesto",
        "Precio Final",
        "Costo cancelacion 1",
        "Costo cambios 1",
        "Costo cancelacion 2",
        "Costo cambios 2",
    ]
    bool_cols = [
        "Mochila o cartera",
        "Equipaje de mano",
        "Equipaje para documentar",
        "Cancelacion 1",
        "Cambios 1",
        "Cancelacion 2",
        "Cambios 2",
    ]
    log(INFO, "Eliminando filas que no contengan información del precio")
    data = drop_rows(data, data[data[price_col] == "n.d."].index)
    data.reset_index(drop=True, inplace=True)

    log(INFO, "Reemplazar escala 3 a 2")
    data[stopover_cols] = replace_values(data[stopover_cols], 3, 2)

    log(INFO, "Reemplazar puntos de las columnas relacionadas con el precio")
    data = remove_punctuation(data, [price_col])

    log(INFO, "Cambiar tipo de dato para la columna Precio")
    data[price_cols[:2]] = change_datatype(data[price_cols[:2]])
    log(INFO, "Corrigiendo precios")
    data = fix_price(data, [price_col])
    log(INFO, "Calculando los nuevos precios finales")
    data[price_cols[2]] = get_final_price(data, *data[price_cols[:2]])

    log(INFO, "Cambiando tipo de dato de las columnas")
    data[bool_cols] = change_datatype(data[bool_cols], "str")
    data[bool_cols] = replace_values(data[bool_cols], "True", "VERDADERO")
    data[bool_cols] = replace_values(data[bool_cols], "False", "FALSO")

    return data


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
        filename = r"despegar_full_destinos_2023-01-27_2023-02_2023-01-29_8.69_.csv"
        if not path.isfile(filename):
            log(ERROR, "El archivo especificado no existe o se encuentra en otra ruta")
            return

        DESPEGAR = "1"
        BOOKING = "2"
        PEDIDOS_YA = "3"
        tipo_info = input(
        """
        PREPROCESSING

        De qué página desea limpiar la data:
        1. Despegar (Digite 1)
        2. Booking (Digite 2)
        3. Pedidos Ya (Digite 3)
        Ingrese una opción: 
        """
        )
        if tipo_info not in [DESPEGAR, BOOKING, PEDIDOS_YA]:
            log(
                ERROR,
                "Se ha digitado un valor que no corresponde. Se admiten solo los valores {DESPEGAR}, {BOOKING} y {PEDIDOS_YA}",
            )
            return
        filenameFixed = get_new_filename(filename)

        log(INFO, "Lectura del archivo csv")
        data = read_dataset(filename, decimal=",")
        log(INFO, "Archivo leído satisfactoriamente")

        if len(data) <= 0:
            log(
                ERROR,
                "La data no tiene información para ser procesada",
            )
            return

        log(INFO, "Procesando la data")
        data = process_data_general(data)
        log(INFO, "Data procesada satisfactoriamente")

        log(INFO, "Procesando la data a profundidad")
        if tipo_info == DESPEGAR:
            data = process_data_despegar(data)
        elif tipo_info == BOOKING:
            pass
        elif tipo_info == PEDIDOS_YA:
            pass
        else:
            return
        log(INFO, "Data procesada a profundidad con éxito")

        log(INFO, "Guardando la data limipia en un nuevo archivo csv")
        data.to_csv(filenameFixed, sep=";", index=False, encoding="utf-8-sig")
        log(INFO, "Datos guardados satisfactoriamente")
        log(INFO, "Programa ejecutado satisfactoriamente")

    except Exception as error:
        log(ERROR, f"Error: {error}")
        log(INFO, "Programa ejecutado con fallos")


if __name__ == "__main__":
    main()
