from os import makedirs, path
from pandas import read_excel, read_csv
from re import compile
from unidecode import unidecode

def eliminar_caracteres(df_data, na_action="ignore"):
    return df_data.applymap(lambda x: unidecode(str(x)), na_action=na_action)

def reemplazar_nulos(df_data, value_to_replace="n.d."):
    return df_data.fillna(value_to_replace)

def reemplazar_valores(df_data, pattern, value=None, regex=False):
    clase_pattern = type(pattern) 
    if clase_pattern == str:
        if regex:
            pattern = compile(pattern)
        return df_data.replace(to_replace=pattern, value=value,regex=regex)
    else:
        return df_data.replace(to_replace=pattern)
    
def remover_publicaciones(df_data, column_1="descripcion", column_2="titulo_marketplace", pattern="#adi"):
    return df_data.drop(df_data[df_data[column_1].isna() & df_data[column_2].str.lower().str.contains(pattern)].index)

def remover_duplicados(df_data, columns):
    return df_data.drop(df_data[df_data[columns].duplicated()].index)

def remover_columnas_innecesarias(df_data, columns):
    return df_data.drop(columns, axis = 1)

def cambiar_tipo_dato(df_data, datatype="str"):
    return df_data.astype(datatype)

def procesar_data(df_data):    
    cols_str = ['titulo_marketplace', 'descripcion', 'locacion']
    cols_bool = ["disponible", "vendido"]
    df_data[cols_str] = eliminar_caracteres(df_data[cols_str])
    df_data[cols_str] = reemplazar_valores(df_data[cols_str], r"\r?\n", " ", regex=True)
    df_data[cols_str] = reemplazar_valores(df_data[cols_str], "[,.]{2,}(?![\sa-zA-Zá-úÁ-Ú])", "", regex=True)
    df_data[cols_str] = reemplazar_valores(df_data[cols_str], "[,.](?=[a-zA-Zá-úÁ-Ú])", " ", regex=True)
    df_data[cols_str] = reemplazar_valores(df_data[cols_str], "undefined", "n.d.")
    df_data[cols_str] = reemplazar_valores(df_data[cols_str], "-", "n.d.")
    df_data[cols_str] = reemplazar_valores(df_data[cols_str], "null", "n.d.")
    df_data = remover_publicaciones(df_data)
    df_data = reemplazar_nulos(df_data, "n.d.")
    df_data[cols_bool] = cambiar_tipo_dato(df_data[cols_bool])
    df_data = remover_duplicados(df_data, ["id_vendedor", "titulo_marketplace"])
    return df_data

def read_data(data_filename, sep=";", encoding="utf-8"):
    _ , ext = data_filename.split(".")
    df_ropa = None
    if ext == "csv":
        df_ropa = read_csv(data_filename, sep=sep, encoding=encoding)
    elif ext == "xlsx":
        df_ropa = read_excel(data_filename)
    return df_ropa

def main():
    data_filename = "archivo.xlsx"
    data_folder = "datos_depurados"
    data_path = path.join(data_folder, data_filename)
    df_ropa = read_data(data_filename)
    if not df_ropa:
        return
    df_ropa = procesar_data(df_ropa)
    if not path.exists(data_folder):
        makedirs(data_folder)
    df_ropa.to_excel(data_path)

if __name__ == "__main__":
    main()
    
    


""" import pandas as pd

def replace_values(dataframe, old_value, new_value):
    return dataframe.replace(old_value, new_value)

def replace_null(dataframe, new_value):
    return dataframe.fillna(new_value)


# Variables
filename = 'fb_vehiculos_27122022_2138.csv'
sufix = 'corregido'
filenameFixed = filename.split('.')[0] +'-' + sufix + '.'+ filename.split('.')[1]

# Lectura del archivo csv
data = pd.read_csv(filename, sep=';', encoding='utf-8')

# Cambio de codificación a latin-1 por columnas específicas (solo las de texto)
cols = ['titulo_marketplace', 'descripcion', 'vendedor', 'locacion']
data[cols] = data[cols].apply(lambda x: x.str.encode('latin-1', 'ignore').str.decode('latin-1'))   

#Reemplazando valores nulos
data = replace_null(data, "n.d.")
#Reemplazando el valor undefined por n.d.
data = replace_values(data, "undefined", "n.d.")
#Reemplazando el valor null por n.d.
data = replace_values(data, "null", "n.d.")
#Reemplazando el caracter - por n.d.
data = replace_values(data, "-", "n.d.")

# Lectura del archivo csv corregido
data.to_csv(filenameFixed, sep=';', index=False, encoding='latin-1') """


# def RemoveSpecialCharacter(data):
#     for row in data:
#         for specialCharacter in specialCharacters:
#             row = row.replace(specialCharacter, 'á')

# # Eliminando filas que contienen vacíos (solo en columnas específicas)
# print(data)





# # Sobreescribiendo archivo csv
# data.to_csv(filenameFixed, sep = ';', index=False)