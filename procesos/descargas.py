# encoding=utf8
from flask import Blueprint
from flask import jsonify
from google.cloud import bigquery
import pandas as pd
from pandas import DataFrame
import socket

descargas_api = Blueprint('descargas_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

# ---------------------------------------------------------------------------------------
# Procedimiento que permite descargar informacion a un archivo csv. Fruto de la ejecucion de una consulta SQL desde BigQuery.
# La funcion recibe como parametros:
#   myRoute: Ruta en donde se va a descargar el archivo csv resultante.
#   myQuery: String que define la consulta SQL a ejecutar en BigQuery.
#   myHeader: Títulos de los campos resultantes de la ejecución del query.
# En el procedimiento llamado PRUEBA_3 se ejemplifica la manera practica de invocar la funcion 'descargar_csv'.
@descargas_api.route("/descargar_csv")
def descargar_csv(myRoute, myQuery, myHeader):

    response = {}
    response["code"] = 5000
    response["description"] = "Descarga Exitosa"
    response["status"] = False

    query = myQuery
    client = bigquery.Client()
    df = client.query(query).to_dataframe() # Permite utilizar el resultado del query desde pandas, mediante DataFrame.

    cabecera = myHeader
    df.colums = cabecera    # Agrega los titulos de los campos al DataFrame.

    mi_ruta_descarga = fileserver_baseroute + myRoute
    df.to_csv(mi_ruta_descarga, encoding='utf-8')   # Ejecuta la descarga del arvhivo csv, especificando el tipo de codificacion 'utf-8'.

    return jsonify(response), response["code"]

# ---------------------------------------------------------------------------------------
# A continuacion un ejemplo, el cual muestra la manera correcta de invocar la funcion 'descargar_csv':
# @descargas_api.route("/PRUEBA_3")
# def PRUEBA_3():

#     response = {}
#     response["code"] = 5000
#     response["description"] = "Listo Prueba 3"
#     response["status"] = False

#     # Defino la ruta de descarga (Sin incluir '//192.168.20.87').
#     route = '/BI_Archivos/GOOGLE/Metlife/Base/Descargas/base.csv'
#     # Defino la consulta SQL a ejecutar en BigQuery.
#     query = 'SELECT * FROM `contento-bi.MetLife.Base`'
#     # Defino los títulos de los campos resultantes de la ejecución del query.
#     header = ["IDKEY","FECHA","REFERENCIA","TIPO_IDENTIFICACION","CEDULA_NIT","NOMBRE_DEL_CLIENTE","SEXO","FECHA_DE_NACIMIENTO","EDAD","CODIGO_PRODUCTO","CICLO","DIRECCION_CORRESPONDENCIA","CODIGO_CIUDAD_DE_CORRESPON","TEL_NRO_CORRESPONDENCIA","CELULAR","NOMBRE_EMPRESA","DIRNRO_EMPRESA","TELNRO_OFICINA","CODIGO_CIUDAD_OFICINA","CARGO_EMPRESA","FECHA_DE_INGRESO","CODIGO_ESTADO_CIVIL","CEDULA_CONYUGE","PERSONAS_A_CARGO","CODIGO_PROFESION","SALARIO_MENSUAL","TOTAL_EGRESOS","OCUPACION","CUPO","SALDO_DISPONIBLE","MERCADO","FECHA_DE_APROBACION_TCO","FECHA_DESBLOQUEO_TCO","MAIL","TIPO_EXTRACTO","SEGMENTO","CATEGORIA","ANO_MES_ENVIO","COD_GESTION","TIPIFICACION","TEL_MARCADO","PROSPECT_NUM","RANGO_CUPO_VENTA","PLAN_MAX_AP"]
          
#     b = descargar_csv(route, query, header) # Hago el llamado a la función de descarga.

#     return jsonify(response), response["code"]
    