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
@descargas_api.route("/PRUEBA_3")
def PRUEBA_3():

    response = {}
    response["code"] = 5000
    response["description"] = "Listo Prueba 3"
    response["status"] = False

    # Defino la ruta de descarga (Sin incluir '//192.168.20.87').
    route = '/BI_Archivos/GOOGLE/Bancolombia_Cast/Base_marcada/Base Calculada/Bancolombia_Cast_Base_Calculada.csv'
    # Defino la consulta SQL a ejecutar en BigQuery.
    query = 'SELECT * FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_BM`'# LIMIT 15'
    # Defino los títulos de los campos resultantes de la ejecución del query.
    header = ["IDKEY","FECHA","CONSECUTIVO_DOCUMENTO_DEUDOR","VALOR_CUOTA","NIT","NOMBRES","NUMERO_DOCUMENTO","TIPO_PRODUCTO","FECHA_ACTUALIZACION_PRIORIZACION","FECHA_PAGO_CUOTA","NOMBRE_DE_PRODUCTO","FECHA_DE_PERFECCIONAMIENTO","FECHA_VENCIMIENTO_DEF","NUMERO_CUOTAS","CUOTAS_EN_MORA","DIA_DE_VENCIMIENTO_DE_CUOTA","VALOR_OBLIGACION","VALOR_VENCIDO","SALDO_ACTIVO","SALDO_ORDEN","REGIONAL","CIUDAD","GRABADOR","CODIGO_AGENTE","NOMBRE_ASESOR","CODIGO_ABOGADO","NOMBRE_ABOGADO","FECHA_ULTIMA_GESTION_PREJURIDICA","ULTIMO_CODIGO_DE_GESTION_PARALELO","ULTIMO_CODIGO_DE_GESTION_PREJURIDICO","DESCRIPCION_SUBSECTOR","DESCRIPCION_CODIGO_SEGMENTO","DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO","DESCRIPCION_SUBSEGMENTO","DESCRIPCION_SECTOR","DESCRIPCION_CODIGO_CIIU","CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO","DESC_CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO","FECHA_ULTIMA_GESTION_JURIDICA","ULTIMA_FECHA_DE_ACTUACION_JURIDICA","ULTIMA_FECHA_PAGO","EJEC_ULTIMO_CODIGO_DE_GESTION_JURIDICO","DESC_ULTIMO_CODIGO_DE_GESTION_JURIDICO","CANT_OBLIG","CLUSTER_PERSONA","DIAS_MORA","PAIS_RESIDENCIA","TIPO_DE_CARTERA","CALIFICACION","RADICACION","ESTADO_DE_LA_OBLIGACION","FONDO_NACIONAL_GARANTIAS","REGION","SEGMENTO","CODIGO_SEGMENTO","FECHA_IMPORTACION","NIVEL_DE_RIESGO","FECHA_ULTIMA_FACTURACION","SUBSEGMENTO","TITULAR_UNIVERSAL","NEGOCIO_TITUTULARIZADO","SECTOR_ECONOMICO","PROFESION","CAUSAL","OCUPACION","CUADRANTE","FECHA_TRASLADO_PARA_COBRO","DESC_CODIGO_DE_GESTION_VISITA","FECHA_GRABACION_VISITA","ENDEUDAMIENTO","CALIFICACION_REAL","FECHA_PROMESA","RED","ESTADO_NEGOCIACION","TIPO_CLIENTE_SUFI","CLASE","FRANQUICIA","SALDO_CAPITAL_PESOS","SALDO_INTERESES_PESOS","PROBABILIDAD_DE_PROPENSION_DE_PAGO","PRIORIZACION_FINAL","PRIORIZACION_POR_CLIENTE","GRUPO_DE_PRIORIZACION","FECHA_PROMESA_V2","FECHA_PROMESA_AJUSTADA","DIAS_DESDE_TRASLADO","DIAS_SIN_COMPROMISO","DIAS_SIN_PAGO","DIAS_SIN_RPC","FRANJA_MORA","RANGO_PROP_TRASLADO","RANGO_PROP_PAGO","RANGO_PROP_CONTACTO","RANGO_PROP_ACUERDO","DESFASE","EQUIPO","MEJOR_DIA","MEJOR_HORA","LOTE","VUELTAS_REQUERIDAS","VUELTAS_REALES","GRABADOR_AJUSTADO"]
    
    b = descargar_csv(route, query, header) # Hago el llamado a la función de descarga.

    return jsonify(response), response["code"]
    