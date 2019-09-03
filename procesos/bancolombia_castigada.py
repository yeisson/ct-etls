# encoding=utf8
from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.bancolombia.bancolombia_castigada_seguimiento_beam as bancolombia_castigada_seguimiento_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_pagos_beam as bancolombia_castigada_pagos_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_prejuridico_beam as bancolombia_castigada_prejuridico_beam
import procesos.descargas as descargas
import os
import socket
import time
import pandas as pd
import google.auth
from pandas import DataFrame
# import sqlalchemy
# import BigQueryHelper

# from google.cloud import bigquery_storage_v1beta1

bancolombia_castigada_api = Blueprint('bancolombia_castigada_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@bancolombia_castigada_api.route("/archivos_seguimiento_castigada")
def archivos_Seguimiento_castigada():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[29:37]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_seguimiento_beam.run('gs://ct-bancolombia_castigada/info-seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Seguimiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                time.sleep(240) # Le da tiempo al Storage, para que lleve la informacion a la tabla seguimiento en BigQuery.

                # Inicia proceso de calculo para Best Time.
                # ----------------------------------------------------------------------------------------------------------------
                # Extraccion de Contactos con Titular:
                deleteQuery_2 = "INSERT INTO `contento-bi.bancolombia_castigada.contactos_titular` (SELECT A.NIT, A.FECHA_GESTION FROM `contento-bi.bancolombia_castigada.QRY_EXTRACT_RPC` A LEFT JOIN `contento-bi.bancolombia_castigada.contactos_titular` B ON A.NIT = B.NIT AND A.FECHA_GESTION = B.FECHA_GESTION WHERE B.FECHA_GESTION IS NULL)"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                time.sleep(60)

                # # Calculo de Mejor dia (UPDATE):
                deleteQuery_3 = "UPDATE `contento-bi.bancolombia_castigada.best_time` BT SET BT.MEJOR_DIA = QRY.MI_DIA FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_BEST_DAY_UP` QRY WHERE BT.NIT = QRY.NIT"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(deleteQuery_3)
                query_job_3.result()

                time.sleep(60)

                # # Calculo de Mejor dia (INSERT):
                deleteQuery_4 = "INSERT INTO `contento-bi.bancolombia_castigada.best_time` (NIT, MEJOR_DIA) (SELECT NIT, MI_DIA FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_BEST_DAY_IN`)"
                client_4 = bigquery.Client()
                query_job_4 = client_4.query(deleteQuery_4)
                query_job_4.result()

                time.sleep(60)

                # # Calculo de Mejor hora (UPDATE):
                deleteQuery_5 = "UPDATE `contento-bi.bancolombia_castigada.best_time` BT SET BT.MEJOR_HORA = QRY.MI_HORA FROM (SELECT NIT, MI_HORA FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_BEST_HOUR_UP`) QRY WHERE BT.NIT = QRY.NIT"
                client_5 = bigquery.Client()
                query_job_5 = client_5.query(deleteQuery_5)
                query_job_5.result()
                # ----------------------------------------------------------------------------------------------------------------
                # Finaliza proceso de calculo para Best Time.


    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


@bancolombia_castigada_api.route("/archivos_pagos")
def archivos_Pagos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Pagos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[29:37]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-pagos/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.pagos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_pagos_beam.run('gs://ct-bancolombia_castigada/info-pagos/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Pagos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

@bancolombia_castigada_api.route("/archivos_prejuridico")
def archivos_Prejuridico_castigada():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Prejuridico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[29:37]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-prejuridico/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.prejuridico` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery) 

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_prejuridico_beam.run('gs://ct-bancolombia_castigada/info-prejuridico/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Prejuridico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                time.sleep(240) # Le da tiempo al Storage, para que lleve la informacion a la tabla prejuridico en BigQuery.

                # Inicia proceso de calculo para Fecha de Promesa Ajustada.
                # ----------------------------------------------------------------------------------------------------------------
                # Busqueda de Fecha Promesa Ajustada (UPDATE):
                deleteQuery_2 = "UPDATE `contento-bi.bancolombia_castigada.ajuste_promesas` A SET A.MAX_FECHA_PROMESA_AJUSTADA = CAST(B.FECHA_PROMESA AS DATE) FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_MAX_DATE_HIT_UP` B WHERE A.NIT = B.NIT"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                time.sleep(60)

                # Busqueda de Fecha Promesa Ajustada (INSERT):
                deleteQuery_3 = "INSERT INTO `contento-bi.bancolombia_castigada.ajuste_promesas` (NIT, MAX_FECHA_PROMESA_AJUSTADA)	(SELECT NIT, CAST(FECHA_PROMESA AS DATE) FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_MAX_DATE_HIT_IN`)"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(deleteQuery_3)
                query_job_3.result()
                # ----------------------------------------------------------------------------------------------------------------

                time.sleep(30)

                # Query de ejecución de los campos calculados:
                # Defino la ruta de descarga.
                route = '/BI_Archivos/GOOGLE/Bancolombia_Cast/Base_marcada/Base Calculada/Bancolombia_Cast_Base_Calculada.csv'
                # Defino la consulta SQL a ejecutar en BigQuery.
                query = 'SELECT * FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_BM`'
                # Defino los títulos de los campos resultantes de la ejecución del query.
                header = ["IDKEY","FECHA","CONSECUTIVO_DOCUMENTO_DEUDOR","VALOR_CUOTA","NIT","NOMBRES","NUMERO_DOCUMENTO","TIPO_PRODUCTO","FECHA_ACTUALIZACION_PRIORIZACION","FECHA_PAGO_CUOTA","NOMBRE_DE_PRODUCTO","FECHA_DE_PERFECCIONAMIENTO","FECHA_VENCIMIENTO_DEF","NUMERO_CUOTAS","CUOTAS_EN_MORA","DIA_DE_VENCIMIENTO_DE_CUOTA","VALOR_OBLIGACION","VALOR_VENCIDO","SALDO_ACTIVO","SALDO_ORDEN","REGIONAL","CIUDAD","GRABADOR","CODIGO_AGENTE","NOMBRE_ASESOR","CODIGO_ABOGADO","NOMBRE_ABOGADO","FECHA_ULTIMA_GESTION_PREJURIDICA","ULTIMO_CODIGO_DE_GESTION_PARALELO","ULTIMO_CODIGO_DE_GESTION_PREJURIDICO","DESCRIPCION_SUBSECTOR","DESCRIPCION_CODIGO_SEGMENTO","DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO","DESCRIPCION_SUBSEGMENTO","DESCRIPCION_SECTOR","DESCRIPCION_CODIGO_CIIU","CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO","DESC_CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO","FECHA_ULTIMA_GESTION_JURIDICA","ULTIMA_FECHA_DE_ACTUACION_JURIDICA","ULTIMA_FECHA_PAGO","EJEC_ULTIMO_CODIGO_DE_GESTION_JURIDICO","DESC_ULTIMO_CODIGO_DE_GESTION_JURIDICO","CANT_OBLIG","CLUSTER_PERSONA","DIAS_MORA","PAIS_RESIDENCIA","TIPO_DE_CARTERA","CALIFICACION","RADICACION","ESTADO_DE_LA_OBLIGACION","FONDO_NACIONAL_GARANTIAS","REGION","SEGMENTO","CODIGO_SEGMENTO","FECHA_IMPORTACION","NIVEL_DE_RIESGO","FECHA_ULTIMA_FACTURACION","SUBSEGMENTO","TITULAR_UNIVERSAL","NEGOCIO_TITUTULARIZADO","SECTOR_ECONOMICO","PROFESION","CAUSAL","OCUPACION","CUADRANTE","FECHA_TRASLADO_PARA_COBRO","DESC_CODIGO_DE_GESTION_VISITA","FECHA_GRABACION_VISITA","ENDEUDAMIENTO","CALIFICACION_REAL","FECHA_PROMESA","RED","ESTADO_NEGOCIACION","TIPO_CLIENTE_SUFI","CLASE","FRANQUICIA","SALDO_CAPITAL_PESOS","SALDO_INTERESES_PESOS","PROBABILIDAD_DE_PROPENSION_DE_PAGO","PRIORIZACION_FINAL","PRIORIZACION_POR_CLIENTE","GRUPO_DE_PRIORIZACION","FECHA_PROMESA_V2","FECHA_PROMESA_AJUSTADA","DIAS_DESDE_TRASLADO","DIAS_SIN_COMPROMISO","DIAS_SIN_PAGO","DIAS_SIN_RPC","FRANJA_MORA","RANGO_PROP_TRASLADO","RANGO_PROP_PAGO","RANGO_PROP_CONTACTO","RANGO_PROP_ACUERDO","DESFASE","EQUIPO","MEJOR_DIA","MEJOR_HORA","LOTE","VUELTAS_REQUERIDAS","VUELTAS_REALES","GRABADOR_AJUSTADO"]
                
                b = descargas.descargar_csv(route, query, header) # Hago el llamado a la función de descarga.

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje