# encoding=utf8
from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.bancolombia.bancolombia_castigada_seguimiento_beam as bancolombia_castigada_seguimiento_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_factura_beam as bancolombia_castigada_factura_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_franjas_beam as bancolombia_castigada_franjas_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_metas_beam as bancolombia_castigada_metas_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_prejuridico_beam as bancolombia_castigada_prejuridico_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_compromisos_beam as bancolombia_castigada_compromisos_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_predictivo_beam as bancolombia_castigada_predictivo_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_sms_beam as bancolombia_castigada_sms_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_tts_beam as bancolombia_castigada_tts_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_pagos_beam as bancolombia_castigada_pagos_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_debitos_beam as bancolombia_castigada_debitos_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_robotics_debitos_beam as bancolombia_castigada_robotics_debitos_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_robotics_pagos_beam as bancolombia_castigada_robotics_pagos_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_robotics_prejuridico_beam as bancolombia_castigada_robotics_prejuridico_beam
import dataflow_pipeline.bancolombia.bancolombia_castigada_robotics_seguimiento_beam as bancolombia_castigada_robotics_seguimiento_beam
import procesos.descargas as descargas
import os
import socket
import time
import pandas as pd
import google.auth
from pandas import DataFrame
import codecs
# import sqlalchemy
# import BigQueryHelper

## from google.cloud import bigquery_storage_v1beta1

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

                # Guarda la información en Seguimiento Consolidado.
                # ----------------------------------------------------------------------------------------------------------------
                deleteQuery_1 = "DELETE FROM `contento-bi.Contento.seguimiento_consolidado` WHERE ID_OPERACION = '5' AND fecha = '" + mifecha + "'" 
                client_1 = bigquery.Client()
                query_job_1 = client_1.query(deleteQuery_1)
                query_job_1.result()    
                
                insertQuery_1 = "INSERT INTO `contento-bi.Contento.seguimiento_consolidado` (SELECT * FROM `contento-bi.bancolombia_castigada.QRY_CONSL_HORA_HORA` WHERE FECHA = '" + mifecha + "')"
                client_11 = bigquery.Client()
                query_job_11 = client_11.query(insertQuery_1)
                query_job_11.result()    
                # ----------------------------------------------------------------------------------------------------------------

                # Inicia proceso de calculo para Best Time.
                # ----------------------------------------------------------------------------------------------------------------
                # Extraccion de Contactos con Titular:
                deleteQuery_2 = "INSERT INTO `contento-bi.bancolombia_castigada.contactos_titular` (SELECT A.NIT, A.FECHA_GESTION FROM `contento-bi.bancolombia_castigada.QRY_EXTRACT_RPC` A LEFT JOIN `contento-bi.bancolombia_castigada.contactos_titular` B ON A.NIT = B.NIT AND A.FECHA_GESTION = B.FECHA_GESTION WHERE B.FECHA_GESTION IS '')"
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

################################################################################################################################################################

@bancolombia_castigada_api.route("/archivos_factura")
def archivos_factura():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Factura/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[17:25]
            tipo =  archivo[0:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-factura/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.factura` WHERE NOMBRE_ARCHIVO = '" + tipo + "'"
            #deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.factura` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_factura_beam.run('gs://ct-bancolombia_castigada/info-factura/' + archivo, mifecha, tipo)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Factura/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

################################################################################################################################################################

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

                time.sleep(210) # Le da tiempo al Storage, para que lleve la informacion a la tabla prejuridico en BigQuery.

                # Inicia proceso de calculo para Fecha de Promesa Ajustada.
                # ----------------------------------------------------------------------------------------------------------------
                # Busqueda de Fecha Promesa Ajustada (UPDATE):
                deleteQuery_2 = "UPDATE `contento-bi.bancolombia_castigada.ajuste_promesas` A SET A.MAX_FECHA_PROMESA_AJUSTADA = CAST(B.FECHA_PROMESA AS DATE) FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_MAX_DATE_HIT_UP` B WHERE A.NIT = B.NIT"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                time.sleep(30)

                # Busqueda de Fecha Promesa Ajustada (INSERT):
                deleteQuery_3 = "INSERT INTO `contento-bi.bancolombia_castigada.ajuste_promesas` (NIT, MAX_FECHA_PROMESA_AJUSTADA)	(SELECT NIT, CAST(FECHA_PROMESA AS DATE) FROM `contento-bi.bancolombia_castigada.QRY_CALCULATE_MAX_DATE_HIT_IN`)"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(deleteQuery_3)
                query_job_3.result()
                # ----------------------------------------------------------------------------------------------------------------

                time.sleep(15)

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

################################################################################################################################################################

@bancolombia_castigada_api.route("/archivos_compromiso")
def archivos_Compromisos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Compromisos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[20:28]
            tipo =  archivo[0:19]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-compromisos/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            #deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.compromisos` WHERE NOMBRE_ARCHIVO = '" + tipo + "'"
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.compromisos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_compromisos_beam.run('gs://ct-bancolombia_castigada/info-compromisos/' + archivo, mifecha, tipo)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Compromisos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                time.sleep(200) # Le da tiempo al Storage, para que lleve la informacion a la tabla compromisos en BigQuery.

                # ----------------------------------------------------------------------------------------------------------------
                #  (DELETE):
                deleteQuery_2 = "DELETE FROM `contento-bi.bancolombia_castigada.consolidado_compromisos` WHERE fecha = '" + mifecha + "'"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                ##time.sleep(60)

                # Busqueda de Fecha Promesa Ajustada (INSERT):
                deleteQuery_3 = "INSERT INTO `contento-bi.bancolombia_castigada.consolidado_compromisos` (FECHA, CEDULA, EQUIPO, NOMBRE_REGIONAL, DIAS_MORA, FECHA_GENERACION, CODIGO_DE_ABOGADO, FECHA_COMPROMISO, NO_DE_OBLIGACION, ESTADO, ID_GRABADOR, nombre_colaborador, nombre_lider, CDIGO_DE_GESTIN, HIT, CODIGO_CIERRE_COMPROMISO, DESCRIPCIN_CDIGO_DE_GESTIN, VALOR_PACTADO, VALOR_PAGADO, RANK) (SELECT * FROM `contento-bi.bancolombia_castigada.Informe_Compromisos`)"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(deleteQuery_3)
                query_job_3.result()

        return jsonify(response), response["code"]
        # return "Corriendo : " + mensaje

################################################################################################################################################################
@bancolombia_castigada_api.route("/archivos_franjas")
def archivos_franjas():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Franjas_sufi/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[13:21]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-franjas/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.franjas` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_franjas_beam.run('gs://ct-bancolombia_castigada/info-franjas/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Franjas_sufi/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

    ################################################################################################################################################################
@bancolombia_castigada_api.route("/archivos_metas")
def archivos_metas():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Metas/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[18:26]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-metas/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.metas` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_metas_beam.run('gs://ct-bancolombia_castigada/info-metas/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Metas/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

# ############################################################################################

@bancolombia_castigada_api.route("/archivos_masivos")
def archivos_masivos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Masivos/"

    # Definicion de variables insumo.
    tipo = ['Predictivo','SMS','TTS']
    fecha_ini = [11,4,4]
    fecha_fin = [21,14,14]
    my_storage = ['info-predictivo','info-sms','info-tts']
    my_table = ['predictivo','sms','tts']
    my_pipeline = [bancolombia_castigada_predictivo_beam,bancolombia_castigada_sms_beam,bancolombia_castigada_tts_beam]
    my_dates_process = ['','','']
    my_files = [[],[],[]]   # Almacena las fechas de los archivos.
    my_query = ['QRY_MASIVO_PREDICTIVO','QRY_MASIVO_SMS','QRY_MASIVO_TTS']

    # Hace limpieza de las 3 tablas completamente una sola vez, y no por cada archivo. Esto para prevenir el streaming buffer.
    for i in [0, 1, 2]:
        deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada." + my_table[i] + "` WHERE 1 = 1"                
        client = bigquery.Client()  #Eliminamos todos los registros que contengan esa fecha
        query_job = client.query(deleteQuery)
        query_job.result() # Corremos el job de eliminacion de datos de BigQuery

    # Limpia la tabla masivos con los resultados finales.
    for i in [0, 1, 2]:
        my_route = local_route + tipo[i] + '/'
        archivos = os.listdir(my_route)
        myCont = 0
        for archivo in archivos:
            if archivo.endswith(".csv"):
                mifecha = archivo[fecha_ini[i]:fecha_fin[i]]    # Almacena las fechas de los archivos.
                my_files[i].append(mifecha)
                if myCont == 0:
                    my_dates_process[i] = my_dates_process[i] + "'" + mifecha + "'"
                else:
                    my_dates_process[i] = my_dates_process[i] + ",'" + mifecha + "'"
                myCont += 1
                        
    deleteQuery_1 = "DELETE FROM `contento-bi.bancolombia_castigada.masivos` WHERE TIPO = 'Predictivo' AND FECHA IN (" + my_dates_process[0] + ") OR TIPO = 'SMS' AND FECHA IN (" + my_dates_process[1] + ") OR TIPO = 'TTS' AND FECHA IN (" + my_dates_process[2] + ")"
    client_1 = bigquery.Client()
    query_job_1 = client_1.query(deleteQuery_1)
    query_job_1.result() 

    # Inicia el proceso de carga de la información para los 3 tipos de masivos (Predictivo, SMS y TTS)
    for i in [0, 1, 2]:
        my_route = local_route + tipo[i] + '/'

        archivos = os.listdir(my_route)
        for archivo in archivos:
            if archivo.endswith(".csv"):
                mifecha = archivo[fecha_ini[i]:fecha_fin[i]]

                storage_client = storage.Client()
                bucket = storage_client.get_bucket('ct-bancolombia_castigada')

                # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
                blob = bucket.blob(my_storage[i] +'/' + archivo)
                blob.upload_from_filename(my_route + archivo)

                # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
                # deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada." + my_table[i] + "` WHERE fecha = '" + mifecha + "'"                

                # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job                                
                mensaje = my_pipeline[i].run('gs://ct-bancolombia_castigada/' + my_storage[i] + '/' + archivo, mifecha)
                if mensaje == "Corrio Full HD":
                    move(my_route + archivo, my_route + "Procesados/" + archivo)
                    response["code"] = 200
                    response["description"] = "Se realizo la peticion Full HD"
                    response["status"] = True

                    time.sleep(30) # Le da tiempo al Storage, para que lleve la informacion a la tabla seguimiento en BigQuery.

                    # Guarda la información ya procesada en Tabla General Masivos.                                          
                    insertQuery_1 = "INSERT INTO `contento-bi.bancolombia_castigada.masivos` (SELECT '" + mifecha + "','" + tipo[i] + "', LISTO FROM `contento-bi.bancolombia_castigada." + my_query[i] + "`)"
                    client_11 = bigquery.Client()
                    query_job_11 = client_11.query(insertQuery_1)
                    query_job_11.result()
    
    # Inicia el proceso de descarga de la Información ya procesada:
    for i in [0,1,2]:
         for my_file in my_files[i]:
            # Defino la ruta de descarga (Sin incluir '//192.168.20.87').
            download_route = '/BI_Archivos/GOOGLE/Bancolombia_Cast/Masivos/Exportes/' + tipo[i] + '_' + my_file + '.csv'
            # Defino la consulta SQL a ejecutar en BigQuery.
            export_query = "SELECT INFO FROM `contento-bi.bancolombia_castigada.masivos` WHERE TIPO = '" + tipo[i] + "' AND FECHA = '"  + my_file + "'"
            # Defino los títulos de los campos resultantes de la ejecución del query.
            header = ['INFO']
            
            descargas.descargar_csv(download_route, export_query, header) # Hago el llamado a la función de descarga.

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

################################################################################################################################################################

@bancolombia_castigada_api.route("/archivos_pagos_v2")
def archivos_pagos_v2():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Pagos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[6:14]
            # tipo =  archivo[0:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-pagos_v2/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery            
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.pagos_v2` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_pagos_beam.run('gs://ct-bancolombia_castigada/info-pagos_v2/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Pagos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    


################################################################################################################################################################

@bancolombia_castigada_api.route("/archivos_debitos")
def archivos_debitos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Debitos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-debitos/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.debitos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_debitos_beam.run('gs://ct-bancolombia_castigada/info-debitos/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Debitos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

################################################################################################################################################################

@bancolombia_castigada_api.route("/llevar_archivos")
def llevar_archivos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    # robotics_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Robotics/Pagos/"
    # bi_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Pagos/"

    fuente_local = "C:/Users/Daniel.Caro/Documents/Carteras de Cobranza/Bancolombia Castigada/Automatismo Pagos/Insumos/Pruebas/Seguimiento/Procesados/"

    # archivos = os.listdir(robotics_route)
    archivos = os.listdir(fuente_local)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            # mifecha = archivo[8:25]
            # copyfile(robotics_route+archivo,bi_route+"Pagos"+mifecha+".csv")
            #read input file
            # with codecs.open(robotics_route+archivo, 'r', encoding = 'utf8') as file:
            # with codecs.open(fuente_local+archivo, 'r', encoding = 'mbcs') as file:
            #     lines = file.read()

            #write output file
            # with codecs.open(bi_route+archivo, 'w', encoding = 'utf8') as file:
            # with codecs.open(fuente_local+archivo, 'w', encoding = 'utf8') as file:
            #     file.write(lines)
            os.remove(fuente_local+archivo)

    # move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Debitos/Procesados/"+archivo)
    response["code"] = 200
    response["description"] = "Se realizo la peticion Full HD"
    response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

################################################################################################################################################################

@bancolombia_castigada_api.route("/robotics/<variable>")
def robotics(variable):

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    tipo = ''

    if variable == 'Estado Cartera': 
        tipo = 'prejuridico'
    else: 
        tipo = variable.lower()

    # my_pipeline = 'bancolombia_castigada_robotics_'+ tipo +'_beam'
    my_pipeline = [
        bancolombia_castigada_robotics_debitos_beam,
        bancolombia_castigada_robotics_pagos_beam,
        bancolombia_castigada_robotics_prejuridico_beam,
        bancolombia_castigada_robotics_seguimiento_beam
        ]

    my_qry_insert = [
        "INSERT INTO `contento-bi.bancolombia_castigada.debitos` (SELECT AUX.IDKEY, AUX.FECHA, AUX.CONSECUTIVO_GESTION, AUX.CONSECUTIVO_OBLIGACION, AUX.NIT, AUX.NRO_DOCUMENTO, AUX.FECHA_GESTION, AUX.GRABADOR, AUX.CODIGO_DE_GESTION, AUX.DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO FROM `contento-bi.bancolombia_castigada.debitos` DEB RIGHT JOIN `contento-bi.bancolombia_castigada.rob_aux_debitos` AUX ON DEB.CONSECUTIVO_GESTION = AUX.CONSECUTIVO_GESTION WHERE DEB.CONSECUTIVO_GESTION IS NULL)",
        "INSERT INTO `contento-bi.bancolombia_castigada.pagos_v2` (SELECT AUX.IDKEY, AUX.FECHA, AUX.CONSECUTIVO, AUX.NIT, AUX.NOMBRES, AUX.FECHA_DE_PAGO, AUX.OBLIGACION, AUX.VALOR_PAGADO, AUX.CODIGO_ABOGADO, AUX.NOMBRE_ASESOR, AUX.FECHA_DE_GRABACION FROM `contento-bi.bancolombia_castigada.pagos_v2` PAG RIGHT JOIN `contento-bi.bancolombia_castigada.rob_aux_pagos` AUX ON PAG.FECHA_DE_PAGO = AUX.FECHA_DE_PAGO AND PAG.CONSECUTIVO = AUX.CONSECUTIVO AND PAG.VALOR_PAGADO = AUX.VALOR_PAGADO WHERE PAG.CONSECUTIVO IS NULL)",
        "INSERT INTO `contento-bi.bancolombia_castigada.prejuridico` (SELECT AUX.IDKEY, AUX.FECHA, AUX.CONSECUTIVO_DOCUMENTO_DEUDOR, AUX.VALOR_CUOTA, AUX.NIT, AUX.NOMBRES, AUX.NUMERO_DOCUMENTO, AUX.TIPO_PRODUCTO, AUX.FECHA_ACTUALIZACION_PRIORIZACION, AUX.FECHA_PAGO_CUOTA, AUX.NOMBRE_DE_PRODUCTO, AUX.FECHA_DE_PERFECCIONAMIENTO, AUX.FECHA_VENCIMIENTO_DEF, AUX.NUMERO_CUOTAS, AUX.CUOTAS_EN_MORA, AUX.DIA_DE_VENCIMIENTO_DE_CUOTA, AUX.VALOR_OBLIGACION, AUX.VALOR_VENCIDO, AUX.SALDO_ACTIVO, AUX.SALDO_ORDEN, AUX.REGIONAL, AUX.CIUDAD, AUX.GRABADOR, AUX.CODIGO_AGENTE, AUX.NOMBRE_ASESOR, AUX.CODIGO_ABOGADO, AUX.NOMBRE_ABOGADO, AUX.FECHA_ULTIMA_GESTION_PREJURIDICA, AUX.ULTIMO_CODIGO_DE_GESTION_PARALELO, AUX.ULTIMO_CODIGO_DE_GESTION_PREJURIDICO, AUX.DESCRIPCION_SUBSECTOR, AUX.DESCRIPCION_CODIGO_SEGMENTO, AUX.DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO, AUX.DESCRIPCION_SUBSEGMENTO, AUX.DESCRIPCION_SECTOR, AUX.DESCRIPCION_CODIGO_CIIU, AUX.CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO, AUX.DESC_CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO, AUX.FECHA_ULTIMA_GESTION_JURIDICA, AUX.ULTIMA_FECHA_DE_ACTUACION_JURIDICA, AUX.ULTIMA_FECHA_PAGO, AUX.EJEC_ULTIMO_CODIGO_DE_GESTION_JURIDICO, AUX.DESC_ULTIMO_CODIGO_DE_GESTION_JURIDICO, AUX.CANT_OBLIG, AUX.CLUSTER_PERSONA, AUX.DIAS_MORA, AUX.PAIS_RESIDENCIA, AUX.TIPO_DE_CARTERA, AUX.CALIFICACION, AUX.RADICACION, AUX.ESTADO_DE_LA_OBLIGACION, AUX.FONDO_NACIONAL_GARANTIAS, AUX.REGION, AUX.SEGMENTO, AUX.CODIGO_SEGMENTO, AUX.FECHA_IMPORTACION, AUX.NIVEL_DE_RIESGO, AUX.FECHA_ULTIMA_FACTURACION, AUX.SUBSEGMENTO, AUX.TITULAR_UNIVERSAL, AUX.NEGOCIO_TITUTULARIZADO, AUX.SECTOR_ECONOMICO, AUX.PROFESION, AUX.CAUSAL, AUX.OCUPACION, AUX.CUADRANTE, AUX.FECHA_TRASLADO_PARA_COBRO, AUX.DESC_CODIGO_DE_GESTION_VISITA, AUX.FECHA_GRABACION_VISITA, AUX.ENDEUDAMIENTO, AUX.CALIFICACION_REAL, AUX.FECHA_PROMESA, AUX.RED, AUX.ESTADO_NEGOCIACION, AUX.TIPO_CLIENTE_SUFI, AUX.CLASE, AUX.FRANQUICIA, AUX.SALDO_CAPITAL_PESOS, AUX.SALDO_INTERESES_PESOS, AUX.PROBABILIDAD_DE_PROPENSION_DE_PAGO, AUX.PRIORIZACION_FINAL, AUX.PRIORIZACION_POR_CLIENTE, AUX.GRUPO_DE_PRIORIZACION FROM `contento-bi.bancolombia_castigada.prejuridico` PRE RIGHT JOIN `contento-bi.bancolombia_castigada.rob_aux_prejuridico`  AUX ON PRE.CONSECUTIVO_DOCUMENTO_DEUDOR = AUX.CONSECUTIVO_DOCUMENTO_DEUDOR AND PRE.FECHA = AUX.FECHA WHERE PRE.CONSECUTIVO_DOCUMENTO_DEUDOR IS NULL)",
        "INSERT INTO `contento-bi.bancolombia_castigada.seguimiento` (SELECT AUX.IDKEY, AUX.FECHA, AUX.CONSECUTIVO_GESTION, AUX.DURACION, AUX.CONSECUTIVO_OBLIGACION, AUX.NIT, AUX.NOMBRE, AUX.NRO_DOCUMENTO, AUX.FECHA_GESTION, AUX.DIAS_MORA, AUX.ASESOR, AUX.REGIONAL, AUX.TIPO, AUX.TELEFONO, AUX.GRABADOR, AUX.NOMBRE_ABOGADO, AUX.CODIGO_ABOGADO, AUX.CODIGO_DE_GESTION, AUX.DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO, AUX.FECHA_VENCI, AUX.OCUPACION, AUX.T_ENTRADA, AUX.HORA_GRABACION, AUX.T_IGRABA, AUX.SITIO, AUX.HORA_DE_COMPROMISO, AUX.CODIGO_CAUSAL, AUX.CODIGO_DE_COBRO_ANTERIOR, AUX.CODCOB_MEC_NO, AUX.CODCOB_MEC_NOR_TRA, AUX.CODCOB_MEC_NOR_UTIL, AUX.CODCOB_T, AUX.CODIGO_DE_CONTACTO, AUX.CONTROL, AUX.CONSDIREC, AUX.CUADRANTE, AUX.FECHA_PROMESA, AUX.NOTA FROM `contento-bi.bancolombia_castigada.seguimiento` SEG RIGHT JOIN `contento-bi.bancolombia_castigada.rob_aux_seguimiento` AUX ON SEG.Consecutivo_Gestion = AUX.CONSECUTIVO_GESTION WHERE SEG.Consecutivo_Gestion IS NULL)"
    ]

    if tipo == 'debitos':
        puntero = 0
    elif tipo == 'pagos':
        puntero = 1
    elif tipo == 'prejuridico':
        puntero = 2
    elif tipo == 'seguimiento':
        puntero = 3

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Robotics/" + variable + "/"
    # local_route = "C:/Users/Daniel.Caro/Documents/Carteras de Cobranza/Bancolombia Castigada/Automatismo Pagos/Insumos/Pruebas/" + variable + "/"
    procesados_route = local_route + "Procesados/"
    
    archivos = os.listdir(local_route)    
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:16]
            # mihora = archivo[17:25]  

            # Lee el archivo en codificación ANSI.       
            try:
                with codecs.open(local_route+archivo, 'r', encoding = 'latin-1') as file:
                    lines = file.read()
            except:
                try:                    
                    with codecs.open(local_route+archivo, 'r', encoding = 'cp1252') as file:
                        lines = file.read()
                except:
                        try:
                            with codecs.open(local_route+archivo, 'r', encoding = 'mbcs') as file:
                                lines = file.read()
                        except:
                            print('definitivamente no se pudo leer la codificacion del archivo')                        

            # Sobreescribe el archivo en la codificación UTF-8.
            with codecs.open(local_route+archivo, 'w', encoding = 'utf8') as file:
                file.write(lines)

            # Inicia el proceso de Google Storage.
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-rob-aux-'+ tipo +'/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery            
            # deleteQuery = "CREATE TABLE `contento-bi.bancolombia_castigada.rob_aux_" + tipo + "1` AS (SELECT * FROM `contento-bi.bancolombia_castigada.rob_aux_" + tipo + "`);"
            # ejecutar_query(deleteQuery)
            # time.sleep(5)
            # deleteQuery = "DROP TABLE `contento-bi.bancolombia_castigada.rob_aux_" + tipo + "`;"
            # ejecutar_query(deleteQuery)
            # time.sleep(5)
            # deleteQuery = "CREATE TABLE `contento-bi.bancolombia_castigada.rob_aux_" + tipo + "` AS (SELECT * FROM `contento-bi.bancolombia_castigada.rob_aux_" + tipo + "1`);"
            # ejecutar_query(deleteQuery)
            # time.sleep(5)
            # deleteQuery = "DROP TABLE `contento-bi.bancolombia_castigada.rob_aux_" + tipo + "1`;"
            # ejecutar_query(deleteQuery)
            # time.sleep(5)
            # deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.rob_aux_" + tipo + "` WHERE 1 = 1;"
            # ejecutar_query(deleteQuery)

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = my_pipeline[puntero].run('gs://ct-bancolombia_castigada/info-rob-aux-'+ tipo + '/' + archivo, mifecha)
            # mensaje.wait_until_finish()
            if mensaje == "Corrio Full HD":
                ejecutar_query(my_qry_insert[puntero])  # Inserta sólo los nuevos elementos en la tabla principal a partir de la tabla auxiliar.
                move(local_route + archivo, procesados_route + archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return tipo

# @bancolombia_castigada_api.route("/ejecutar_query")
def ejecutar_query(mi_query):
    # Procedimiento que ejecuta los queries.
    client = bigquery.Client()
    query_job = client.query(mi_query)
    query_job.result() # Ejecución en BigQuery
    
    return mi_query + " ejecutado"

@bancolombia_castigada_api.route("/prueba/<variable>")
def prueba(variable):  
    # tipo = variable.lower()
    # deleteQuery = (
    #     'DELETE FROM `contento-bi.bancolombia_castigada.rob_aux_' + tipo + '` WHERE 1 =1;'
    #     'CREATE TABLE `contento-bi.bancolombia_castigada.rob_aux_' + tipo + '1` AS (SELECT * FROM `contento-bi.bancolombia_castigada.rob_aux_' + tipo + '`);'
    #     'DROP TABLE `contento-bi.bancolombia_castigada.rob_aux_' + tipo + '`;'
    #     'CREATE TABLE `contento-bi.bancolombia_castigada.rob_aux_' + tipo + '` AS (SELECT * FROM `contento-bi.bancolombia_castigada.rob_aux_' + tipo + '1`);'
    #     'DROP TABLE `contento-bi.bancolombia_castigada.rob_aux_' + tipo + '1`;'
    # )
    # respuesta = ejecutar_query(variable)    

    return 'Proceso de cargue para ' + variable 
    # return respuesta
    # return deleteQuery

@bancolombia_castigada_api.route("/limpiar_robotics") 
def limpiar_robotics():
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    tipo = ['Debitos','Pagos','Estado Cartera','Seguimiento','Historico']

    for i in [0,1,2,3,4]:
        if i < 4:
            robotics_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Robotics/" + tipo[i] + "/Procesados/"
            ext = ".csv"
        else:
            robotics_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Robotics/Historico/"
            ext = ".zip"

        archivos = os.listdir(robotics_route)
        for archivo in archivos:
            if archivo.endswith(ext):
                os.remove(robotics_route + archivo)

    response["code"] = 200
    response["description"] = "Se realizo la peticion Full HD"
    response["status"] = True

    return jsonify(response), response["code"]