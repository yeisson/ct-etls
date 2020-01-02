from flask import Blueprint

from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.leonisa.leonisa_seguimiento_beam as leonisa_seguimiento_beam
import dataflow_pipeline.leonisa.leonisa_prejuridico_beam as leonisa_prejuridico_beam
import dataflow_pipeline.leonisa.leonisa_recaudo_beam as leonisa_recaudo_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import socket
import time

leonisa_api = Blueprint('leonisa_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@leonisa_api.route("/archivos_seguimiento_leonisa")
def archivos_Seguimiento_leonisa():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Leonisa/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-leonisa')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.leonisa.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = leonisa_seguimiento_beam.run('gs://ct-leonisa/info-seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Leonisa/Seguimiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                # ----------------------------------------------------------------------------------------------------------------
                # Elimina datos de la tabla de Seguimiento Consolidado:
                deleteQuery_2 = "DELETE FROM `contento-bi.Contento.seguimiento_consolidado` WHERE ID_OPERACION = '3' AND FECHA = '" + mifecha + "'"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                time.sleep(240)

                # Inserta la informacion agrupada segun funciones de agregacion en la tabla de Seguimiento Consolidado:
                inserteDatos = "INSERT INTO `contento-bi.Contento.seguimiento_consolidado` (ID_OPERACION,FECHA,ANO,MES,NOMBRE_MES,DIA,HORA,GRABADOR,NEGOCIADOR,ID_LIDER,LIDER,EJECUTIVO,GERENTE,TIPO_CONTACTO,RANGO_MORA,TIENDA,MACRO_PRODUCTO,PRODUCTO,META_GESTIONES,TRABAJO,GESTIONES,WPC,RPC,HIT) (SELECT * FROM `contento-bi.leonisa.QRY_CONSL_HORA_HORA` WHERE FECHA = '"+ mifecha +"')"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(inserteDatos)
                query_job_3.result()
                # ----------------------------------------------------------------------------------------------------------------                                

    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]

#################################################################################################################

@leonisa_api.route("/archivos_prejuridico")
def archivos_Prejuridico():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Leonisa/Prejuridico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[17:25]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-leonisa')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-prejuridico/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.leonisa.prejuridico` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = leonisa_prejuridico_beam.run('gs://ct-leonisa/info-prejuridico/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Leonisa/Prejuridico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                # ----------------------------------------------------------------------------------------------------------------
                # Elimina datos de la tabla de Asignacion Consolidada:
                deleteQuery_2 = "DELETE FROM `contento-bi.Contento.asignacion_consolidada` WHERE ID_OPERACION = '3' AND FECHA = '" + mifecha + "'"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                time.sleep(240)

                # Inserta la informacion agrupada segun funciones de agregacion en la tabla de Asignacion Consolidada:
                inserteDatos = "INSERT INTO `contento-bi.Contento.asignacion_consolidada` (ID_OPERACION,FECHA,ANO,MES,NOMBRE_MES,DIA,FECHA_BASE,NOM_ABOGADO,REGION,PRODUCTO,CAL,ESTADO,SEGMENTO,RANGO_MORA,GERENTE,DIRECTOR,NOM_OPERACION,UEN,SEDE,TIPO_CARTERA,OBLIGACIONES,CLIENTES,VAL_OBLIGACION,VAL_VENCIDO,SALDO_ACTIVO) (SELECT * FROM `contento-bi.leonisa.VIEW_ASIGNACION_LEONISA` WHERE FECHA = '"+ mifecha +"')"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(inserteDatos)
                query_job_3.result()
                # ----------------------------------------------------------------------------------------------------------------                

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

#################################################################################################################

@leonisa_api.route("/archivos_recaudo")
def archivos_Recaudo():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Leonisa/Recaudo/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[16:24]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-leonisa')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-recaudo/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.leonisa.recaudo` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = leonisa_recaudo_beam.run('gs://ct-leonisa/info-recaudo/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Leonisa/Recaudo/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                # ----------------------------------------------------------------------------------------------------------------
                # Elimina datos de la tabla de Recaudo Consolidado:
                deleteQuery_2 = "DELETE FROM `contento-bi.Contento.recaudo_consolidado` WHERE ID_OPERACION = '3' AND FECHA = '" + mifecha + "'"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                time.sleep(240)

                # Inserta la informacion agrupada segun funciones de agregacion en la tabla de Recaido Consolidado:
                inserteDatos = "INSERT INTO `contento-bi.Contento.recaudo_consolidado` (ID_OPERACION,FECHA,ANO,MES,NOMBRE_MES,DIA,FECHA_BASE,REGION,PRODUCTO,SEGMENTO,RANGO_MORA,GRABADOR,NEGOCIADOR,LIDER,EJECUTIVO,GERENTE,DIRECTOR,NOM_OPERACION,UEN,SEDE,TIPO_CARTERA,VALOR_PAGADO,CANT_PAGOS,FACTURA) (SELECT * FROM `contento-bi.leonisa.VIEW_RECAUDO_LEONISA` WHERE FECHA = '"+ mifecha +"')"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(inserteDatos)
                query_job_3.result()
                # ----------------------------------------------------------------------------------------------------------------


    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje