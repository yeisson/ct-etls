from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.linead.linead_seguimiento_beam as linead_seguimiento_beam
import dataflow_pipeline.linead.linead_prejuridico_beam as linead_prejuridico_beam
import dataflow_pipeline.linead.SF_Nivel_atencion_beam as SF_Nivel_atencion_beam
import dataflow_pipeline.linead.SF_Chats_hora_beam as SF_Chats_hora_beam
import dataflow_pipeline.linead.SF_Calificacion_chats_beam as SF_Calificacion_chats_beam
import dataflow_pipeline.linead.SF_Chats_estados_beam as SF_Chats_estados_beam
import dataflow_pipeline.linead.SF_NS_solucion_beam as SF_NS_solucion_beam
import dataflow_pipeline.linead.SF_Chats_agente_beam as SF_Chats_agente_beam
import dataflow_pipeline.linead.chat_interacciones_beam as chat_interacciones_beam
import dataflow_pipeline.linead.chats_beam as chats_beam
import os
import socket

linead_api = Blueprint('linead_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@linead_api.route("/archivos_seguimiento_linead")
def archivos_Seguimiento_linead():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = linead_seguimiento_beam.run('gs://ct-linead/info-seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Seguimiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje
################################################################################################################

@linead_api.route("/archivos_prejuridico")
def archivos_Prejuridico():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Prejuridico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[26:34]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-prejuridico/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.prejuridico` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = linead_prejuridico_beam.run('gs://ct-linead/info-prejuridico/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Prejuridico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


##################################### INFORMES SALESFORCE DE ATENCION ######################################

######################################### NIVEL DE ATENCION #########################################

@linead_api.route("/nivel_atencion")
def nivel_atencion():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Nivel_atencion/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[14:22]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Salesforce/Nivel_atencion/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.SF_Nivel_atencion` WHERE fecha = '20200824'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = SF_Nivel_atencion_beam.run('gs://ct-linead/Salesforce/Nivel_atencion/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Nivel_atencion/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]



############################# CHATS POR HORA ######################################

@linead_api.route("/chats_hora")
def chats_hora():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Chats_por_hora/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[14:22]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Salesforce/Chats_hora/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.SF_Chats_por_hora` WHERE fecha = '20200824'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = SF_Chats_hora_beam.run('gs://ct-linead/Salesforce/Chats_hora/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Chats_por_hora/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]



##################################### CALIFICACION ATENCION CHATS ############################################

@linead_api.route("/calificacion_chat")
def calificacion_chat():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Calificacion_atencion_chat/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[18:26]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Salesforce/Calificacion_chats/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.SF_Calificacion_chats` WHERE fecha = '20200824'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = SF_Calificacion_chats_beam.run('gs://ct-linead/Salesforce/Calificacion_chats/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Calificacion_atencion_chat/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]


######################################## CASOS CHATS POR ESTADO ####################################

@linead_api.route("/estados_chats")
def estados_chats():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Estados_chats_por_atencion/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[12:20]
            
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Salesforce/Chats_estados/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.SF_Estado_chats` WHERE fecha = '20200824'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = SF_Chats_estados_beam.run('gs://ct-linead/Salesforce/Chats_estados/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Estados_chats_por_atencion/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]

########################################### NIVEL SERVICIO SOLUCION ##########################################

@linead_api.route("/ns_solucion")
def ns_solucion():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Nivel_atencion_solucion/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[11:19]
            
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Salesforce/ns_solucion/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.SF_NS_Solucion` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = SF_NS_solucion_beam.run('gs://ct-linead/Salesforce/ns_solucion/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Nivel_atencion_solucion/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]

##################################### CHATS POR AGENTE #######################################

@linead_api.route("/chats_agente")
def chats_agente():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Chats_por_agente/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[12:20]
            
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Salesforce/chats_agente/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.SF_Chats_por_agente` WHERE fecha = '20200825'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = SF_Chats_agente_beam.run('gs://ct-linead/Salesforce/chats_agente/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Atencion/Chats_por_agente/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]

###################################################################################################################################
################################# BASE CHAT INTERACCIONES #################################

@linead_api.route("/interacciones")
def interacciones():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route =  fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Chat_interacciones/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[5:13]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-telefonia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Chat_interacciones/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.telefonia.Chat_interacciones` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = chat_interacciones_beam.run('gs://ct-telefonia/Chat_interacciones/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Chat_interacciones/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

##################################################################################################################################


####################################################### CHATS SALESFORCE ##################################################


@linead_api.route("/chats")
def chats():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route =  fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Chats/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-linead')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Chats/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.linea_directa.Chats` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = chats_beam.run('gs://ct-linead/Chats/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Linead/Salesforce/Chats/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje
