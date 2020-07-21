from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import dataflow_pipeline.refinancia.refinancia_seguimiento_beam as refinancia_seguimiento_beam
import dataflow_pipeline.refinancia.refinancia_prejuridico_beam as refinancia_prejuridico_beam
import dataflow_pipeline.refinancia.refinancia_seguimiento_aut_beam as refinancia_seguimiento_aut_beam
import os
import socket
import time
import _mssql
import datetime
import sys

#coding: utf-8 


refinancia_api = Blueprint('refinancia_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@refinancia_api.route("/archivos_seguimiento")
def archivos_Seguimiento():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-refinancia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.refinancia.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = refinancia_seguimiento_beam.run('gs://ct-refinancia/info-seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/Seguimiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

################################################################################################################
    
@refinancia_api.route("/archivos_Prejuridico")
def archivos_Prejuridico():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/Prejuridico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[23:31]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-refinancia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-prejuridico/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.refinancia.prejuridico` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = refinancia_prejuridico_beam.run('gs://ct-refinancia/info-prejuridico/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/Prejuridico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

########################################################################################################################################################################################################

@refinancia_api.route("/seguimiento_aut")
def seguimiento_aut():
    
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="BDA01\DELTA"
    USER="BI_Bogota"
    PASSWORD="340$Uuxwp7Mcxo7Khy"
    DATABASE="Refinancia"
    TABLE_DB = "dbo.Tb_Seguimiento"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_seguimiento,Id_docdeu,Id_gestion,Id_causal,fecha_seguimiento,Nota,Id_usuario,Valor_Saldo_Total,numero_contac FROM ' + TABLE_DB  + ' where CAST(fecha_seguimiento AS date) = CAST(GETDATE() as DATE) ')
    ##conn.execute_query('SELECT Id_seguimiento,Id_docdeu,Id_gestion,Id_causal,fecha_seguimiento,Nota,Id_usuario,Valor_Saldo_Total,numero_contac FROM ' + TABLE_DB  + ' where CAST(fecha_seguimiento AS date) = CAST(' + "'2020-07-15' as DATE) ")
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        Nota = str(row['Nota']).replace('\r', '').replace('\n', '')
        text_row =  ""
        text_row += str(row['Id_seguimiento'])+ "|"
        text_row += str(row['Id_docdeu']).encode('utf-8') + "|"
        text_row += str(row['Id_gestion']).encode('utf-8') + "|"
        text_row += str(row['Id_causal']).encode('utf-8') + "|"
        text_row += str(row['fecha_seguimiento']).encode('utf-8') + "|"
        text_row += str(row['Id_usuario']).encode('utf-8') + "|"
        text_row += str(row['Valor_Saldo_Total']).encode('utf-8') + "|"
        text_row += str(row['numero_contac']).encode('utf-8') + "|"

        if Nota is None:
            text_row += "" + "|"
        if Nota.find("|") >= 0:
            text_row += NOTA.replace("|","*") + "|"
        else:
            text_row += Nota + "|"

        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "seguimiento_aut/Refinancia_Seguimiento_aut" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-refinancia")
    
    try:
        deleteQuery = "DELETE FROM `contento-bi.refinancia.seguimiento_aut` WHERE CAST(SUBSTR(fecha_seguimiento,0,10) AS DATE) = CURRENT_DATE()"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    time.sleep(30)

    flowAnswer = refinancia_seguimiento_aut_beam.run()

    time.sleep(40)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-refinancia')
    blob = bucket.blob("seguimiento_aut/Refinancia_Seguimiento_aut" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "X" + "flowAnswer" 
   