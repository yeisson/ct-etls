from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.avon.avon_prejuridico_beam as avon_prejuridico_beam
import dataflow_pipeline.avon.avon_seguimiento_beam as avon_seguimiento_beam
import dataflow_pipeline.avon.avon_pagos_beam as avon_pagos_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime

#coding: utf-8 

avon_api = Blueprint('avon_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@avon_api.route("/archivos_pagos")
def archivos_pagos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Avon/Pagos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".txt"):
            mifecha = archivo[11:19]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-avon')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info_Pagos/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.avon.Info_Pagos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = avon_pagos_beam.run('gs://ct-avon/info_Pagos/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Avon/Pagos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

@avon_api.route("/archivos_Balance")
def archivos_Seguimiento():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Avon/Balance/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".txt"):
            mifecha = archivo[8:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-avon')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Balance/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.avon_Balance` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = avon_pagos_beam.run('gs://ct-avon/Balance/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Avon/Balance/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


@avon_api.route("/prejuridico")
def prejuridico():
    SERVER="192.168.20.63\DELTA"
    USER="DP_USER"
    PASSWORD="Contento2018"
    DATABASE="Avon"
    TABLE_DB = "dbo.Tb_Docdeu"
    FECHA_CARGUE = str(datetime.date.today())
    Fecha = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    # Insertamos los datos de la nueva consulta equivalentes al mismo dia de la anterior eliminacion
    conn.execute_query("SELECT * FROM " + TABLE_DB + " WHERE Fecha = " + "CAST('"+ Fecha + "'AS DATE)")
    # conn.execute_query("SELECT * FROM " + TABLE_DB + " WHERE Fecha >= CAST('2018-12-20' AS DATE)")

    cloud_storage_rows = ""
    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += row['Id_Docdeu'].encode('utf-8') + "|"
        text_row += row['Nit'].encode('utf-8') + "|"
        text_row += row['Factura'].encode('utf-8') + "|"
        text_row += str(row['Fecha_Factura']).encode('utf-8') + "|"
        text_row += row['Campana'].encode('utf-8') + "|"
        text_row += row['Ano'].encode('utf-8') + "|"
        text_row += row['Zona'].encode('utf-8') + "|"
        text_row += str(row['Unidad']).encode('utf-8') + "|"
        text_row += str(row['Seccion']).encode('utf-8') + "|"
        text_row += str(row['Past Due']).encode('utf-8') + "|"
        text_row += str(row['Ultim_Num_InVoice']).encode('utf-8') + "|"
        text_row += str(row['Valor_Factura']).encode('utf-8') + "|"
        text_row += str(row['Saldo']).encode('utf-8') + "|"
        text_row += str(row['N_Vencidas']).encode('utf-8') + "|"
        text_row += str(row['Num_Campanas']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['Valor_PD1']).encode('utf-8') + "|"
        text_row += str(row['CT']).encode('utf-8') + "|"
        text_row += row['Fecha'].encode('utf-8') + "|"
        text_row += row['Usuario'].encode('utf-8') + "|"
        text_row += str(row['asignacion']).encode('utf-8') + "|"
        text_row += row['Ciclo'].encode('utf-8') + "|"
        text_row += str(row['Vlr_redimir']).encode('utf-8') + "|"
        text_row += str(row['dia']).encode('utf-8') + "|"
        text_row += str(row['Dia_Estrategia']).encode('utf-8') + "|"
        text_row += str(row['Origen']).encode('utf-8') + "|"
        text_row += row['marca'].encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row
    
    filename = "prejuridico/Avon_inf_prej_" + FECHA_CARGUE +  ".csv"
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-avon")

    try:
        deleteQuery = "DELETE FROM `contento-bi.avon.prejuridico` WHERE CAST(Fecha AS DATE) = CURRENT_DATE()"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar porque no existe una tabla llamada asi")

    time.sleep(10)
    
    flowAnswer = avon_prejuridico_beam.run()

    time.sleep(20)
# Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-avon')
    blob = bucket.blob(filename)
    # Eliminar el archivo en la variable
    blob.delete()
    return "R, " + 'flowAnswer'

############################################################################################
############################################################################################

@avon_api.route("/seguimiento")
def seguimiento():
    SERVER="192.168.20.63\DELTA"
    USER="DP_USER"
    PASSWORD="Contento2018"
    DATABASE="Avon"
    TABLE_DB = "dbo.Tb_Seguimiento"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    # conn.execute_query('SELECT Ano,Cam-pana,Factura,Zona,Unidad,Seccion,Territorio,Nit,Apellidos,Nombres,Direccion_Deudor,Direccion_Deudor_1,Barrio_Deudor,Departamento_Deudor,Ciudad_Deudor,Telefono_Deudor,Telefono_Deudor_1,Num_Campanas,Past Due,Ultim_Num_Invoice,Valor_Factura,Ultim_Ano_Pedido,Ultim_Campana_Pedido,Saldo,Email,Fecha_Factura,Valor_PD1,Telefono_Deudor_2,CT,Nombres_Referencia_Personal_1,Telefono_Referencia_Personal_1,Nombres_Referencia_Personal_2,Telefono_Referencia_Personal_2,Nombres_Referencia_Comercial_1,Telefono_Referencia_Comercial_1,Nombres_Referencia_Comercial_2,Telefono_Referencia_Comercial_2,Est.Disp,Ciclo,Vlr_redimir,Origen FROM' + TABLE_DB)
    conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) = CAST(GETDATE() as DATE) ')

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Gestion']).encode('utf-8') + "|"
        text_row += str(row['Id_Causal']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Seguimiento']).encode('utf-8') + "|"
        text_row += row['Id_Usuario'].encode('utf-8') + "|"
        text_row += str(row['Valor_Obligacion']).encode('utf-8') + "|"
        text_row += str(row['Id_Docdeu']).encode('utf-8')


        text_row += "\n"

        cloud_storage_rows += text_row
        

    filename = "Seguimiento/Avon_inf_seg_" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-avon")

    try:
        deleteQuery = "DELETE FROM `contento-bi.avon.seguimiento` WHERE CAST(SUBSTR(Fecha_Seguimiento,0,10) AS DATE) = CURRENT_DATE()"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    


    flowAnswer = avon_seguimiento_beam.run()

    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-avon')
    blob = bucket.blob("Seguimiento/Avon_inf_seg_" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()

    # return jsonify(flowAnswer), 200
    return "X" + "flowAnswer" 
