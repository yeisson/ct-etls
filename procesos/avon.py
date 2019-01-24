from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.avon.avon_prejuridico_beam as avon_prejuridico_beam
import dataflow_pipeline.avon.avon_pagos_beam as avon_pagos_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import socket
import _mssql
import datetime


avon_api = Blueprint('avon_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@avon_api.route("/archivos_pagos")
def archivos_Seguimiento():

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

<<<<<<< HEAD
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

    
=======

@avon_api.route("/prejuridico")
def prejuridico():
    SERVER="192.168.20.63\DELTA"
    USER="DP_USER"
    PASSWORD="Contento2018"
    DATABASE="Avon"
    TABLE_DB = "dbo.Tb_Cargue"
    FECHA_CARGUE = datetime.datetime.now()

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    # conn.execute_query('SELECT Ano,Campana,Factura,Zona,Unidad,Seccion,Territorio,Nit,Apellidos,Nombres,Direccion_Deudor,Direccion_Deudor_1,Barrio_Deudor,Departamento_Deudor,Ciudad_Deudor,Telefono_Deudor,Telefono_Deudor_1,Num_Campanas,Past Due,Ultim_Num_Invoice,Valor_Factura,Ultim_Ano_Pedido,Ultim_Campana_Pedido,Saldo,Email,Fecha_Factura,Valor_PD1,Telefono_Deudor_2,CT,Nombres_Referencia_Personal_1,Telefono_Referencia_Personal_1,Nombres_Referencia_Personal_2,Telefono_Referencia_Personal_2,Nombres_Referencia_Comercial_1,Telefono_Referencia_Comercial_1,Nombres_Referencia_Comercial_2,Telefono_Referencia_Comercial_2,Est.Disp,Ciclo,Vlr_redimir,Origen FROM' + TABLE_DB)
    conn.execute_query('SELECT * FROM' + TABLE_DB)

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += FECHA_CARGUE + "|"
        text_row += str(row['Ano']).encode('utf-8') + "|"
        text_row += str(row['Campana']).encode('utf-8') + "|"
        text_row += str(row['Factura']).encode('utf-8') + "|"
        text_row += str(row['Zona']).encode('utf-8') + "|"
        text_row += str(row['Unidad']).encode('utf-8') + "|"
        text_row += str(row['Seccion']).encode('utf-8') + "|"
        text_row += str(row['Territorio']).encode('utf-8') + "|"
        text_row += str(row['Nit']).encode('utf-8') + "|"
        text_row += str(row['Apellidos']).encode('utf-8') + "|"
        text_row += str(row['Nombres']).encode('utf-8') + "|"
        text_row += str(row['Direccion_Deudor']).encode('utf-8') + "|"
        text_row += str(row['Direccion_Deudor_1']).encode('utf-8') + "|"
        text_row += str(row['Barrio_Deudor']).encode('utf-8') + "|"
        text_row += str(row['Departamento_Deudor']).encode('utf-8') + "|"
        text_row += str(row['Ciudad_Deudor']).encode('utf-8') + "|"
        text_row += str(row['Telefono_Deudor']).encode('utf-8') + "|"
        text_row += str(row['Telefono_Deudor_1']).encode('utf-8') + "|"
        text_row += str(row['Num_Campanas']).encode('utf-8') + "|"
        text_row += str(row['Past Due']).encode('utf-8') + "|"
        text_row += str(row['Ultim_Num_Invoice']).encode('utf-8') + "|"
        text_row += str(row['Valor_Factura']).encode('utf-8') + "|"
        text_row += str(row['Ultim_Ano_Pedido']).encode('utf-8') + "|"
        text_row += str(row['Ultim_Campana_Pedido']).encode('utf-8') + "|"
        text_row += str(row['Saldo']).encode('utf-8') + "|"
        text_row += str(row['Email']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Factura']).encode('utf-8') + "|"
        text_row += str(row['Valor_PD1']).encode('utf-8') + "|"
        text_row += str(row['Telefono_Deudor_2']).encode('utf-8') + "|"
        text_row += str(row['CT']).encode('utf-8') + "|"
        text_row += str(row['Nombres_Referencia_Personal_1']).encode('utf-8') + "|"
        text_row += str(row['Telefono_Referencia_Personal_1']).encode('utf-8') + "|"
        text_row += str(row['Nombres_Referencia_Personal_2']).encode('utf-8') + "|"
        text_row += str(row['Telefono_Referencia_Personal_2']).encode('utf-8') + "|"
        text_row += str(row['Nombres_Referencia_Comercial_1']).encode('utf-8') + "|"
        text_row += str(row['Telefono_Referencia_Comercial_1']).encode('utf-8') + "|"
        text_row += str(row['Nombres_Referencia_Comercial_2']).encode('utf-8') + "|"
        text_row += str(row['Telefono_Referencia_Comercial_2']).encode('utf-8') + "|"
        text_row += str(row['Est.Disp']).encode('utf-8') + "|"
        text_row += str(row['Ciclo']).encode('utf-8') + "|"
        text_row += str(row['Vlr_redimir']).encode('utf-8') + "|"
        text_row += str(row['Origen']).encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row

    filename = "prejuridico/Avon_inf_prej_" + FECHA_CARGUE + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-avon")

    flowAnswer = avon_prejuridico_beam.run(filename, FECHA_CARGUE)

    return jsonify(flowAnswer), 200
>>>>>>> 60ca528489caa3bb171500cfecc0f465da84992e
