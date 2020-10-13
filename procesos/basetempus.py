from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.basetempus.adeinco_base_beam as adeinco_base_beam
import dataflow_pipeline.basetempus.adeinco2_base_beam as adeinco2_base_beam
import dataflow_pipeline.basetempus.agaval_base_beam as agaval_base_beam
import dataflow_pipeline.basetempus.agaval_base2_beam as agaval_base2_beam
import dataflow_pipeline.basetempus.leonisa_estrategia_beam as leonisa_estrategia_beam
import dataflow_pipeline.basetempus.leonisa_historico_beam as leonisa_historico_beam
import dataflow_pipeline.basetempus.leonisa_prejuridico_beam as leonisa_prejuridico_beam
import dataflow_pipeline.basetempus.linea_lineatel_beam as linea_lineatel_beam
import dataflow_pipeline.basetempus.linea_cod_beam as linea_cod_beam
import dataflow_pipeline.basetempus.linea_prejul_beam as linea_prejul_beam
import dataflow_pipeline.basetempus.aval_aval1_beam as aval_aval1_beam
import dataflow_pipeline.basetempus.aval_aval2_beam as aval_aval2_beam
import socket
import os
import requests
from flask import request
from google.cloud import bigquery
import pandas as pd
from pandas import DataFrame
import socket
import os


basetempus_api = Blueprint('basetempus_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]
 
######################################################################################################

@basetempus_api.route("/adeinco1")
def adeinco1():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Adeinco1/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('adeinco1/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.adeinco1` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = adeinco_base_beam.run('gs://ct-unificadas/adeinco1/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Adeinco1/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

############################################################################################################3

@basetempus_api.route("/adeinco2")
def adeinco2():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Adeinco2/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('adeinco2/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.adeinco2` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = adeinco2_base_beam.run('gs://ct-unificadas/adeinco2/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Adeinco2/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

#########################################################################################################

@basetempus_api.route("/agaval")
def agaval():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Agaval/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agaval/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.agaval` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = agaval_base_beam.run('gs://ct-unificadas/agaval/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Agaval/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


########################################################################################################################33333333

@basetempus_api.route("/agaval2")
def agaval2():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Agaval2/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agaval2/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.agaval2` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = agaval_base2_beam.run('gs://ct-unificadas/agaval2/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Agaval/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

########################################################################################################################33333333

@basetempus_api.route("/descargar")
def descargar(myRoute, myQuery, myHeader):

    query = myQuery
    client = bigquery.Client()
    df = client.query(query).to_dataframe()

    cabecera = myHeader
    df.colums = cabecera

    if df.empty:
        code = 400
        descripcion = 'ERROR!! No existen datos para los parametros ingresados'
          
    else:        
        mi_ruta_descarga = fileserver_baseroute + myRoute
        df.to_csv(mi_ruta_descarga, encoding='utf-8')   # Ejecuta la descarga del arvhivo csv, especificando el tipo de codificacion 'utf-8'.
        code = 200
        descripcion = 'Descarga Exitosa, Ubicacion: ' + mi_ruta_descarga   

    response = {}
    response["code"] = code
    response["description"] = descripcion
    response["status"] = False

    return jsonify(response), response["code"]

########################################################################################################################33333333    

@basetempus_api.route("/tempus")
def tempus():

    response = {}
    response["code"] = 5000
    response["description"] = "Solicitud de descarga realizada"
    response["status"] = False

    route = '/BI_Archivos/GOOGLE/Tempus/Descarga/base.csv'

    query = 'SELECT * FROM `contento-bi.unificadas.Base_consolidada_tempus`'
    header = ["ID_Cargue","id_campana","id_segmento","zona","Cod_Ciudad","Documento","cod_interno","tipo_comprador","customer_class","cupo","num_obligacion","vlr_factura","Fecha_Factura","Fecha_Vencimiento","vlr_saldo_cartera","Dias_vencimiento","Campana_Orig","Ult_Campana","codigo","Nombre_Cliente","Apellidos_Cliente","Telefono","Celular","Tel_Cel_2","Email","Aut_Envio_SMS","Aut_CVoz_SMS","Aut_Mail_SMS","Direccion","Barrio","Ciudad","Departamento","Direccion1","Barrio1","Ciudad1","Departamento1","Nombre_Ref","Apellidos_Ref","Parentesco_Ref","Celular_Ref","Nombre_Ref1","Apellidos_Ref1","Direccion_Ref1","Ciudad_ref1","Departamento_Ref1","Parentesco_Ref1","Telefono_Ref1","Celular_Ref1","Nombre_Ref2","Apellidos_Ref2","Telefono_Ref2","Celular_Ref2","Direccion_Ref2","Ciudad_ref2","Departamento_Ref2","Nombre_Ref3","Apellidos_Ref3","Direccion_Ref3","Telefono_Ref3","Celular_Ref3","Ciudad_ref3","Departamento_Ref3","Abogado","Division","Pais","Fecha_Prox_Conferencia","Cod_Gestion","fecha_gestion","Fecha_Promesa_Pago","Actividad_Economica","Saldo_capital","Num_Cuotas","Num_Cuotas_Pagadas","Num_Cuotas_Faltantes","Num_Cuotas_Mora","Cant_Veces_Mora","Fecha_Ult_Pago","Saldo_Total_Vencido","Plan","Cod_Consecionario","Concesionario","Cod_Gestion_Ant","Grabador","Interes_Mora","Vlr_Cuotas_Vencidas","seguro_vencido"]

    b = descargar(route, query, header)

    return jsonify(response), response["code"]


########################################################################################################################33333333

@basetempus_api.route("/estrategia")
def estrategia():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Estrategia/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('estrategia/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.estrategia` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = leonisa_estrategia_beam.run('gs://ct-unificadas/estrategia/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Estrategia/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

   ####################################################################################################### 


   ########################################################################################################################33333333

@basetempus_api.route("/historico")
def historico():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Historico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('historico/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.historico` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = leonisa_historico_beam.run('gs://ct-unificadas/historico/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Historico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

   ####################################################################################################### 

@basetempus_api.route("/prejuridico")
def prejuridico():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Prejuridico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('prejuridico_2/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.prejuridico` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = leonisa_prejuridico_beam.run('gs://ct-unificadas/prejuridico_2/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Prejuridico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

   ####################################################################################################### 

    ####################################################################################################### 

@basetempus_api.route("/lineatel")
def lineatel():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Lineatel/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('lineatel/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.lineatel` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = linea_lineatel_beam.run('gs://ct-unificadas/lineatel/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Lineatel/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

#######################################################################################################


@basetempus_api.route("/cod")
def cod():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Cod/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('cod/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.cod` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = linea_cod_beam.run('gs://ct-unificadas/cod/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Cod/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

#######################################################################################################
#######################################################################################################


@basetempus_api.route("/prejul")
def prejul():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Prejul/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('prejul/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.prejul` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = linea_prejul_beam.run('gs://ct-unificadas/prejul/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/prejul/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

   #######################################################################################################

#######################################################################################################


@basetempus_api.route("/aval1")
def aval1():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Aval1/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('aval1/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.aval1` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = aval_aval1_beam.run('gs://ct-unificadas/aval1/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Aval1/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

   #######################################################################################################

   #######################################################################################################


@basetempus_api.route("/aval2")
def aval2():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Aval2/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-unificadas')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('aval2/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.unificadas.aval2` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = aval_aval2_beam.run('gs://ct-unificadas/aval2/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Tempus/Aval2/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje    

   #######################################################################################################