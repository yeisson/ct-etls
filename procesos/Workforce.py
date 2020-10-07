from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from flask import request
import dataflow_pipeline.workforce.workforce_beam as workforce_beam
import dataflow_pipeline.workforce.Iti_beam as Iti_beam
import dataflow_pipeline.workforce.Iti_detalle_beam as Iti_detalle_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import dataflow_pipeline.massive as pipeline
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import sys

#coding: utf-8 

workforce_api = Blueprint('workforce_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@workforce_api.route("/workforce" , methods=['GET']  )
def workforce():
    
    dateini = request.args.get('dateini')
    dateend = request.args.get('dateend')

    if dateini is None: 
        dateini = ""
    else:
        dateini = dateini    

    if dateend is None: 
        dateend = ""
    else:
        dateend = dateend          

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "workforce"')
    query_job = client.query(QUERY)
    rows = query_job.result()
    data = ""

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd
    

    reload(sys)
    sys.setdefaultencoding('utf8')
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    # Nos conectamos a la BD y obtenemos los registros
  


    if dateini == "":    
        conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        conn.execute_query('SELECT documento_neg, segmento,Iter,Fecha_Malla,Hora_Inicio,Fecha_Final,Hora_Final,logueo,Deslogueo,Dif_Inicio,Dif_Final,Ausentismo,tiempo_malla,tiempo_conexion,tiempo_conexion_tiempo,Tiempo_EstAux,Tiempo_EstAux_tiempo,tiempo_estaux_out,tiempo_estaux_out_tiempo,adherencia_malla,adherencia_tiempo,Centro_Costo,rel_unico,rel_orden FROM ' + tabla_bd + ' WHERE CONVERT(DATE, FECHA_MALLA) = CONVERT(DATE,GETDATE())') 
        cloud_storage_rows = ""

        # conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        # conn.execute_query('SELECT documento_neg, segmento,Iter,Fecha_Malla,Hora_Inicio,Fecha_Final,Hora_Final,logueo,Deslogueo,Dif_Inicio,Dif_Final,Ausentismo,tiempo_malla,tiempo_conexion,tiempo_conexion_tiempo,Tiempo_EstAux,Tiempo_EstAux_tiempo,tiempo_estaux_out,tiempo_estaux_out_tiempo,adherencia_malla,adherencia_tiempo,Centro_Costo,rel_unico,rel_orden FROM ' + tabla_bd ) 
        # cloud_storage_rows = ""
    else:
        conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        conn.execute_query('SELECT documento_neg, segmento,Iter,Fecha_Malla,Hora_Inicio,Fecha_Final,Hora_Final,logueo,Deslogueo,Dif_Inicio,Dif_Final,Ausentismo,tiempo_malla,tiempo_conexion,tiempo_conexion_tiempo,Tiempo_EstAux,Tiempo_EstAux_tiempo,tiempo_estaux_out,tiempo_estaux_out_tiempo,adherencia_malla,adherencia_tiempo,Centro_Costo,rel_unico,rel_orden FROM ' + tabla_bd + ' WHERE CONVERT(DATE, FECHA_MALLA)'  ' between ' + "'" + dateini + "'" +" and " + "'" + dateend + "'"  )   
        cloud_storage_rows = ""



    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['documento_neg']).encode('utf-8') + "|"
        text_row += str(row['segmento']).encode('utf-8') + "|"
        text_row += str(row['Iter']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Malla']).encode('utf-8') + "|"
        text_row += str(row['Hora_Inicio']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Final']).encode('utf-8') + "|"
        text_row += str(row['Hora_Final']).encode('utf-8') + "|"
        text_row += str(row['logueo']).encode('utf-8') + "|"
        text_row += str(row['Deslogueo']).encode('utf-8') + "|"
        text_row += str(row['Dif_Inicio']).encode('utf-8') + "|"
        text_row += str(row['Dif_Final']).encode('utf-8') + "|"
        text_row += str(row['Ausentismo']).encode('utf-8') + "|"
        text_row += str(row['tiempo_malla']).encode('utf-8') + "|"
        text_row += str(row['tiempo_conexion']).encode('utf-8') + "|"
        text_row += str(row['tiempo_conexion_tiempo']).encode('utf-8') + "|"
        text_row += str(row['Tiempo_EstAux']).encode('utf-8') + "|"
        text_row += str(row['Tiempo_EstAux_tiempo']).encode('utf-8') + "|"
        text_row += str(row['tiempo_estaux_out']).encode('utf-8') + "|"
        text_row += str(row['tiempo_estaux_out_tiempo']).encode('utf-8') + "|"
        text_row += str(row['adherencia_malla']).encode('utf-8') + "|"
        text_row += str(row['adherencia_tiempo']).encode('utf-8') + "|"
        text_row += str(row['Centro_Costo']).encode('utf-8') + "|"
        text_row += str(row['rel_unico']).encode('utf-8') + "|"
        text_row += str(row['rel_orden']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "adherencia/workforce" + ".csv"
     #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-workforce")

    # try:
    #     deleteQuery = "DELETE FROM `contento-bi.Workforce.Adherencia` WHERE FECHA = '" + mifecha + "'"
    try:
        if dateini == "":
            deleteQuery = 'DELETE FROM `contento-bi.Workforce.Adherencia` WHERE CAST(Fecha_Malla AS DATE) = CURRENT_DATE()'
            # deleteQuery = 'DELETE FROM `contento-bi.Workforce.Adherencia` WHERE 1=1

            client = bigquery.Client()
            query_job = client.query(deleteQuery)
            query_job.result()
        else:
            deleteQuery2 = 'DELETE FROM `contento-bi.Workforce.Adherencia` WHERE CAST(Fecha_Malla AS DATE) between ' + "'" + dateini + "'" +" and " + "'" + dateend + "'"
            client = bigquery.Client()
            query_job = client.query(deleteQuery2)
            query_job.result()            
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = workforce_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-workforce')
    blob = bucket.blob("adherencia/workforce" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "data cargada" + "flowAnswer" 



##################################### TABLA ITI #####################################


@workforce_api.route("/Iti", methods=['GET'] )
def Iti():

    dateini = request.args.get('dateini')
    dateend = request.args.get('dateend')

    if dateini is None: 
        dateini = ""
    else:
        dateini = dateini    

    if dateend is None: 
        dateend = ""
    else:
        dateend = dateend   

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "iti"')
    query_job = client.query(QUERY)
    rows = query_job.result()
    data = ""

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')


    # Nos conectamos a la BD y obtenemos los registros
    if dateini == "":      
        conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        conn.execute_query('SELECT Id_Iti,Fecha,Hora,Centro_Costo,Peso,fecha_ejecucion,Estado FROM ' + tabla_bd  + " WHERE CONVERT(DATE, Fecha) = CONVERT(DATE,GETDATE())")
        # conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        # conn.execute_query('SELECT Id_Iti,Fecha,Hora,Centro_Costo,Peso,fecha_ejecucion,Estado FROM ' + tabla_bd)
    else:
        conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        conn.execute_query('SELECT Id_Iti,Fecha,Hora,Centro_Costo,Peso,fecha_ejecucion,Estado FROM ' + tabla_bd  + ' WHERE CONVERT(DATE, Fecha)'  ' between ' + "'" + dateini + "'" +" and " + "'" + dateend + "'"  )
    
    # conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    # conn.execute_query('SELECT Id_Iti,Fecha,Hora,Centro_Costo,Peso,fecha_ejecucion,Estado FROM ' + tabla_bd)
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Iti']).encode('utf-8') + "|"
        text_row += str(row['Fecha']).encode('utf-8') + "|"
        text_row += str(row['Hora']).encode('utf-8') + "|"
        text_row += str(row['Centro_Costo']).encode('utf-8') + "|"
        text_row += str(row['Peso']).encode('utf-8') + "|"
        text_row += str(row['fecha_ejecucion']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "workforce/iti" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-workforce")
   


    try:
        if dateini == "":
            deleteQuery = 'DELETE FROM `contento-bi.Workforce.Iti` WHERE CAST(Fecha AS DATE) = CURRENT_DATE()'
            # deleteQuery = "DELETE FROM `contento-bi.Workforce.Iti` WHERE id_iti is not null"
            client = bigquery.Client()
            query_job = client.query(deleteQuery)
            query_job.result()
        else:
            deleteQuery2 = "DELETE FROM `contento-bi.Workforce.Iti` WHERE CAST(Fecha AS DATE) between " + "'" + dateini + "'" +" and " + "'" + dateend + "'"  
            # deleteQuery = "DELETE FROM `contento-bi.Workforce.Iti` WHERE id_iti is not null"
            client = bigquery.Client()
            query_job = client.query(deleteQuery2)
            query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    flowAnswer = Iti_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-workforce')
    blob = bucket.blob("workforce/iti" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "data cargada" + "flowAnswer" 



################################### TII V2 #################################################

# @workforce_api.route("/Iti_detalle")
# def Iti_detalle():
#     reload(sys)
#     sys.setdefaultencoding('utf8')
#     SERVER="BDA01\DOKIMI"
#     USER="BI_Workforce"
#     PASSWORD="340$Uuxwp7Mcxo7Khy.*"
#     DATABASE="Workforce"
#     TABLE_DB ="tb_iti_detalle"
#     HOY = datetime.datetime.today().strftime('%Y-%m-%d')

#     # Nos conectamos a la BD y obtenemos los registros
    
#     conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
#     conn.execute_query('SELECT  Centro_Costos,fecha_malla,"07:30:00","07:45:00","08:00:00","08:15:00","08:30:00","08:45:00","09:00:00","09:15:00","09:30:00","09:45:00","10:00:00","10:15:00","10:30:00","10:45:00","11:00:00","11:15:00","11:30:00","11:45:00","12:00:00","12:15:00","12:30:00","12:45:00","13:00:00","13:15:00","13:30:00","13:45:00","14:00:00","14:15:00","14:30:00","14:45:00","15:00:00","15:15:00","15:30:00","15:45:00","16:00:00","16:15:00","16:30:00","16:45:00","17:00:00","17:15:00","17:30:00","17:45:00","18:00:00","18:15:00","18:30:00","18:45:00","19:00:00","19:15:00","19:30:00","19:45:00","20:00:00","20:15:00","20:30:00","20:45:00","21:00:00"  FROM ' + TABLE_DB)
#     #  + " WHERE CONVERT(DATE, Fecha_malla) = CONVERT(DATE,GETDATE())")
    
    
#     cloud_storage_rows = ""

#     # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
#     for row in conn:
#         text_row =  ""
#         text_row += str(row['Centro_Costos']).encode('utf-8') + "|"
#         text_row += str(row['fecha_malla']).encode('utf-8') + "|"
#         text_row += str(row['07:30:00']).encode('utf-8') + "|"
#         text_row += str(row['07:45:00']).encode('utf-8') + "|"
#         text_row += str(row['08:00:00']).encode('utf-8') + "|"
#         text_row += str(row['08:15:00']).encode('utf-8') + "|"
#         text_row += str(row['08:30:00']).encode('utf-8') + "|"
#         text_row += str(row['08:45:00']).encode('utf-8') + "|"
#         text_row += str(row['09:00:00']).encode('utf-8') + "|"
#         text_row += str(row['09:15:00']).encode('utf-8') + "|"
#         text_row += str(row['09:30:00']).encode('utf-8') + "|"
#         text_row += str(row['09:45:00']).encode('utf-8') + "|"
#         text_row += str(row['10:00:00']).encode('utf-8') + "|"
#         text_row += str(row['10:15:00']).encode('utf-8') + "|"
#         text_row += str(row['10:30:00']).encode('utf-8') + "|"
#         text_row += str(row['10:45:00']).encode('utf-8') + "|"
#         text_row += str(row['11:00:00']).encode('utf-8') + "|"
#         text_row += str(row['11:15:00']).encode('utf-8') + "|"
#         text_row += str(row['11:30:00']).encode('utf-8') + "|"
#         text_row += str(row['11:45:00']).encode('utf-8') + "|"
#         text_row += str(row['12:00:00']).encode('utf-8') + "|"
#         text_row += str(row['12:15:00']).encode('utf-8') + "|"
#         text_row += str(row['12:30:00']).encode('utf-8') + "|"
#         text_row += str(row['12:45:00']).encode('utf-8') + "|"
#         text_row += str(row['13:00:00']).encode('utf-8') + "|"
#         text_row += str(row['13:15:00']).encode('utf-8') + "|"
#         text_row += str(row['13:30:00']).encode('utf-8') + "|"
#         text_row += str(row['13:45:00']).encode('utf-8') + "|"
#         text_row += str(row['14:00:00']).encode('utf-8') + "|"
#         text_row += str(row['14:15:00']).encode('utf-8') + "|"
#         text_row += str(row['14:30:00']).encode('utf-8') + "|"
#         text_row += str(row['14:45:00']).encode('utf-8') + "|"
#         text_row += str(row['15:00:00']).encode('utf-8') + "|"
#         text_row += str(row['15:15:00']).encode('utf-8') + "|"
#         text_row += str(row['15:30:00']).encode('utf-8') + "|"
#         text_row += str(row['15:45:00']).encode('utf-8') + "|"
#         text_row += str(row['16:00:00']).encode('utf-8') + "|"
#         text_row += str(row['16:15:00']).encode('utf-8') + "|"
#         text_row += str(row['16:30:00']).encode('utf-8') + "|"
#         text_row += str(row['16:45:00']).encode('utf-8') + "|"
#         text_row += str(row['17:00:00']).encode('utf-8') + "|"
#         text_row += str(row['17:15:00']).encode('utf-8') + "|"
#         text_row += str(row['17:30:00']).encode('utf-8') + "|"
#         text_row += str(row['17:45:00']).encode('utf-8') + "|"
#         text_row += str(row['18:00:00']).encode('utf-8') + "|"
#         text_row += str(row['18:15:00']).encode('utf-8') + "|"
#         text_row += str(row['18:30:00']).encode('utf-8') + "|"
#         text_row += str(row['18:45:00']).encode('utf-8') + "|"
#         text_row += str(row['19:00:00']).encode('utf-8') + "|"
#         text_row += str(row['19:15:00']).encode('utf-8') + "|"
#         text_row += str(row['19:30:00']).encode('utf-8') + "|"
#         text_row += str(row['19:45:00']).encode('utf-8') + "|"
#         text_row += str(row['20:00:00']).encode('utf-8') + "|"
#         text_row += str(row['20:15:00']).encode('utf-8') + "|"
#         text_row += str(row['20:30:00']).encode('utf-8') + "|"
#         text_row += str(row['20:45:00']).encode('utf-8') + "|"
#         text_row += str(row['21:00:00']).encode('utf-8') + "|"
#         text_row += "\n"
#         cloud_storage_rows += text_row
#     conn.close()


#     filename = "workforce/iti_detalle" + ".csv"
#     #Finalizada la carga en local creamos un Bucket con los datos
#     gcscontroller.create_file(filename, cloud_storage_rows, "ct-workforce")


#     try:
#         # deleteQuery = 'DELETE FROM `contento-bi.Workforce.Iti.detalle` WHERE CAST(Fecha_malla AS DATE) = CURRENT_DATE()'

#         deleteQuery = "DELETE FROM `contento-bi.Workforce.Iti_detalle` WHERE Centro_Costos is not null"
#         client = bigquery.Client()
#         query_job = client.query(deleteQuery)
#         query_job.result()
#     except:
#         print("no se pudo eliminar")

#     #Primero eliminamos todos los registros que contengan esa fecha
    
#     flowAnswer = Iti_detalle_beam.run()

#     # time.sleep(60)
#     # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
#     storage_client = storage.Client()
#     bucket = storage_client.get_bucket('ct-workforce')
#     blob = bucket.blob("workforce/iti_detalle" + ".csv")
#     # Eliminar el archivo en la variable
#     blob.delete()
    
#     # return jsonify(flowAnswer), 200
#     return "data cargada" + "flowAnswer" 