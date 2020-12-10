from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import sys
import dataflow_pipeline.bdalarmas.alarmas_beam as alarmas_beam

#coding: utf-8 

bdalarmas_api = Blueprint('bdalarmas_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@bdalarmas_api.route("/bd")
def bd():


    client = bigquery.Client()
    QUERY = ('select string_field_0, string_field_1, string_field_2, string_field_3, string_field_4 from `contento-bi.sensus.db_conn`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    data = ""


    for row in rows:
        servidor = row.string_field_0
        usuario = row.string_field_1
        contrasena = row.string_field_2
        data_base = row.string_field_3 
        tabla_bd = row.string_field_4


    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER= servidor
    USER=usuario
    PASSWORD=contrasena
    DATABASE=data_base
    TABLE_DB = tabla_bd

    
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Nombre_Guia, Id_resultado, fecha_registro, Doc_Asesor, nombres, doc_team, Nombre_team_leader, estado_aseg, estado_alarma, reagendamiento,Descripcion,Asignacion FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Nombre_Guia']).encode('utf-8') + "|"
        text_row += str(row['Id_resultado']).encode('utf-8') + "|"
        text_row += str(row['fecha_registro']).encode('utf-8') + "|"
        text_row += str(row['Doc_Asesor']).encode('utf-8') + "|"
        text_row += str(row['nombres']).encode('utf-8') + "|"
        text_row += str(row['doc_team']).encode('utf-8') + "|"
        text_row += str(row['Nombre_team_leader']).encode('utf-8') + "|"
        text_row += str(row['estado_aseg']).encode('utf-8') + "|"
        text_row += str(row['estado_alarma']).encode('utf-8') + "|"        
        text_row += str(row['reagendamiento']).encode('utf-8') + "|"
        text_row += str(row['Descripcion']).encode('utf-8') + "|"
        text_row += str(row['Asignacion']).encode('utf-8') + "|"           
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    print ("Estos son los datos: " )

    filename = "Segmento/alarmas" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-sensus")

    try:
        deleteQuery = "DELETE FROM `contento-bi.sensus.bd_alarmas` WHERE CAST(SUBSTR(Fecha_registro,0,10) AS DATE) = DATE_ADD(CURRENT_DATE(),INTERVAL -1 DAY)"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    time.sleep(60)

    flowAnswer = alarmas_beam.run()

    time.sleep(600)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-sensus')
    blob = bucket.blob("Segmento/alarmas" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "X" + "flowAnswer" 