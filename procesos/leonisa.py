from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.leonisa.leonisa_seguimiento_beam as leonisa_seguimiento_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import sys

#coding: utf-8 

leonisa_api = Blueprint('leonisa_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@leonisa_api.route("/seguimiento")
def seguimiento():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\DELTA"
    USER="DP_USER"
    PASSWORD="Contento2018"
    DATABASE="Leonisa"
    TABLE_DB = "dbo.Tb_Seguimiento"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    # conn.execute_query('SELECT Id_Gestion,Fecha_Seguimiento,Id_Usuario,Id_Docdeu FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) = CAST(GETDATE() as DATE) ')
    conn.execute_query('SELECT Id_Gestion,Fecha_Seguimiento,Id_Usuario,Id_Docdeu FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) = CAST(' + "'2019-04-02' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Gestion']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Seguimiento']).encode('utf-8') + "|"
        text_row += row['Id_Usuario'].encode('utf-8') + "|"
        text_row += str(row['Id_Docdeu']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Seguimiento/Leonisa_inf_seg_" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-leonisa")

    try:
        deleteQuery = "DELETE FROM `contento-bi.leonisa.seguimiento` WHERE CAST(SUBSTR(Fecha_Seguimiento,0,10) AS DATE) = CURRENT_DATE()"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = leonisa_seguimiento_beam.run()

    # time.sleep(600)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-leonisa')
    blob = bucket.blob("Seguimiento/Leonisa_inf_seg_" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "X" + "flowAnswer" 
   