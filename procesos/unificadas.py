from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.unificadas.unificadas_segmentos_beam as unificadas_segmentos_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import sys

#coding: utf-8 

unificadas_api = Blueprint('unificadas_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@unificadas_api.route("/segmento")
def segmento():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_segmentos"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Segmento,Nombre_Segmento,Fecha_Creacion,Usuario_Creacion,Estado FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Segmento']).encode('utf-8') + "|"
        text_row += str(row['Nombre_Segmento']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Usuario_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Segmento/Unificadas_Segmento" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.segmentos` WHERE CAST(SUBSTR(Fecha_Creacion,0,10) AS DATE) = CURRENT_DATE()"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    time.sleep(60)

    flowAnswer = unificadas_segmentos_beam.run()

    time.sleep(600)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("Segmento/Unificadas_Segmento" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "X" + "flowAnswer" 
   