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
import dataflow_pipeline.bdlal.lal_beam as lal_beam

#coding: utf-8 

bdlal_api = Blueprint('bdlal_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@bdlal_api.route("/bd")
def bd():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\delta"
    USER="jordan_ramirez"
    PASSWORD="Ramirez26082019*"
    DATABASE="sensus_copc"
    TABLE_DB = "dbo.dwh_historico_lado_lado"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT id_lal, centro_c, negociador, nombres_neg, producto, doc_team, nombres_team, doc_ejec, nombre_ejecutivo, doc_ger,Nombre_gerente,id_call,evualuador,nombres,fecha_registro,hora_registro,cumple,cierre_detalle,compromiso FROM ' + TABLE_DB + ' WHERE CONVERT(DATE,fecha_registro) = CONVERT(DATE,GETDATE()-1)')
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_lal']).encode('utf-8') + "|"
        text_row += str(row['centro_c']).encode('utf-8') + "|"
        text_row += str(row['negociador']).encode('utf-8') + "|"
        text_row += str(row['nombres_neg']).encode('utf-8') + "|"
        text_row += str(row['producto']).encode('utf-8') + "|"
        text_row += str(row['doc_team']).encode('utf-8') + "|"
        text_row += str(row['nombres_team']).encode('utf-8') + "|"
        text_row += str(row['doc_ejec']).encode('utf-8') + "|"
        text_row += str(row['nombre_ejecutivo']).encode('utf-8') + "|"        
        text_row += str(row['doc_ger']).encode('utf-8') + "|"
        text_row += str(row['Nombre_gerente']).encode('utf-8') + "|"
        text_row += str(row['id_call']).encode('utf-8') + "|"
        text_row += str(row['evualuador']).encode('utf-8') + "|"
        text_row += str(row['nombres']).encode('utf-8') + "|"
        text_row += str(row['fecha_registro']).encode('utf-8') + "|"       
        text_row += str(row['hora_registro']).encode('utf-8') + "|"
        text_row += str(row['cumple']).encode('utf-8') + "|"        
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    print ("Estos son los datos: " )

    filename = "Segmento/bd_lal" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-sensus")

    try:
        deleteQuery = "DELETE FROM `contento-bi.sensus.bd_lal` WHERE CAST(SUBSTR(Fecha_registro,0,10) AS DATE) = DATE_ADD(CURRENT_DATE(),INTERVAL -1 DAY)"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    time.sleep(60)

    flowAnswer = lal_beam.run()

    time.sleep(600)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-sensus')
    blob = bucket.blob("Segmento/bd_lal" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "X" + "flowAnswer" 