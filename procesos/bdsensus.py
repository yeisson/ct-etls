from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.sensus.sensus_bd_beam as sensus_bd_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import sys

#coding: utf-8 

bdsensus_api = Blueprint('bdsensus_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@bdsensus_api.route("/bd")
def bd():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\delta"
    USER="jordan_ramirez"
    PASSWORD="Ramirez26082019*"
    DATABASE="sensus_copc"
    TABLE_DB = "dbo.dwh_historico"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_resultado,Id_CC,Doc_Asesor,nombres,doc_lider,Nombre_team_leader,doc_ejecutivo,nombre_ejecutivo,doc_gerente,Nombre_Gerente,descripcion,sede,producto,Doc_Asegurador,Evaluador,Fecha_Aseguramiento,estado_aseg,Hora_Aseguramiento,PEC,PENC,GOs,Id_Call,Doc_Cliente,Telefono_Cliente,Tipificacion,Fecha_Registro,Hora_Registro FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_resultado']).encode('utf-8') + "|"
        text_row += str(row['Id_CC']).encode('utf-8') + "|"
        text_row += str(row['Doc_Asesor']).encode('utf-8') + "|"
        text_row += str(row['nombres']).encode('utf-8') + "|"
        text_row += str(row['doc_lider']).encode('utf-8') + "|"
        text_row += str(row['Nombre_team_leader']).encode('utf-8') + "|"
        text_row += str(row['doc_ejecutivo']).encode('utf-8') + "|"
        text_row += str(row['nombre_ejecutivo']).encode('utf-8') + "|"
        text_row += str(row['doc_gerente']).encode('utf-8') + "|"
        text_row += str(row['Nombre_Gerente']).encode('utf-8') + "|"
        text_row += str(row['descripcion']).encode('utf-8') + "|"
        text_row += str(row['sede']).encode('utf-8') + "|"
        text_row += str(row['producto']).encode('utf-8') + "|"
        text_row += str(row['Doc_Asegurador']).encode('utf-8') + "|"
        text_row += str(row['Evaluador']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Aseguramiento']).encode('utf-8') + "|"
        text_row += str(row['estado_aseg']).encode('utf-8') + "|"
        text_row += str(row['Hora_Aseguramiento']).encode('utf-8') + "|"
        text_row += str(row['PEC']).encode('utf-8') + "|"
        text_row += str(row['PENC']).encode('utf-8') + "|"
        text_row += str(row['GOs']).encode('utf-8') + "|"
        text_row += str(row['Id_Call']).encode('utf-8') + "|"
        text_row += str(row['Doc_Cliente']).encode('utf-8') + "|"
        text_row += str(row['Telefono_Cliente']).encode('utf-8') + "|"
        text_row += str(row['Tipificacion']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Registro']).encode('utf-8') + "|"
        text_row += str(row['Hora_Registro']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Segmento/bd_sensus" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-sensus")

    try:
        deleteQuery = "DELETE FROM `contento-bi.sensus.bd_sensus` WHERE CAST(SUBSTR(Fecha_registro,0,10) AS DATE) = CURRENT_DATE()"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    time.sleep(60)

    flowAnswer = sensus_bd_beam.run()

    time.sleep(600)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-sensus')
    blob = bucket.blob("Segmento/bd_sensus" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "X" + "flowAnswer" 