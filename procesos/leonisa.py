from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.leonisa.leonisa_seguimiento_beam as leonisa_seguimiento_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import socket
import _mssql
import time
from datetime import datetime

leonisa_api = Blueprint('leonisa_api', __name__)

@leonisa_api.route("/")
def inicio():
    return "API's de Leonisa"

@leonisa_api.route("/seguimiento")
def Seguimiento():

    #Variables de conexion a MS SQL
    SERVER="BDA01\DELTA"
    USER="DP_USER"
    PASSWORD="Contento2018"
    DATABASE="Leonisa"

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Seguimiento, Id_Docdeu, Id_Gestion, Id_Causal, Fecha_Seguimiento, Id_Usuario, Id_Abogado, Id_pago FROM dbo.Tb_Seguimiento WHERE Fecha_Seguimiento BETWEEN  GETDATE()-1 AND GETDATE()+1')

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Seguimiento']).encode('utf-8') + "|"
        text_row += row['Id_Docdeu'].encode('utf-8') + "|"
        text_row += str(row['Id_Gestion']).encode('utf-8') + "|"
        text_row += str(row['Id_Causal']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Seguimiento']).encode('utf-8') + "|"
        text_row += str(row['Id_Usuario']).encode('utf-8') + "|"
        text_row += str(row['Id_Abogado']).encode('utf-8') + "|"
        text_row += str(row['Id_pago']).encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row
    
    date = datetime.today().strftime('%Y-%m-%d-%H-%M')

    filename = "info-seguimiento/LEONISA_INF_SEG_" + date + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-leonisa")

    # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
    #deleteQuery = "DELETE FROM `contento-bi.leonisa.seguimiento` WHERE 1=1"

    #Primero eliminamos todos los registros que contengan esa fecha
    #client = bigquery.Client()
    #query_job = client.query(deleteQuery)

    #result = query_job.result()
    #query_job.result() # Corremos el job de eliminacion de datos de BigQuery

    #Luego procedemos a correr el proceso de Dataflow
    flowAnswer = leonisa_seguimiento_beam.run(filename, date)

    return jsonify(flowAnswer), 200