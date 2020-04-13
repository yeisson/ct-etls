# -*- coding: utf-8 -*-

#######################################################################################################################
#Espíritu santo de DIOS, que sean tus manos tirando este código. en el nombre de JESÚS. amén y amén
#######################################################################################################################

from flask import Blueprint
from flask import jsonify
from flask import request
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.contento_tech.bancoSAC_beam as bancoSAC_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import time


bancosac_api = Blueprint('bancosac_api', __name__)
fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@bancosac_api.route("/bancosac", methods=['GET'])
def bancosac():

    import sys
    reload(sys)

    SERVER="192.168.20.63\DELTA"
    USER="CORR3WHPSXV"
    PASSWORD="Thanos2020***"
    DATABASE="CORR3WHPSXV"
    FECHA_CARGUE = datetime.date.today()
    AHORA = FECHA_CARGUE.strftime("%Y-%m-%d")

    filename = DATABASE + str(FECHA_CARGUE) +  ".csv"
    sub_path = 'bancolombiaSAC/'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-tech-tof')
    blob = bucket.blob(sub_path + filename)
    client = bigquery.Client()


    try:
        blob.delete() #Eliminar del storage-----
    except: 
        print("Eliminado de storage")

    try:
        QUERY = ("delete FROM `contento-bi.Contento_Tech.Gestiones_BancoSAC` where Fecha_Cargue = '" + AHORA + "'")
        query_job = client.query(QUERY)
        rows2 = query_job.result()
    except: 
        print("Eliminado de bigquery")


    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query("SELECT * FROM CORR3WHPSXV.dbo.CRM_TO_GOOGLE where CAST(Fecha_Gestion AS DATE) = '" + AHORA + "'")

    cloud_storage_rows = ""
    for row in conn:
        text_row =  ""
        text_row += '' + "|" if str(row['Id_Gestion']).encode('utf-8') is None else str(row['Id_Gestion']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Id_Cod_Gestion']).encode('utf-8') is None else str(row['Id_Cod_Gestion']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre_Codigo'].encode('utf-8') is None else row['Nombre_Codigo'].encode('utf-8') + "|"
        # text_row += '' + "|" if row['Observacion'].encode('utf-8') is None else row['Observacion'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Fecha_Gestion']).encode('utf-8') is None else str(row['Fecha_Gestion']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Usuario_gestion'].encode('utf-8') is None else row['Usuario_gestion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Documento'].encode('utf-8') is None else row['Documento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Num_Obligacion'].encode('utf-8') is None else row['Num_Obligacion'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Id_Campana']).encode('utf-8') is None else str(row['Id_Campana']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre_Campana'].encode('utf-8') is None else row['Nombre_Campana'].encode('utf-8') + "\n"

        cloud_storage_rows += text_row

    gcscontroller.create_file(sub_path + filename, cloud_storage_rows, "ct-tech-tof")   # Revisar problema con las subcarpetas
    flowAnswer = bancoSAC_beam.run(filename)

    conn.close()
    return flowAnswer