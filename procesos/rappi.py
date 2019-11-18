from flask import Blueprint
from flask import jsonify
from flask import request
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from google.cloud import datastore
from google.cloud import bigquery
from google.cloud import storage
import logging
import uuid
import json
import urllib3
import urllib
import socket
import requests
import os
import dataflow_pipeline.massive as pipeline
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import datetime
import time
import sys
import dataflow_pipeline.rappi.rappi_beam as rappi_beam

rappi_api = Blueprint('rappi_api', __name__)
fecha = time.strftime('%Y-%m-%d')


@rappi_api.route("/" + "formulario_rappi", methods=['GET']) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
def Ejecutar():

    reload(sys)
    sys.setdefaultencoding('utf8')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-rappi')
    gcs_path = 'gs://ct-rappi'
    sub_path = 'formulario_rappi_'
    output = gcs_path + "/" + sub_path + fecha + ".csv"
    blob = bucket.blob(sub_path + fecha + ".csv")
    dateini = request.args.get('fechaInicial')
    dateinip = str(dateini) + "%20" + "00:00:00"
    dateend = request.args.get('fechaFinal')
    dateendp = str(dateend) + "%20" + "23:59:59"
    client = bigquery.Client()
    Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]
    ruta_completa = "/"+ Ruta +"/BI_Archivos/GOOGLE/Rappi/"+ sub_path + fecha +".csv"

    if dateini is None:
        dateini = fecha
    else:   
        dateini = dateini

    if dateend is None:
        dateend = fecha
    else:
        dateend = dateend
    
    try:
        os.remove(ruta_completa) #Eliminar de aries
    except: 
        print("Eliminado de aries")
    
    try:
        blob.delete() #Eliminar del storage
    except: 
        print("Eliminado de storage")

    # try:
    #     QUERY2 = ('delete FROM `contento-bi.telefonia.login_logout` where date = ' + '"' + dateini[0:4] + '-' + dateini[4:-8] + '-' + dateini[6:-6] + '"')
    #     query_job = client.query(QUERY2)
    #     rows2 = query_job.result()
    # except: 
    #     print("Eliminado de bigquery")


    url = "https://webapp.contentobps.com/app/rappi/Desarrollo/back/API.php?token=HUCQHCHMENUNETUP&tipo=Encuesta&fechaInicial="+dateinip+"&fechaFinal="+dateendp
    datos = requests.get(url).content

    if len(requests.get(url).content) < 50:
        return jsonify("No hay datos entre estas fechas: " + (dateini) + " y " + (dateend))
    else:
        i = json.loads(datos)
        file = open(ruta_completa,"a")
        for row in i:
            file.write(
                str(row["0"]).encode('utf-8') +";"+
                str(row["1"]).encode('utf-8') +";"+
                str(row["2"]).encode('utf-8') +";"+
                str(row["3"]).encode('utf-8') +";"+
                str(row["4"]).encode('utf-8') +";"+
                str(row["5"]).encode('utf-8') +";"+
                str(row["6"]).encode('utf-8') +";"+
                str(row["7"]).encode('utf-8') +";"+
                str(row["8"]).encode('utf-8') +";"+
                str(row["9"]).replace("\r\n","").encode('utf-8') +";"+
                str(row["10"]).encode('utf-8') +";"+
                str(row["11"]).encode('utf-8') +";"+
                str(row["12"]).encode('utf-8') +";"+
                str(row["13"]).encode('utf-8') +";"+
                str(row["14"]).encode('utf-8') +";"+
                str(row["15"]).encode('utf-8') +";"+
                str(row["16"]).encode('utf-8') +";"+
                str(row["17"]).encode('utf-8') +";"+
                str(row["18"]).encode('utf-8') +";"+
                str(row["19"]).encode('utf-8') +";"+
                str(row["20"]).encode('utf-8') +";"+
                str(row["21"]).encode('utf-8') +";"+
                str(row["22"]).encode('utf-8') +";"+
                str(row["23"]).encode('utf-8') +";"+
                str(row["24"]).encode('utf-8') +"\n")
    
        file.close()
        blob.upload_from_filename(ruta_completa)
        time.sleep(10)
        ejecutar = rappi_beam.run(output) 
        time.sleep(60)

    return ("creo el archivo relajado")