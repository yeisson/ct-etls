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
import dataflow_pipeline.telefonia.cdr_unconnected_beam as cdr_unconnected_beam #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]

cdr_unconnected_api = Blueprint('cdr_unconnected_api', __name__) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]


########################### DEFINICION DE VARIABLES ###########################
fecha = time.strftime('%Y%m%d')
hour1 = "000000"
hour2 = "235959"

GetDate1 = time.strftime('%Y%m%d')+str(hour1)
GetDate2 = time.strftime('%Y%m%d')+str(hour2)

KEY_REPORT = "cdr_unconnected" #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
CODE_REPORT = "cdr_unconnected_calls" #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]
ext = ".csv"
ruta_completa = "/"+ Ruta +"/BI_Archivos/GOOGLE/Telefonia/"+ KEY_REPORT +"/" + fecha + ext


########################### CODIGO #####################################################################################

@cdr_unconnected_api.route("/" + KEY_REPORT, methods=['GET']) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
def Ejecutar():
    
    reload(sys)
    sys.setdefaultencoding('utf8')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-telefonia')
    gcs_path = 'gs://ct-telefonia'
    sub_path = KEY_REPORT + '/'
    output = gcs_path + "/" + sub_path + fecha + ext
    blob = bucket.blob(sub_path + fecha + ext)
    dateini = request.args.get('dateini')
    dateend = request.args.get('dateend')

    if dateini is None:
        dateini = GetDate1
    else:
        dateini = dateini + hour1

    if dateend is None:
        dateend = GetDate2
    else:
        dateend = dateend + hour2

    client = bigquery.Client()
    QUERY = (
        'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera FROM telefonia.parametros_prueba where Estado = "Activado" ') #WHERE ipdial_code = "intcob-unisabaneta" 
        #  'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera FROM telefonia.parametros_ipdial where ipdial_code = "intcob-banco-sufi-cast"') 
    query_job = client.query(QUERY)
    rows = query_job.result()
    data = ""
    
    try:
        os.remove(ruta_completa) #Eliminar de aries
    except: 
        print("Eliminado de aries")
    
    try:
        blob.delete() #Eliminar del storage
    except: 
        print("Eliminado de storage")

    try:
        QUERY2 = ('delete FROM `contento-bi.telefonia.cdr_unconnected` where cast(substr(date,0,10)as date) = ' + '"' + dateini[0:4] + '-' + dateini[4:-8] + '-' + dateini[6:-6] + '"')
        query_job = client.query(QUERY2)
        rows2 = query_job.result()
    except: 
        print("Eliminado de bigquery")

    file = open(ruta_completa,"a")
    for row in rows:
        url = 'http://' + str(row.servidor) + '/ipdialbox/api_reports.php?token=' + row.token + '&report=' + str(CODE_REPORT) + '&date_ini=' + dateini + '&date_end=' + dateend
        datos = requests.get(url).content

        
        if len(requests.get(url).content) < 50:
            continue
        else:
            i = json.loads(datos)
            for rown in i:
                file.write(
                   str(rown["name_agent"]).encode('utf-8')+"|"+
                    str(rown["date"]).encode('utf-8')+"|"+
                    str(rown["destination"]).encode('utf-8')+"|"+
                    str(rown["tel"]).encode('utf-8')+"|"+
                    str(rown["time"]).encode('utf-8')+"|"+
                    str(rown["dialstatus"]).encode('utf-8')+"|"+
                    str(rown["type_call"]).encode('utf-8')+"|"+
                    str(rown["id_customer"]).encode('utf-8')+"|"+
                    str(rown["id_campaing"]).encode('utf-8')+"|"+              
                    str(row.id_cliente)+"|"+
                    str(row.ipdial_code)+"|"+
                    str(row.cartera).encode('utf-8') + "\n")
    
    file.close()
    blob.upload_from_filename(ruta_completa)
    time.sleep(10)
    ejecutar = cdr_unconnected_beam.run(output, KEY_REPORT) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]    
    time.sleep(60)

    return("Se acaba de ejecutar el proceso de " + KEY_REPORT + " Para actualizar desde: " + dateini + " hasta " + dateend)
########################################################################################################################

@cdr_unconnected_api.route("/" + KEY_REPORT + "_recov", methods=['GET']) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
def Ejecutar2():

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    ayer = yesterday.strftime("%Y%m%d")

    reload(sys)
    sys.setdefaultencoding('utf8')
    urllib.urlopen('http://35.239.77.81:5000/telefonia/'+ KEY_REPORT + '?dateini=' + str(ayer) + '&dateend=' + str(ayer))

    return ('Datos recuperados por el desperdicio hora a hora')