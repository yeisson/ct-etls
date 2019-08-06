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
import socket
import requests
import os
import dataflow_pipeline.massive as pipeline
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import datetime
import time
import sys
import dataflow_pipeline.telefonia.agent_status_beam as agent_status_beam #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]

agent_status_api = Blueprint('agent_status_api', __name__) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]


#############################3 DEFINICION DE VARIABLES ###########################

zona_horaria = (1, 2)[socket.gethostname()=="contentobi"]
hoy = datetime.datetime.now()
ayer = datetime.datetime.today() - datetime.timedelta(days = zona_horaria)
ano = str(hoy.year)
hour1 = "000000"
hour2 = "235959"
if len(str(hoy.day)) == 1:
    dia = "0" + str(hoy.day)
else:
    dia = str(hoy.day)

if len(str(hoy.month)) == 1:
    mes = "0"+ str(hoy.month)
else:
    mes = str(hoy.month)

GetDate1 = str(ano)+str(mes)+str(dia)+str(hour1)
GetDate2 = str(ano)+str(mes)+str(dia)+str(hour2)

fecha = str(ano)+str(mes)+str(dia)
KEY_REPORT = "agent_status" #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
CODE_REPORT = "cbps_satustime" #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]
ext = ".csv"
ruta_completa = "/"+ Ruta +"/BI_Archivos/GOOGLE/Telefonia/"+ KEY_REPORT +"/" + fecha + ext


########################### CODIGO #####################################################################################

@agent_status_api.route("/" + KEY_REPORT, methods=['GET']) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
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
        'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera FROM telefonia.parametros_ipdial') #WHERE ipdial_code = "intcob-unisabaneta"
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
        QUERY2 = ('delete FROM `contento-bi.telefonia.agent_status` where replace(substr(date,0,10),"-","") = ' + '"' + dateini[0:8] + '"')
        query_job = client.query(QUERY2)
        rows2 = query_job.result()
    except: 
        print("Eliminado de bigquery")

    file = open(ruta_completa,"a")
    for row in rows:
        url = 'http://' + str(row.servidor) + '/ipdialbox/api_reports.php?token=' + row.token + '&report=' + str(CODE_REPORT) + '&date_ini=' + dateini + '&date_end=' + dateend
        datos = requests.get(url).content
        if len(requests.get(url).content) < 40:
            continue
        else:
            i = json.loads(datos)
            for rown in i:
                file.write(
                    str(rown["operation"]).encode('utf-8')+"|"+
                    str(rown["date"])+"|"+
                    str(rown["hour"])+"|"+
                    str(rown["id_agent"])+"|"+
                    str(rown["agent_identification"])+"|"+
                    str(rown["agent_name"]).encode('utf-8')+"|"+
                    str(rown["CALLS"])+"|"+
                    str(rown["CALLS INBOUND"])+"|"+
                    str(rown["CALLS OUTBOUND"])+"|"+
                    str(rown["CALLS INTERNAL"])+"|"+
                    str(rown["READY TIME"])+"|"+
                    str(rown["INBOUND TIME"])+"|"+
                    str(rown["OUTBOUND TIME"])+"|"+
                    str(rown["NOT-READY TIME"])+"|"+
                    str(rown["RING TIME"])+"|"+
                    str(rown["LOGIN TIME"])+"|"+
                    str(rown["AHT"])+"|"+
                    str(rown["OCUPANCY"]).encode('utf-8')+"|"+
                    str(rown["AUX TIME"])+"|"+
                    str(row.id_cliente)+"|"+
                    str(row.cartera).encode('utf-8') + "\n")

    file.close()
    blob.upload_from_filename(ruta_completa)
    time.sleep(10)
    ejecutar = agent_status_beam.run(output, KEY_REPORT) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]    
    time.sleep(60)

    return("El proceso " + KEY_REPORT + ". Fue Cargado Exitosamente en la fecha: " + fecha)
########################################################################################################################