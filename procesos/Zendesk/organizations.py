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
import dataflow_pipeline.Ofima.organization_beam as organization_beam #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]


organizations_api = Blueprint('organizations_api', __name__) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]


############################# DEFINICION DE VARIABLES ###########################
fecha = time.strftime('%Y%m%d')
hour1 = "000000"
hour2 = "235959"

GetDate1 = time.strftime('%Y%m%d')+str(hour1)
GetDate2 = time.strftime('%Y%m%d')+str(hour2)

KEY_REPORT = "organizations" #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
CODE_REPORT = "organizations" #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]
ext = ".csv"
ruta_completa = "/"+ Ruta +"/BI_Archivos/GOOGLE/Agendamientos/"+ KEY_REPORT +"/" + fecha + ext

user = 'BLOCKED'
token = 'BLOCKED'

########################### CODIGO #####################################################################################

@organizations_api.route("/" + KEY_REPORT, methods=['GET']) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
def Ejecutar():

    reload(sys)
    sys.setdefaultencoding('utf8')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-ofima_sac')
    gcs_path = 'gs://ct-ofima_sac'
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

    try:
     QUERY = (
         'Delete `contento-bi.Ofima_sac.organizations` where id is not null ') #WHERE ipdial_code = "intcob-unisabaneta"
     query_job = client.query(QUERY)
     rows = query_job.result()
     data = ""
    except:
        print("Esto es una prueba xD")  


    try:
        os.remove(ruta_completa) #Eliminar de aries
    except: 
        print("Eliminado de aries")
    
    try:
        blob.delete() #Eliminar del storage-----
    except: 
        print("Eliminado de storage")

     
   
    dato = 0
    

    file = open(ruta_completa,"a")
    for i in range(20):
        
        url = 'https://mesadeayudaofima.zendesk.com' + '/api/v2/organizations'  + '.json?page=' + str(dato)  
        dato = dato + 1             
        print ('URL es igual '+ url)                
        datos = requests.get(url, auth=(user,token))
        print ('Los datos son ' + str(datos.content))
        
        # print(url)

        if len(datos.content) < 50:
            continue
        else:
          ##  i = json.loads(datos)
            i = datos.json()
            for rown in i['organizations']:
                file.write(
                    str(rown["name"]).encode('utf-8')+"|"+
                    str(rown["id"]).encode('utf-8')+"|"+
                    str(rown["shared_tickets"]).encode('utf-8').replace('\n', ' ').replace('\r', '').replace('&nbsp', '')+"|"+
                    str(rown["shared_comments"]).encode('utf-8')+"|"+
                    str(rown["external_id"]).encode('utf-8')+"|"+
                    str(rown["created_at"]).encode('utf-8')+"|"+
                    str(rown["updated_at"]).encode('utf-8')+"|"+ "\n")
               
    file.close()
    blob.upload_from_filename(ruta_completa)
    time.sleep(10)
    ejecutar = organization_beam.run(output, KEY_REPORT) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]    
    time.sleep(60)

    return("Se acaba de ejecutar el proceso de " + KEY_REPORT + " Para actualizar desde: " + dateini + " hasta " + dateend)
########################################################################################################################

