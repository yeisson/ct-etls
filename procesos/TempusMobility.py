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
from datetime import datetime, timedelta
import dataflow_pipeline.TempusMobility.tipificador_mobility_beam as tipificador_mobility_beam
import time
import sys

tipificador_api = Blueprint('tipificador_api', __name__) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]

now = datetime.now()
fecha = now.strftime('%Y-%m-%d')
hora = now.strftime('%H:%M:%S')
TimeTest = str(hora)
KEY_REPORT = "tipificador"

hora_ini = "07:00:00"  ## En esta parte se define la hora inicial, la cual siempre sera a partir de las 7am
hora_fin = TimeTest  ## Aqui convertimos la hora en string para la compatibilidad


""" En la siguiente parte se realizara la extraccion de la ruta donde se almacenara el JSON convertido """

Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]
ext = ".csv"
ruta_completa = "/"+ Ruta +"/BI_Archivos/GOOGLE/Mobility/Tipificacion/" + fecha + ext

"""Comenzamos con el desarrollo del proceso que ejecutaremos para la actualizacion del API  """


@tipificador_api.route("/" + "Tipificador", methods=['POST','GET']) 
def Ejecutar():

    reload(sys)
    sys.setdefaultencoding('utf8')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-auteco')
    gcs_path = 'gs://ct-auteco'
    sub_path = "tipificacion_tempus/"
    output = gcs_path + "/" + sub_path + fecha + ext
    blob = bucket.blob(sub_path + fecha + ext)  

       


    client = bigquery.Client()
    QUERY = (
        'SELECT Client_Service, Auth_Key, Content_Type, Authorization, User_ID FROM `contento-bi.Auteco_Mobility.Auth` ') 
    query_job = client.query(QUERY)
    rows = query_job.result()   
      


    try:
        os.remove(ruta_completa) #Eliminar de aries
    except: 
        print("Eliminado de aries")
    
    try:
        blob.delete() #Eliminar del storage-----
    except: 
        print("Eliminado de storage")

    try:
        QUERY2 = ('DELETE FROM `contento-bi.Auteco_Mobility.tipificador` where substr(fecha_gestion,1,10)' + ' = ' + "'" + str(fecha) + "'")
        query_job = client.query(QUERY2)
        rows2 = query_job.result()
    except: 
        print QUERY2    
   

    """A continuacion, se procedera con abrir el archivo y realizar la escritura del JSON """
    
    file = open(ruta_completa,"a") 
    
    for row in rows:
        url = url = "https://prd.contentotech.com/services/microservices/bi/index.php/Rest_Bi_Reports/data"
        print ('URL es igual '+ url)   
        
        headers = {
        'Client-Service': row.Client_Service,
        'Auth-Key': row.Auth_Key,
        'Content-Type': row.Content_Type,
        'Authorization': row.Authorization,
        'User-ID': str(row.User_ID),
        
        }

        payload = "{\"base\":\"102\",\"fecha_ini\":\"" + str(fecha) + "\",\"hora_ini\":\"07:00:00\",\"fecha_fin\":\"" + str(fecha) + "\",\"hora_fin\":\"" + str(hora_fin) + "\"}"

        print(payload)

        datos = requests.request("POST", url, headers=headers, data=payload)
        print ('Los datos son ' + str(datos.text))
        

        if len(datos.content) < 50:
            continue
        else:
            i = datos.json()
            for rown in i:
                file.write(
                    str(rown["Transaccion"]).encode('utf-8')+"|"+
                    str(rown["Documento"]).encode('utf-8')+"|"+
                    str(rown["Nombre_Campana"]).encode('utf-8')+"|"+
                    str(rown["Gestion"]).encode('utf-8')+"|"+
                    str(rown["Causal"]).encode('utf-8')+"|"+
                    str(rown["SubCausal"]).encode('utf-8')+"|"+
                    str(rown["SubCausal1"]).encode('utf-8')+"|"+
                    str(rown["SubCausal2"]).encode('utf-8')+"|"+
                    str(rown["Observacion"]).encode('utf-8')+"|"+
                    str(rown["Completado"]).encode('utf-8')+"|"+
                    str(rown["Ult_Pagina"]).encode('utf-8')+"|"+
                    str(rown["Lenguaje"]).encode('utf-8')+"|"+
                    str(rown["Agente"]).encode('utf-8')+"|"+
                    str(rown["Fecha_Ingreso"]).encode('utf-8')+"|"+
                    str(rown["Hora_Ingreso"]).encode('utf-8')+"|"+
                    str(rown["Pregunta"]).encode('utf-8')+"|"+
                    str(rown["Fecha_Respuesta"]).encode('utf-8')+"|"+
                    str(rown["Hora_Respuesta"]).encode('utf-8')+"|"+
                    str(rown["Fecha_Gestion"]).encode('utf-8')+"|"+
                    str(rown["Negociador"]).encode('utf-8')+"|"+
                    str(rown["Id_Gestion"]).encode('utf-8')+"|"+ "\n")
                                      
                    

               
    file.close()
    blob.upload_from_filename(ruta_completa)
    time.sleep(10)
    ejecutar = tipificador_mobility_beam.run(output, KEY_REPORT) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]    
    time.sleep(60)

    return("Gestion Finalizada ")
########################################################################################################################

