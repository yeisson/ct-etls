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
import dataflow_pipeline.rappi.rappi2_beam as rappi2_beam

rappi_api = Blueprint('rappi_api', __name__)
fecha = time.strftime('%Y-%m-%d')


@rappi_api.route("/" + "formulario_rappi", methods=['GET']) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
def Ejecutar():

    # ejemplo de consulta: http://35.239.77.81:5000/rappi/formulario_rappi?fechaInicial=2019-11-14&fechaFinal=2019-11-17

    reload(sys)
    sys.setdefaultencoding('utf8')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-rappi')
    gcs_path = 'gs://ct-rappi'
    sub_path = 'formulario_rappi_'
    fecha = time.strftime('%Y-%m-%d')
    output = gcs_path + "/" + sub_path + fecha + ".csv"
    blob = bucket.blob(sub_path + fecha + ".csv")
    dateini = request.args.get('fechaInicial')
    dateend = request.args.get('fechaFinal')
    client = bigquery.Client()
    Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]
    ruta_completa = "/"+ Ruta +"/BI_Archivos/GOOGLE/Rappi/"+ sub_path + fecha +".csv"

    if dateini is None:
        dateini = fecha
        dateinip = str(dateini) + "%20" + "00:00:00"
    else:   
        dateini = dateini
        dateinip = str(dateini) + "%20" + "00:00:00"

    if dateend is None:
        dateend = fecha
        dateendp = str(dateend) + "%20" + "23:59:59"
    else:
        dateend = dateend
        dateendp = str(dateend) + "%20" + "23:59:59"
    
    try:
        os.remove(ruta_completa) #Eliminar de aries
    except: 
        print("Eliminado de aries")
    
    try:
        blob.delete() #Eliminar del storage
    except: 
        print("Eliminado de storage")

    try:
        QUERY2 = ('delete from `Rappi.flujo_react2` where substr(fecha,1,10) between ' + '"' + dateini + '"' + ' and "' + dateend + '"')
        query_job = client.query(QUERY2)
        rows2 = query_job.result()
    except: 
        print("Eliminado de bigquery")


    url = "https://webapp.contentobps.com/app/rappi/Desarrollo/back/API.php?token=HUCQHCHMENUNETUP&tipo=Encuesta&fechaInicial="+dateinip+"&fechaFinal="+dateendp
    datos = requests.get(url).content

    if len(requests.get(url).content) < 50:
        return jsonify("No hay datos entre estas fechas: " + (dateini) + " y " + (dateend))
    else:
        i = json.loads(datos)
        file = open(ruta_completa,"a")
        for row in i:
            file.write(
                ((str(row['id']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['fecha']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['accion']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['idLupe']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['idAgente']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['correo']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['telMarcado']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['ultimoPedido28Dias']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['motivoRegreso']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['comentarioTipificacion']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Motivo']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['SubMotivio']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Causal']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Codigo']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['SubCodigo']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Deuda']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Deuda_Mayor']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Deuda_Contra']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['MotivoChurcn']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['SubMotivoChurcn']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Tipo_Reactivacion']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Id_Pedido']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Ayuda']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Descrip_Ayuda']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['Cupon']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+"\n")
    
        file.close()
        blob.upload_from_filename(ruta_completa)
        time.sleep(10)
        ejecutar = rappi_beam.run(output) 
        time.sleep(60)

    # return (url)
    return ("creo el archivo relajado")




@rappi_api.route("/" + "formulario_rappi_sc", methods=['GET']) #[[[[[[[[[[[[[[[[[[***********************************]]]]]]]]]]]]]]]]]]
def Ejecutar2():

    # ejemplo de consulta: http://35.239.77.81:5000/rappi/formulario_rappi_sc?fechaInicial=2019-11-14&fechaFinal=2019-11-17

    reload(sys)
    sys.setdefaultencoding('utf8')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-rappi')
    gcs_path = 'gs://ct-rappi'
    sub_path = 'formulario_rappi_'
    fecha = time.strftime('%Y-%m-%d')
    output = gcs_path + "/" + sub_path + fecha + ".csv"
    blob = bucket.blob(sub_path + fecha + ".csv")
    dateini = request.args.get('fechaInicial')
    dateend = request.args.get('fechaFinal')
    client = bigquery.Client()
    Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]
    ruta_completa = "/"+ Ruta +"/BI_Archivos/GOOGLE/Rappi/"+ sub_path + fecha +".csv"

    if dateini is None:
        dateini = fecha
        dateinip = str(dateini) + "%20" + "00:00:00"
    else:   
        dateini = dateini
        dateinip = str(dateini) + "%20" + "00:00:00"

    if dateend is None:
        dateend = fecha
        dateendp = str(dateend) + "%20" + "23:59:59"
    else:
        dateend = dateend
        dateendp = str(dateend) + "%20" + "23:59:59"
    
    try:
        os.remove(ruta_completa) #Eliminar de aries
    except: 
        print("Eliminado de aries")
    
    try:
        blob.delete() #Eliminar del storage
    except: 
        print("Eliminado de storage")

    try:
        QUERY2 = ('delete from `Rappi.flujo_react` where substr(fecha,1,10) between ' + '"' + dateini + '"' + ' and "' + dateend + '"')
        query_job = client.query(QUERY2)
        rows2 = query_job.result()
    except: 
        print("Eliminado de bigquery")


    url = "https://webapp.contentobps.com/app/rappi/Desarrollo/back/API.php?token=HUCQHCHMENUNETUP&tipo=ContactoNoEfectivo&fechaInicial="+dateinip+"&fechaFinal="+dateendp
    datos = requests.get(url).content

    if len(requests.get(url).content) < 50:
        return jsonify("No hay datos entre estas fechas: " + (dateini) + " y " + (dateend))
    else:
        i = json.loads(datos)
        file = open(ruta_completa,"a")
        for row in i:
            file.write(
                ((str(row['id']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['fecha']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['accion']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['idLupe']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['idAgente']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['correo']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['telMarcado']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['comentarioTipificacion']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+';'+
                ((str(row['codigoTipificacion']).replace('\r\n','').replace('\n','')).replace('\r','')).encode('utf-8')+"\n")
    
        file.close()
        blob.upload_from_filename(ruta_completa)
        time.sleep(10)
        ejecutar = rappi2_beam.run(output) 
        time.sleep(60)

    # return (url)
    return ("creo el archivo relajado")