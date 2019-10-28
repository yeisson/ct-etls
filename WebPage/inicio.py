from flask import Blueprint
from flask import jsonify, current_app
from flask import request, render_template
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from google.cloud import datastore
from google.cloud import bigquery
from google.cloud import storage
import os
import time
import socket
import datetime
import numpy as np
from array import *
import cgitb
import json
import sys
import requests
import dataflow_pipeline.telefonia.campaign_beam as campaign_beam
from uuid import uuid4

# coding=utf-8

webpage_app = Blueprint('webpage_app', __name__, template_folder = "templates")

@webpage_app.route("/api", methods=['POST','GET'])
def home():

    # client = bigquery.Client()
    # QUERY = (
    #     'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera FROM telefonia.parametros_ipdial WHERE estado = "Activado" order by 4 asc') #WHERE ipdial_code = "intcob-unisabaneta"
    # query_job = client.query(QUERY)
    # rows = query_job.result()

    # datos = np.array([])

    # for row in rows:
    #     datos = np.append(datos, row[3])

    # return render_template('index.html', x = datos)
    return render_template('index.html')



@webpage_app.route("/procesador", methods=['POST','GET'])
def procesador():

    reload(sys)
    sys.setdefaultencoding('utf8')
    client = bigquery.Client()
    fecha = time.strftime('%Y%m%d')
    hora = time.strftime('%H-%M')
    Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-telefonia')
    gcs_path = 'gs://ct-telefonia'
    sub_path = 'campaign/'    
    operacion = request.form['operacion']
    output = gcs_path + "/" + sub_path + fecha + "_" + operacion +".csv"
    tipo_campana = request.form['tipo_campana']
    id_campana = request.form['id_campana']
    dateinip = request.form['dateini'] + '00000000'
    dateini = dateinip.replace("-","")
    dateendp = request.form['dateend'] + '23595900'
    dateend = dateendp.replace("-","")
    ruta_completa = "/"+ Ruta +"/BI_Archivos/GOOGLE/Telefonia/campaign/" + "campana_" + fecha + "_" + operacion +".csv"
    blob = bucket.blob(sub_path + fecha + "_" + operacion + ".csv")
    rand_token = uuid4()

    
    try:
        os.remove(ruta_completa) #Eliminar de aries
    except: 
        print("Eliminado de aries")
    
    try:
        blob.delete() #Eliminar del storage
    except: 
        print("Eliminado de storage")

    # try:
    #     QUERY2 = ('delete FROM `contento-bi.telefonia.campaign` where ipdial_code = ' + '"' + operacion + '"' + 'and fecha_cargue = '+ '"' + fecha + '"' + 'and hora = '+ '"' + hora + '"')
    #     query_job = client.query(QUERY2)
    #     rows2 = query_job.result()
    # except: 
    #     print("Eliminado de bigquery")


    QUERY = (
        'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera FROM telefonia.parametros_ipdial  WHERE ipdial_code = "' + operacion + '"')
    query_job = client.query(QUERY)
    rows = query_job.result()


    file = open(ruta_completa,"a")
    for row in rows:
        servidor = str(row[0])
        operacion = str(row[1])
        token =  str(row[2])
        ipdial_code = str(row[3])
        id_cliente = str(row[4])
        cartera = str(row[5])
        url = 'http://' + servidor + '/ipdialbox/api_campaing.php?token=' + token + '&action=detail_json&type_campaing=' + tipo_campana + '&campaing='+ id_campana + '&date_ini=' + dateini + '&date_end=' + dateend
        datos = requests.get(url).content

        i = json.loads(datos)
        for rown in i:
            file.write(
                str(rown[0]['nombre_cliente']) +";"+ 
                str(rown[0]['apellido_cliente']).decode('utf-8') +";"+
                str(rown[0]['tipo_doc']).encode('utf-8') +";"+
                str(rown[0]['id_cliente']).encode('utf-8') +";"+
                str(rown[0]['sexo']).encode('utf-8') +";"+
                str(rown[0]['pais']).encode('utf-8') +";"+
                str(rown[0]['departamento']).encode('utf-8') +";"+
                str(rown[0]['ciudad']).encode('utf-8') +";"+
                str(rown[0]['zona']).encode('utf-8') +";"+
                str(rown[0]['direccion']).encode('utf-8') +";"+
                str(rown[0]['opt1']).encode('utf-8') +";"+
                str(rown[0]['opt2']).encode('utf-8') +";"+
                str(rown[0]['opt3']).encode('utf-8') +";"+
                str(rown[0]['opt4']).encode('utf-8') +";"+
                str(rown[0]['opt5']).encode('utf-8') +";"+
                str(rown[0]['opt6']).encode('utf-8') +";"+
                str(rown[0]['opt7']).encode('utf-8') +";"+
                str(rown[0]['opt8']).encode('utf-8') +";"+
                str(rown[0]['opt9']).encode('utf-8') +";"+
                str(rown[0]['opt10']).encode('utf-8') +";"+
                str(rown[0]['opt11']).encode('utf-8') +";"+
                str(rown[0]['opt12']).encode('utf-8') +";"+
                str(rown[0]['tel1']).encode('utf-8') +";"+
                str(rown[0]['tel2']).encode('utf-8') +";"+
                str(rown[0]['tel3']).encode('utf-8') +";"+
                str(rown[0]['tel4']).encode('utf-8') +";"+
                str(rown[0]['tel5']).encode('utf-8') +";"+
                str(rown[0]['tel6']).encode('utf-8') +";"+
                str(rown[0]['tel7']).encode('utf-8') +";"+
                str(rown[0]['tel8']).encode('utf-8') +";"+
                str(rown[0]['tel9']).encode('utf-8') +";"+
                str(rown[0]['tel10']).encode('utf-8') +";"+
                str(rown[0]['tel_extra']).encode('utf-8') +";"+
                str(rown[0]['id_agent']).encode('utf-8') +";"+
                str(rown[0]['fecha']).encode('utf-8') +";"+
                str(rown[0]['llamadas']).encode('utf-8') +";"+
                str(rown[0]['id_call']).encode('utf-8') +";"+
                str(rown[0]['rellamada']).encode('utf-8') +";"+
                str(rown[0]['resultado']).encode('utf-8') +";"+
                str(rown[0]['cod_rslt1']).encode('utf-8') +";"+
                str(rown[0]['cod_rslt2']).encode('utf-8') +";"+
                str(rown[0]['rellamada_count']).encode('utf-8') +";"+
                str(row.id_cliente)+";"+
                str(row.ipdial_code)+";"+
                str(rand_token)+";"+
                str(hora)+";"+
                str(id_campana)+";"+
                str(fecha) + "\n")

    print url
    file.close()
    blob.upload_from_filename(ruta_completa)
    ejecutar = campaign_beam.run(output)
    time.sleep(30)

    return validador(ipdial_code, fecha, rand_token, id_campana)




@webpage_app.route("/validador", methods=['POST','GET'])
def validador(ipdial_code, fecha, rand_token, id_campana):

    reload(sys)
    sys.setdefaultencoding('utf8')
    client = bigquery.Client()
    storage_client = storage.Client()

    QUERY2 = (
        'SELECT id_campana,\
            SUM(answer) AS answer,\
            SUM(clean) AS clean,\
            SUM(answer_machine) AS answer_machine,\
            SUM(no_answer) AS no_answer,\
            SUM(abandon) AS abandon,\
            SUM(failed) AS failed,\
            SUM(busy) AS busy\
        FROM(\
        SELECT \
            distinct(id_campana),\
            CASE WHEN resultado = "ANSWER" THEN sum(1) ELSE 0 END AS answer,\
            CASE WHEN resultado = "CLEAN" THEN sum(1) ELSE 0 END AS clean,\
            CASE WHEN resultado = "ANSWER-MACHINE" THEN sum(1) ELSE 0 END AS answer_machine,\
            CASE WHEN resultado = "NO-ANSWER" THEN sum(1) ELSE 0 END AS no_answer,\
            CASE WHEN resultado = "ABANDON" THEN sum(1) ELSE 0 END AS abandon,\
            CASE WHEN resultado = "FAILED" THEN sum(1) ELSE 0 END AS failed,\
            CASE WHEN resultado = "BUSY" THEN sum(1) ELSE 0 END AS busy\
        FROM `telefonia.campaign`\
        where ipdial_code = '+ "'" + ipdial_code + "'"  +" and rand_token = '" + str(rand_token) +  "'"'\
        GROUP BY id_campana, resultado\
        )\
        GROUP BY id_campana')

    query_job = client.query(QUERY2)
    rows2 = query_job.result()


    id_campanat = np.array([])
    answer = np.array([])
    clean = np.array([])
    answer_machine = np.array([])
    no_answer = np.array([])
    abandon = np.array([])
    failed = np.array([])
    busy = np.array([])

    for row in rows2:
        id_campanat = np.append(id_campanat, row[0])
        answer = np.append(answer, row[1])
        clean = np.append(clean, row[2])
        answer_machine = np.append(answer_machine, row[3])
        no_answer = np.append(no_answer, row[4])
        abandon = np.append(abandon, row[5])
        failed = np.append(failed, row[6])
        busy = np.append(busy, row[7])
        

    return render_template('salida.html', campaign = id_campanat, a = answer, b = clean, c = answer_machine, d = no_answer, e = abandon, f = failed, g = busy)