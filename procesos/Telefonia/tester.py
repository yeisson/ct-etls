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

tester_api = Blueprint('tester_api', __name__)
########################### DEFINICION DE VARIABLES ###########################
fecha = time.strftime('%Y%m%d')
hour1 = "000000"
hour2 = "235959"

GetDate1 = time.strftime('%Y%m%d')+str(hour1)
GetDate2 = time.strftime('%Y%m%d')+str(hour2)

Ruta = ("/192.168.20.87", "media")[socket.gethostname()=="contentobi"]
KEY_REPORT = "tester_ipdial"
CODE_REPORT = "login_time"
fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

########################### CODIGO #####################################################################################

@tester_api.route("/" + KEY_REPORT)
def Ejecutar():

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
        'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera, Estado FROM telefonia.parametros_ipdial') # where Estado = "Activado"
    query_job = client.query(QUERY)
    rows = query_job.result()
    link = ""
    i = 0
    summerize = ""

    for row in rows:
        i = i+1
        url = 'http://' + str(row.servidor) + '/ipdialbox/api_reports.php?token=' + row.token + '&report=' + str(CODE_REPORT) + '&date_ini=' + dateini + '&date_end=' + dateend
        datos = requests.get(url).content
        summerize = datos[1:42]

        # if len(requests.get(url).content) <= 50 and row.Estado == "Activado":
        #     QUERY = (
        #         'UPDATE telefonia.parametros_ipdial SET Estado = "Desactivado" where token = ' + '"' + row.token + '"')
        #     query_job = client.query(QUERY)

        # elif len(requests.get(url).content) >= 50 and row.Estado == "Desactivado":
        #     QUERY = (
        #         'UPDATE telefonia.parametros_ipdial SET Estado = "Activado" where token = ' + '"' + row.token + '"')
        #     query_job = client.query(QUERY)

        link += str(i) + "-->  " + row.ipdial_code + " - " + row.servidor + " - " + row.token + " - <b>" + row.Estado + '</b>'+ summerize +'<br>'
 
    return (link) 
