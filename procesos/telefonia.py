
from __future__ import print_function, absolute_import

from flask import Flask, request, jsonify, redirect
from flask_cors import CORS
from flask import Blueprint
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from google.cloud import datastore
from google.cloud import bigquery
import logging
import uuid
import json
import urllib3
import requests
import os
import dataflow_pipeline.massive as pipeline
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import datetime
import dataflow_pipeline.telefonia.telefonia_beam as telefonia_beam


telefonia_api = Blueprint('telefonia_api', __name__)


hoy = datetime.datetime.now()
ayer = datetime.datetime.today() - datetime.timedelta(days = 1)
ano = ayer.year 
mes = ayer.month
dia = ayer.day
hour1 = "060000"
hour2 = "235959"
GetDate1 = str(ano)+str(mes)+str(dia)+str(hour1)
GetDate2 = str(ano)+str(mes)+str(dia)+str(hour2)

@telefonia_api.route("/telefonia")
def hibrido():
    client = bigquery.Client()
    QUERY = (
        'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera FROM telefonia.parametros_ipdial')
    query_job = client.query(QUERY)
    rows = query_job.result()
    data = "["
    file = open("archivos\Telefonia\data.txt","a")

    for row in rows:
        url = 'http://' + str(row.servidor) + '/ipdialbox/api_reports.php?token=' + row.token + '&report=' + str("login_time") + '&date_ini=' + GetDate1 + '&date_end=' + GetDate2
        datos = requests.get(url).content
        
        if len(requests.get(url).content) < 40:
            continue
        else:
            i = json.loads(datos)
            for rown in i:
                file.write(str(rown["date"])+"|"+str(rown["agent"])+"|"+str(rown["identification"])+"|"+str(rown["login_date"])+"|"+str(rown["logout_date"])+"|"+str(rown["login_time"])+"|"+str(row.ipdial_code) + "|" + str(row.id_cliente) + "|" + str(row.cartera) + "\n")
                data = data + "'" + str(rown["date"])+"|"+str(rown["agent"])+"|"+str(rown["identification"])+"|"+str(rown["login_date"])+"|"+str(rown["logout_date"])+"|"+str(rown["login_time"])+"|"+ str(row.ipdial_code) + "|" + str(row.id_cliente) + "|" + str(row.cartera) + "'," + "\n"
    file.close()
    data = data + "]"
    # ejecutar = telefonia_beam.run(data)
    return ("Llegando " + "ejecutar" + "... Jueputa temazo!!!!")
    