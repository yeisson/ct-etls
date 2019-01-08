####################################################################################################
##                               REPORTE DE TELEFONIA = LOGIN-LOGOUT                              ##
####################################################################################################



######################## INDICE ##############################

# FILA.7..................... INDICE
# FILA.18.................... LIBRERIAS
# FILA.45.................... VARIABLES GLOBALES
# FILA.67.................... CODIGO DE EJECUCION

##############################################################



########################### LIBRERIAS #####################################

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
import dataflow_pipeline.telefonia.agent_status_beam as agent_status_beam

agent_status_api = Blueprint('agent_status_api', __name__)

#############################################################################



########################### DEFINICION DE TIEMPOS ###########################

hoy = datetime.datetime.now()
ayer = datetime.datetime.today() - datetime.timedelta(days = 1)
ano = hoy.year
hour1 = "060000"
hour2 = "235959"
if len(str(ayer.day)) == 1:
    dia = "0" + str(ayer.day)
else:
    dia = ayer.day

if len(str(ayer.month)) == 1:
    mes = "0"+ str(ayer.now().month)
else:
    mes = ayer.month

GetDate1 = str(ano)+str(mes)+str(dia)+str(hour1)
GetDate2 = str(ano)+str(mes)+str(dia)+str(hour2)
# GetDate1 = "20181214"+str(hour1)
# GetDate2 = "20181231"+str(hour2)
#############################################################################


########################### CODIGO #####################################################################################

@agent_status_api.route("/agent_status")
def Ejecutar():
    client = bigquery.Client()
    QUERY = (
        'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera FROM telefonia.parametros_ipdial')
    query_job = client.query(QUERY)
    rows = query_job.result()
    data = ""
    file = open("//192.168.20.87/BI_Archivos/GOOGLE/Telefonia/agent_status.txt","a")
    for row in rows:
        url = 'http://' + str(row.servidor) + '/ipdialbox/api_reports.php?token=' + row.token + '&report=' + str("cbps_satustime") + '&date_ini=' + GetDate1 + '&date_end=' + GetDate2
        datos = requests.get(url).content
        if len(requests.get(url).content) < 40:
            continue
        else:
            i = json.loads(datos)
            for rown in i:
                file.write(
                    str(rown["operation"])+";"+
                    str(rown["date"])+";"+
                    str(rown["hour"])+";"+
                    str(rown["id_agent"])+";"+
                    str(rown["agent_identification"])+";"+
                    rown["agent_name"].encode('utf-8')+";"+
                    str(rown["CALLS"])+";"+
                    str(rown["CALLS INBOUND"])+";"+
                    str(rown["CALLS OUTBOUND"])+";"+
                    str(rown["CALLS INTERNAL"])+";"+
                    str(rown["READY TIME"])+";"+
                    str(rown["INBOUND TIME"])+";"+
                    str(rown["OUTBOUND TIME"])+";"+
                    str(rown["NOT-READY TIME"])+";"+
                    str(rown["RING TIME"])+";"+
                    str(rown["LOGIN TIME"])+";"+
                    str(rown["AHT"])+";"+
                    rown["OCUPANCY"].encode('utf-8')+";"+
                    str(rown["AUX TIME"])+";"+
                    str(row.id_cliente)+";"+
                    str(row.cartera)+
                    "\n")

                data = (
                    data +
                    str(rown["operation"])+";"+
                    str(rown["date"])+";"+
                    str(rown["hour"])+";"+
                    str(rown["id_agent"])+";"+
                    str(rown["agent_identification"])+";"+
                    rown["agent_name"].encode('utf-8')+";"+
                    str(rown["CALLS"])+";"+
                    str(rown["CALLS INBOUND"])+";"+
                    str(rown["CALLS OUTBOUND"])+";"+
                    str(rown["CALLS INTERNAL"])+";"+
                    str(rown["READY TIME"])+";"+
                    str(rown["INBOUND TIME"])+";"+
                    str(rown["OUTBOUND TIME"])+";"+
                    str(rown["NOT-READY TIME"])+";"+
                    str(rown["RING TIME"])+";"+
                    str(rown["LOGIN TIME"])+";"+
                    str(rown["AHT"])+";"+
                    rown["OCUPANCY"].encode('utf-8')+";"+
                    str(rown["AUX TIME"])+";"+
                    str(row.id_cliente)+";"+
                    str(row.cartera))+"\n"
    file.close()
    ejecutar = agent_status_beam.run()
    os.remove("//192.168.20.87/BI_Archivos/GOOGLE/Telefonia/agent_status.txt")
    return ("Proceso de listamiento de datos: listo ..........................................................." + ejecutar)

########################################################################################################################