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
import dataflow_pipeline.telefonia.cdr_beam as cdr_beam

cdr_api = Blueprint('cdr_api', __name__)

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

@cdr_api.route("/cdr")
def Ejecutar():
    client = bigquery.Client()
    QUERY = (
        'SELECT servidor, operacion, token, ipdial_code, id_cliente, cartera FROM telefonia.parametros_ipdial')
    query_job = client.query(QUERY)
    rows = query_job.result()
    data = ""
    file = open("/media/BI_Archivos/GOOGLE/Telefonia/cdr.txt","a")
    # file = open("//192.168.20.87/BI_Archivos/GOOGLE/Telefonia/cdr.txt","a") #local debug
    for row in rows:
        url = 'http://' + str(row.servidor) + '/ipdialbox/api_reports.php?token=' + row.token + '&report=' + str("cbps_cdr") + '&date_ini=' + GetDate1 + '&date_end=' + GetDate2
        datos = requests.get(url).content
        if len(requests.get(url).content) < 40:
            continue
        else:
            i = json.loads(datos)
            for rown in i:
                file.write(
                    str(rown["id_call"])+'|'+
                    str(rown["type_call"])+'|'+
                    str(rown["talk_time"])+'|'+
                    str(rown["id_agent"])+'|'+
                    str(rown["agent_name"])+'|'+
                    str(rown["agent_identification"])+'|'+
                    str(rown["skill"])+'|'+
                    str(rown["date"])+'|'+
                    str(rown["hour"])+'|'+
                    str(rown["day_of_week"])+'|'+
                    str(rown["typing_code"])+'|'+
                    str(rown["descri_typing_code"])+'|'+
                    str(rown["typing_code2"])+'|'+
                    str(rown["descri_typing_code2"])+'|'+
                    str(rown["hit"])+'|'+
                    str(rown["telephone_destination"])+'|'+
                    str(rown["telephone_costs"])+'|'+
                    str(rown["telephone_number"])+'|'+
                    str(rown["who_hangs_up"])+'|'+
                    str(rown["customer_identification"])+'|'+
                    str(rown["month"])+'|'+
                    str(rown["screen_recording"])+'|'+
                    str(rown["operation"])+'|'+
                    str(rown["ring"])+'|'+
                    str(rown["abandon"])+'|'+
                    str(row.id_cliente)+"|"+
                    str(row.cartera)+
                    "\n")

                data = (
                    data +
                    str(rown["id_call"])+'|'+
                    str(rown["type_call"])+'|'+
                    str(rown["talk_time"])+'|'+
                    str(rown["id_agent"])+'|'+
                    str(rown["agent_name"])+'|'+
                    str(rown["agent_identification"])+'|'+
                    str(rown["skill"])+'|'+
                    str(rown["date"])+'|'+
                    str(rown["hour"])+'|'+
                    str(rown["day_of_week"])+'|'+
                    str(rown["typing_code"])+'|'+
                    str(rown["descri_typing_code"])+'|'+
                    str(rown["typing_code2"])+'|'+
                    str(rown["descri_typing_code2"])+'|'+
                    str(rown["hit"])+'|'+
                    str(rown["telephone_destination"])+'|'+
                    str(rown["telephone_costs"])+'|'+
                    str(rown["telephone_number"])+'|'+
                    str(rown["who_hangs_up"])+'|'+
                    str(rown["customer_identification"])+'|'+
                    str(rown["month"])+'|'+
                    str(rown["screen_recording"])+'|'+
                    str(rown["operation"])+'|'+
                    str(rown["ring"])+'|'+
                    str(rown["abandon"])+'|'+
                    str(row.id_cliente)+"|"+
                    str(row.cartera))+"\n"
    file.close()
    ejecutar = cdr_beam.run(data)
    os.remove("/media/BI_Archivos/GOOGLE/Telefonia/cdr.txt")
    # os.remove("//192.168.20.87/BI_Archivos/GOOGLE/Telefonia/cdr.txt") #local debug
    return ("Proceso de listamiento de datos: listo ..........................................................." + ejecutar)

########################################################################################################################