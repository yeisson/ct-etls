# -*- coding: utf-8 -*-
#######################################################################################################################
#Espíritu santo de DIOS, que sean tus manos tirando este código. en el nombre de JESÚS. amén y amén
#######################################################################################################################

from flask import Blueprint
from flask import jsonify
from flask import request
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from google.cloud import datastore
from google.cloud import bigquery
import os
import socket
import datetime
import time
import json
import sys
import requests

bancolombia_api2 = Blueprint('bancolombia_api2', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


##URL DE INVOCACIÓN:
# http://contentobps.contentobi.com:5000/bancolombia_adm_api/api
# PARÁMETROS:
# cedula = número de consecutivo de deudor en ADMINFO
# token = solo puede acceder al api quién tenga el TOKEN de autorización

ip_allowed = []
client = bigquery.Client()
QUERY = ('SELECT * FROM `Contento_Tech.ip_allowed`')
query_job = client.query(QUERY)
rows = query_job.result()
for row in rows:
    ip_allowed.append(row[1])

@bancolombia_api2.route("/api", methods=['POST','GET'])
def api():

    client = bigquery.Client()
    fecha = time.strftime('%Y%m%d')
    token= request.args.get('token')
    tokenq = "AFRV786989182391827898-2312"
    id_cliente = request.args.get('cedula')
    ip = request.remote_addr
    mensaje_ip_no_autorizada = " No está autorizada para ingresar a esta API"
    token_incorrecto = "Ingrese un token válido"


    if ip not in ip_allowed:
        return ("La ip: " + ip + mensaje_ip_no_autorizada)

    if id_cliente is None:
        return("Por favor ingrese una cédula")
    else:
        queryt = "FROM `contento-bi.bancolombia_admin.vista_escritorio_unico` \
                    WHERE NIT = '" + id_cliente + "' \
                    ORDER BY 1,2 DESC"

    if token <> tokenq:
        return(token_incorrecto)
    else:        
        QUERY = ('SELECT * ' + queryt)
        query_job = client.query(QUERY)
        rows = query_job.result()

        items = []
        i = 0
        for row in rows:
            i = i+1
            items.append({
                'nit': row[0],
                'nombres': row[1].encode('utf-8'),
                'Ciudad': row[2],
                'Departamento': row[3],
                'Telefono': row[4],
                'obligaciones': row[5],
                'Consecutivo_Documento_Deudor': row[6],
                'nro_tarjeta': row[7],
                'valor_obligacion': row[8],
                'valor_vencido': row[9],
                'dias_mora': row[10],
                'pago_minimo': row[11],
                'Saldo_Capital_Dolares': row[12],
                'Fecha_de_Pago': row[13],
                'Valor_Pagado': row[14],
                'ciclo': row[15],
                '#Registro': i
                })

    
    return  jsonify(items)


##URL DE INVOCACIÓN:
# http://contentobps.contentobi.com:5000/bancolombia_adm_api/api2
# PARÁMETROS:
# cedula = número de consecutivo de deudor en ADMINFO
# token = solo puede acceder al api quién tenga el TOKEN de autorización


@bancolombia_api2.route("/api2", methods=['POST','GET'])
def api2():

    client = bigquery.Client()
    fecha = time.strftime('%Y%m%d')
    # dinip= request.args.get('dateini')
    # dendp= request.args.get('dateend')
    token= request.args.get('token')
    tokenq = "AFRV786983123123122123-0128"
    id_cliente = request.args.get('cedula')
    ip = request.remote_addr
    mensaje_ip_no_autorizada = " No está autorizada para ingresar a esta API"
    token_incorrecto = "Ingrese un token válido"


    if ip not in ip_allowed:
        return ("La ip:" + ip + mensaje_ip_no_autorizada)

    if id_cliente is None:
        return("Por favor ingrese una cédula")
    else:
        queryt = "FROM \
                    (SELECT fecha_gestion, nit, grabador, codigo_abogado, nota, codigo_de_gestion, desc_ultimo_codigo_de_gestion_prejuridico, nro_documento,\
                    ROW_NUMBER() OVER(PARTITION BY SUBSTR(fecha_gestion,0,16), nit, grabador, codigo_abogado, nota, codigo_de_gestion, desc_ultimo_codigo_de_gestion_prejuridico order by fecha_gestion  desc) AS RANK_GESTIONES_1\
                    FROM `bancolombia_admin.seguimiento`\
                    WHERE NIT = '" + id_cliente + "') WHERE RANK_GESTIONES_1 = 1 order by 1 desc, 2 LIMIT 10"

    if token <> tokenq:
        return(token_incorrecto)
    else:        
        QUERY = ('SELECT * ' + queryt)
        query_job = client.query(QUERY)
        rows = query_job.result()

        items = []
        i = 0
        for row in rows:
            i = i+1
            items.append({
                'fecha_gestion': row[0],
                'nit': row[1],
                'grabador': row[2],
                'codigo_abogado': row[3],
                'nota': row[4].replace("\n",""),
                'codigo_de_gestion': row[5],
                'desc_ultimo_codigo_de_gestion_prejuridico': row[6],
                'nro_documento': row[7],
                '#Registro': i
                })

    
    return  jsonify(items)



    ##URL DE INVOCACIÓN:
    # http://contentobps.contentobi.com:5000/bancolombia_adm_api/api3
    # PARÁMETROS:
    # cedula = número de consecutivo de deudor en ADMINFO
    # token = solo puede acceder al api quién tenga el TOKEN de autorización

@bancolombia_api2.route("/api3", methods=['POST','GET'])
def api3():

    client = bigquery.Client()
    fecha = time.strftime('%Y%m%d')
    token= request.args.get('token')
    tokenq = "AFRV5bf68a2c0ad6a7e08bd6966968ea-46de"
    id_cliente = request.args.get('cedula')
    ip = request.remote_addr
    mensaje_ip_no_autorizada = " No está autorizada para ingresar a esta API"
    token_incorrecto = "Ingrese un token válido"


    if ip not in ip_allowed:
        return ("La ip:" + ip + mensaje_ip_no_autorizada)

    if id_cliente is None:
        return("Por favor ingrese una cédula")
    else:
        queryt = "FROM \
                    (SELECT * FROM `bancolombia_admin.vista_escritorio_unico_pagos` \
                    WHERE NIT = " + id_cliente + ")"

    if token <> tokenq:
        return(token_incorrecto)
    else:        
        QUERY = ('SELECT * ' + queryt)
        query_job = client.query(QUERY)
        rows = query_job.result()

        items = []
        i = 0
        for row in rows:
            i = i+1
            items.append({
                'Nit': row[0],
                'Consecutivo': row[1],
                'Fecha_de_Pago': row[2].strftime('%Y-%m-%d'),
                'Valor_Pagado': row[3],
                '#Registro ': i
                })

    
    return  jsonify(items)