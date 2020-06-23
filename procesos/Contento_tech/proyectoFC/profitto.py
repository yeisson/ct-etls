# -*- coding: utf-8 -*-
#########################################################################################################################################
#Espiritu santo de Dios, que sean tus manos creando este codigo. En el nombre del padre todo poderoso AMEN y AMEN.
#########################################################################################################################################


from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.contento_tech.proyectoFC_bdc_beam as proyectoFC_bdc_beam
import dataflow_pipeline.contento_tech.proyectoFC_bdf_beam as proyectoFC_bdf_beam
import dataflow_pipeline.contento_tech.proyectoFC_cxp_beam as proyectoFC_cxp_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import sys
import smtplib

#coding: utf-8 

profitto_api = Blueprint('profitto_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]
fecha = time.strftime('%H')
if (int(fecha) < 12):
    horario = 'Buenos días'
elif (int(fecha) < 19):
    horario = 'Buenas Tardes'
else:
    horario = 'Buenas Noches'

@profitto_api.route("/start")
def profitto():

    local_route = fileserver_baseroute + "/BI_Archivos/TECH/ProyectoFC/"
    archivos = os.listdir(local_route)
    tokened = 'contento2020**.'
    checking = []
    
    for archivo in archivos:
        if archivo.startswith("bdc_"):
            mifecha = archivo[4:12]
            storage_client = storage.Client()
            client = bigquery.Client()
            bucket = storage_client.get_bucket('ct-tech-tof')
            blob = bucket.blob('profitto/' + archivo)
            
            try:
                blob.delete()
            except: 
                print("Eliminado de storage")
            try:
                QUERY = ('delete FROM `contento-bi.Contento_Tech.profitto_bd_carteras` where 1 = 1')
                query_job = client.query(QUERY)
            except: 
                print("Eliminado de bigquery")
            
            blob.upload_from_filename(local_route + archivo)

            mensaje = proyectoFC_bdc_beam.run('gs://ct-tech-tof/profitto/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                os.remove(local_route + archivo)
                checking += ['bdc->Procesado,']
                try:
                    message = horario + '\n\n' +'El archivo: ' + archivo + '. Se procesó correctamente.' + '\n\n\n\n\n\n' + 'Contento Tech'
                    subject = 'Info-process-etl-python(google cloud platform)'
                    message = ('Subject: {}\n\n{}'.format(subject, message))
                    server = smtplib.SMTP('smtp.gmail.com', 587)
                    server.starttls()
                    server.login('techcontentbisup@gmail.com',tokened)
                    server.sendmail('NO-REPLY@gmail.com','arave@contentobps.com',message)
                    server.quit()
                except:
                    print("No se pudo enviar EMAIL DE VERIFICACIÓN bdc")
            else: checking += ['bdc->NO Procesado,']


        if archivo.startswith("bdf_"):
            mifecha = archivo[4:12]
            storage_client = storage.Client()
            client = bigquery.Client()
            bucket = storage_client.get_bucket('ct-tech-tof')
            blob = bucket.blob('profitto/' + archivo)
            
            try:
                blob.delete()
            except: 
                print("Eliminado de storage")
            try:
                QUERY = ('delete FROM `contento-bi.Contento_Tech.profitto_bd_factura` where 1 = 1')
                query_job = client.query(QUERY)
            except: 
                print("Eliminado de bigquery")

            blob.upload_from_filename(local_route + archivo)

            mensaje = proyectoFC_bdf_beam.run('gs://ct-tech-tof/profitto/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                os.remove(local_route + archivo)
                checking += ['bdf->Procesado,']
                try:
                    message = horario + '\n\n' +'El archivo: ' + archivo + '. Se procesó correctamente.' + '\n\n\n\n\n\n' + 'Contento Tech'
                    subject = 'Info-process-etl-python(google cloud platform)'
                    message = ('Subject: {}\n\n{}'.format(subject, message))
                    server = smtplib.SMTP('smtp.gmail.com', 587)
                    server.starttls()
                    server.login('techcontentbisup@gmail.com',tokened)
                    server.sendmail('NO-REPLY@gmail.com','arave@contentobps.com',message)
                    server.quit()
                except:
                    print("No se pudo enviar EMAIL DE VERIFICACIÓN bdf")
            else: checking += ['bdf->NO Procesado,']

        if archivo.startswith("cxp_"):
            mifecha = archivo[4:12]
            storage_client = storage.Client()
            client = bigquery.Client()
            bucket = storage_client.get_bucket('ct-tech-tof')
            blob = bucket.blob('profitto/' + archivo)
            
            try:
                blob.delete()
            except: 
                print("Eliminado de storage")
            try:
                QUERY = ('delete FROM `contento-bi.Contento_Tech.profitto_CuentasxPagar` where 1 = 1')
                query_job = client.query(QUERY)
            except: 
                print("Eliminado de bigquery")

            blob.upload_from_filename(local_route + archivo)

            mensaje = proyectoFC_cxp_beam.run('gs://ct-tech-tof/profitto/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                os.remove(local_route + archivo)
                checking += ['cxp->Procesado']
                try:
                    message = horario + '\n\n' +'El archivo: ' + archivo + '. Se procesó correctamente.' + '\n\n\n\n\n\n' + 'Contento Tech'
                    subject = 'Info-process-etl-python(google cloud platform)'
                    message = ('Subject: {}\n\n{}'.format(subject, message))
                    server = smtplib.SMTP('smtp.gmail.com', 587)
                    server.starttls()
                    server.login('techcontentbisup@gmail.com',tokened)
                    server.sendmail('NO-REPLY@gmail.com','arave@contentobps.com, arave163@gmail.com',message)
                    server.quit()
                except: 
                    print("No se pudo enviar EMAIL DE VERIFICACIÓN cxp")
                
            else: checking += ['cxp->NO Procesado,']

    if not checking:
        response = {}
        response["code"] = 400
        response["description"] = "No se encontraron ficheros"
        response["status"] = False
        return jsonify(response)
    else:
        return jsonify(checking)