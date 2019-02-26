from flask import Blueprint
from flask import jsonify
from flask import request
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.bridge.bridge_beam as bridge_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime

# coding=utf-8

bridge_api = Blueprint('bridge_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@bridge_api.route("/bridge", methods=['GET'])
def prejuridico():

#Parametros GET para modificar la consulta segun los parametros entregados
    table = request.args.get('bdmssql')

    SERVER="192.168.20.63\DELTA"
    USER="DP_USER"
    PASSWORD="Contento2018"
    DATABASE="Contactabilidad"
    TABLE_DB = "dbo." + str(table)
    FECHA_CARGUE = str(datetime.date.today())
    Fecha = datetime.datetime.today().strftime('%Y-%m-%d')    


    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)

    # Insertamos los datos de la nueva consulta equivalentes al mismo dia de la anterior eliminacion
    conn.execute_query("SELECT * FROM " + TABLE_DB)
    # conn.execute_query("SELECT * FROM " + TABLE_DB + " WHERE Fecha >= CAST('2018-12-20' AS DATE)")
    
    cloud_storage_rows = ""
    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += '' + "|" if row['Consecutivo Documento Deudor'].encode('utf-8') == '' else row['Consecutivo Documento Deudor'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Valor Cuota'].encode('utf-8') == '' else row['Valor Cuota'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Clasificacion Producto'].encode('utf-8') == '' else row['Clasificacion Producto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nit'].encode('utf-8') == '' else row['Nit'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombres'].encode('utf-8') == '' else row['Nombres'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Numero Documento'].encode('utf-8') == '' else row['Numero Documento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo De Producto'].encode('utf-8 ') == '' else row['Tipo De Producto'].encode('utf-8 ') + "|"
        text_row += '' + "|" if row['Fecha Pago Cuota'].encode('utf-8') == '' else row['Fecha Pago Cuota'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Modalidad'].encode('utf-8') == '' else row['Modalidad'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre De Producto'].encode('utf-8') == '' else row['Nombre De Producto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Plan'].encode('utf-8') == '' else row['Plan'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha De Perfeccionamiento'].encode('utf-8') == '' else row['Fecha De Perfeccionamiento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Vencimiento Def '].encode('utf-8') == '' else row['Fecha Vencimiento Def '].encode('utf-8') + "|"
        text_row += '' + "|" if row['Numero Cuotas'].encode('utf-8') == '' else row['Numero Cuotas'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cant_oblig'].encode('utf-8') == '' else row['Cant_oblig'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cuotas En Mora'].encode('utf-8') == '' else row['Cuotas En Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dia De Vencimiento De Cuota'].encode('utf-8') == '' else row['Dia De Vencimiento De Cuota'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Valor Obligacion'].encode('utf-8') == '' else row['Valor Obligacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Valor Vencido'].encode('utf-8') == '' else row['Valor Vencido'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Saldo Activo'].encode('utf-8') == '' else row['Saldo Activo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Saldo Orden'].encode('utf-8') == '' else row['Saldo Orden'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Regional'].encode('utf-8') == '' else row['Regional'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ciudad'].encode('utf-8') == '' else row['Ciudad'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Oficina Radicacion'].encode('utf-8') == '' else row['Oficina Radicacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Grabador'].encode('utf-8') == '' else row['Grabador'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre Asesor'].encode('utf-8') == '' else row['Nombre Asesor'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Asesor'].encode('utf-8') == '' else row['Asesor'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre Abogado'].encode('utf-8') == '' else row['Nombre Abogado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Abogado'].encode('utf-8') == '' else row['Abogado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultima Gestion Prejuridica'].encode('utf-8') == '' else row['Fecha Ultima Gestion Prejuridica'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') == '' else row['Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Codigo De Gestion Paralela'].encode('utf-8') == '' else row['Codigo De Gestion Paralela'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') == '' else row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row[33].encode('utf-8') == '' else row[33].encode('utf-8') + "|"
        text_row += '' + "|" if row[34].encode('utf-8') == '' else row[34].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ejec ultima Fecha Actuacion Juridica'].encode('utf-8') == '' else row['Ejec ultima Fecha Actuacion Juridica'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Grabacion Ult Jur'].encode('utf-8') == '' else row['Fecha Grabacion Ult Jur'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultimo Pago'].encode('utf-8') == '' else row['Fecha Ultimo Pago'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ejec ultimo Codigo Gestion Juridico'].encode('utf-8') == '' else row['Ejec ultimo Codigo Gestion Juridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Ultimo Codigo Gestion Juridica'].encode('utf-8') == '' else row['Desc Ultimo Codigo Gestion Juridica'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo De Credito'].encode('utf-8') == '' else row['Tipo De Credito'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo De Cartera'].encode('utf-8') == '' else row['Tipo De Cartera'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dias Mora'].encode('utf-8') == '' else row['Dias Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Calificacion'].encode('utf-8') == '' else row['Calificacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Radicacion'].encode('utf-8') == '' else row['Radicacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Estado De La Obligacion'].encode('utf-8') == '' else row['Estado De La Obligacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fondo Nacional Garantias'].encode('utf-8') == '' else row['Fondo Nacional Garantias'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Region'].encode('utf-8') == '' else row['Region'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Segmento'].encode('utf-8') == '' else row['Segmento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Importacion'].encode('utf-8') == '' else row['Fecha Importacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Universa Titular'].encode('utf-8') == '' else row['Universa Titular'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Negocio Titularizado'].encode('utf-8') == '' else row['Negocio Titularizado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Red'].encode('utf-8') == '' else row['Red'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Traslado Para Cobro'].encode('utf-8') == '' else row['Fecha Traslado Para Cobro'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Calificacion Real'].encode('utf-8') == '' else row['Calificacion Real'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultima Facturacion'].encode('utf-8') == '' else row['Fecha Ultima Facturacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cuadrante'].encode('utf-8') == '' else row['Cuadrante'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Causal'].encode('utf-8') == '' else row['Causal'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Sector Economico'].encode('utf-8') == '' else row['Sector Economico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha De Promesa'].encode('utf-8') == '' else row['Fecha De Promesa'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Endeudamiento'].encode('utf-8') == '' else row['Endeudamiento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Probabilidad de Propension de pago'].encode('utf-8') == '' else row['Probabilidad de Propension de pago'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Grupo de Priorizacion'].encode('utf-8') == '' else row['Grupo de Priorizacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Unicos'].encode('utf-8') == '' else row['Unicos'].encode('utf-8') + "|"
        text_row += '' + "|" if row[64].encode('utf-8') == '' else row[64].encode('utf-8') + "|"
        text_row += '' + "|" if row['Grupo'].encode('utf-8') == '' else row['Grupo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo TC'].encode('utf-8') == '' else row['Tipo TC'].encode('utf-8') + "|"
        text_row += '' + "|" if row['CICLO'].encode('utf-8') == '' else row['CICLO'].encode('utf-8') + "|"
        text_row += '' + "|" if row[68].encode('utf-8') == '' else row[68].encode('utf-8') + "|"
        text_row += '' + "|" if row['Estrategia Cliente'].encode('utf-8') == '' else row['Estrategia Cliente'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Impacto'].encode('utf-8') == '' else row['Impacto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Provisiona'].encode('utf-8') == '' else row['Provisiona'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Rango Mora'].encode('utf-8') == '' else row['Rango Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo de mora'].encode('utf-8') == '' else row['Tipo de mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Rango Gestion'].encode('utf-8') == '' else row['Rango Gestion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Estrategia gestion'].encode('utf-8') == '' else row['Estrategia gestion'].encode('utf-8') + "|"
        text_row += '' + "|" if row[76].encode('utf-8') == '' else row[76].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dia Para Rodar'].encode('utf-8') == '' else row['Dia Para Rodar'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo Base Inicial'].encode('utf-8') == '' else row['Tipo Base Inicial'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Rango desfase'].encode('utf-8') == '' else row['Rango desfase'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultima Gestion Adminfo'].encode('utf-8') == '' else row['Fecha Ultima Gestion Adminfo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Ultimo Codigo De Gestion Adminfo'].encode('utf-8') == '' else row['Desc Ultimo Codigo De Gestion Adminfo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Mora Base Inicio'].encode('utf-8') == '' else row['Mora Base Inicio'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Mora Bini Definitiva'].encode('utf-8') == '' else row['Mora Bini Definitiva'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Base ini Inter'].encode('utf-8') == '' else row['Base ini Inter'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cosecha'].encode('utf-8') == '' else row['Cosecha'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Trimestre'].encode('utf-8') == '' else row['Trimestre'].encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row

    
    filename = FECHA_CARGUE +  ".csv"
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-bridge")

    flowAnswer = bridge_beam.run(table)

# Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-bridge')
    blob = bucket.blob(filename)

    time.sleep(10)
    # Eliminar el archivo en la variable
    blob.delete()
    conn.close()
    return "R, " + flowAnswer