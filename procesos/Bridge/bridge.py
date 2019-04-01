from flask import Blueprint
from flask import jsonify
from flask import request
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.bridge.bridge_beam as bridge_beam
import dataflow_pipeline.bridge.bridge_beam2 as bridge_beam2
import dataflow_pipeline.bridge.bridge_beam3 as bridge_beam3
import dataflow_pipeline.bridge.bridge_beam4 as bridge_beam4
import dataflow_pipeline.bridge.bridge_beam5 as bridge_beam5
import dataflow_pipeline.bridge.bridge_beam6 as bridge_beam6
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime

# coding=utf-8

bridge_api = Blueprint('bridge_api', __name__)
fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]



#####################################################################################################################################
##################################################### BD_CONSOLIDADO ################################################################
#####################################################################################################################################
#####################################################################################################################################



@bridge_api.route("/bridge", methods=['GET'])
def bridge():

#Parametros GET para modificar la consulta segun los parametros entregados
    table = request.args.get('bdmssql')
    timer = request.args.get('time')
    if timer is None:
        timer = 600
    elif timer == "":
        timer = 600
    else: 
        timer

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
        text_row += '' + "|" if row['Consecutivo Documento Deudor'].encode('utf-8') is None else row['Consecutivo Documento Deudor'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Valor Cuota'].encode('utf-8') is None else row['Valor Cuota'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Clasificacion Producto'].encode('utf-8') is None else row['Clasificacion Producto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nit'].encode('utf-8') is None else row['Nit'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombres'].encode('utf-8') is None else row['Nombres'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Numero Documento'].encode('utf-8') is None else row['Numero Documento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo De Producto'].encode('utf-8 ') is None else row['Tipo De Producto'].encode('utf-8 ') + "|"
        text_row += '' + "|" if row['Fecha Pago Cuota'].encode('utf-8') is None else row['Fecha Pago Cuota'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Modalidad'].encode('utf-8') is None else row['Modalidad'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre De Producto'].encode('utf-8') is None else row['Nombre De Producto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Plan'].encode('utf-8') is None else row['Plan'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha De Perfeccionamiento'].encode('utf-8') is None else row['Fecha De Perfeccionamiento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Vencimiento Def '].encode('utf-8') is None else row['Fecha Vencimiento Def '].encode('utf-8') + "|"
        text_row += '' + "|" if row['Numero Cuotas'].encode('utf-8') is None else row['Numero Cuotas'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cant_oblig'].encode('utf-8') is None else row['Cant_oblig'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cuotas En Mora'].encode('utf-8') is None else row['Cuotas En Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dia De Vencimiento De Cuota'].encode('utf-8') is None else row['Dia De Vencimiento De Cuota'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Valor Obligacion'].encode('utf-8') is None else row['Valor Obligacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Valor Vencido'].encode('utf-8') is None else row['Valor Vencido'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Saldo Activo'].encode('utf-8') is None else row['Saldo Activo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Saldo Orden'].encode('utf-8') is None else row['Saldo Orden'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Regional'].encode('utf-8') is None else row['Regional'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ciudad'].encode('utf-8') is None else row['Ciudad'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Oficina Radicacion'].encode('utf-8') is None else row['Oficina Radicacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Grabador'].encode('utf-8') is None else row['Grabador'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre Asesor'].encode('utf-8') is None else row['Nombre Asesor'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Asesor'].encode('utf-8') is None else row['Asesor'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre Abogado'].encode('utf-8') is None else row['Nombre Abogado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Abogado'].encode('utf-8') is None else row['Abogado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultima Gestion Prejuridica'].encode('utf-8') is None else row['Fecha Ultima Gestion Prejuridica'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') is None else row['Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Codigo De Gestion Paralela'].encode('utf-8') is None else row['Codigo De Gestion Paralela'].encode('utf-8') + "|"
        text_row += '' + "|" if row[32].encode('utf-8') is None else row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row[33].encode('utf-8') is None else row[33].encode('utf-8') + "|"
        text_row += '' + "|" if row[34].encode('utf-8') is None else row[34].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ejec ultima Fecha Actuacion Juridica'].encode('utf-8') is None else row['Ejec ultima Fecha Actuacion Juridica'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Grabacion Ult Jur'].encode('utf-8') is None else row['Fecha Grabacion Ult Jur'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultimo Pago'].encode('utf-8') is None else row['Fecha Ultimo Pago'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ejec ultimo Codigo Gestion Juridico'].encode('utf-8') is None else row['Ejec ultimo Codigo Gestion Juridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Ultimo Codigo Gestion Juridica'].encode('utf-8') is None else row['Desc Ultimo Codigo Gestion Juridica'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo De Credito'].encode('utf-8') is None else row['Tipo De Credito'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo De Cartera'].encode('utf-8') is None else row['Tipo De Cartera'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dias Mora'].encode('utf-8') is None else row['Dias Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Calificacion'].encode('utf-8') is None else row['Calificacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Radicacion'].encode('utf-8') is None else row['Radicacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Estado De La Obligacion'].encode('utf-8') is None else row['Estado De La Obligacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fondo Nacional Garantias'].encode('utf-8') is None else row['Fondo Nacional Garantias'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Region'].encode('utf-8') is None else row['Region'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Segmento'].encode('utf-8') is None else row['Segmento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Importacion'].encode('utf-8') is None else row['Fecha Importacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Universa Titular'].encode('utf-8') is None else row['Universa Titular'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Negocio Titularizado'].encode('utf-8') is None else row['Negocio Titularizado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Red'].encode('utf-8') is None else row['Red'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Traslado Para Cobro'].encode('utf-8') is None else row['Fecha Traslado Para Cobro'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Calificacion Real'].encode('utf-8') is None else row['Calificacion Real'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultima Facturacion'].encode('utf-8') is None else row['Fecha Ultima Facturacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cuadrante'].encode('utf-8') is None else row['Cuadrante'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Causal'].encode('utf-8') is None else row['Causal'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Sector Economico'].encode('utf-8') is None else row['Sector Economico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha De Promesa'].encode('utf-8') is None else row['Fecha De Promesa'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Endeudamiento'].encode('utf-8') is None else row['Endeudamiento'].encode('utf-8') + "|"
        text_row += '' + "|" if row[61].encode('utf-8') is None else row[61].encode('utf-8') + "|"
        text_row += '' + "|" if row[62].encode('utf-8') is None else row[62].encode('utf-8') + "|"
        text_row += '' + "|" if row['Unicos'].encode('utf-8') is None else row['Unicos'].encode('utf-8') + "|"
        text_row += '' + "|" if row[64].encode('utf-8') is None else row[64].encode('utf-8') + "|"
        text_row += '' + "|" if row['Grupo'].encode('utf-8') is None else row['Grupo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo TC'].encode('utf-8') is None else row['Tipo TC'].encode('utf-8') + "|"
        text_row += '' + "|" if row['CICLO'].encode('utf-8') is None else row['CICLO'].encode('utf-8') + "|"
        text_row += '' + "|" if row[68].encode('utf-8') is None else row[68].encode('utf-8') + "|"
        text_row += '' + "|" if row['Estrategia Cliente'].encode('utf-8') is None else row['Estrategia Cliente'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Impacto'].encode('utf-8') is None else row['Impacto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Provisiona'].encode('utf-8') is None else row['Provisiona'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Rango Mora'].encode('utf-8') is None else row['Rango Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo de mora'].encode('utf-8') is None else row['Tipo de mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Rango Gestion'].encode('utf-8') is None else row['Rango Gestion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Estrategia gestion'].encode('utf-8') is None else row['Estrategia gestion'].encode('utf-8') + "|"
        text_row += '' + "|" if row[76].encode('utf-8') is None else row[76].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dia Para Rodar'].encode('utf-8') is None else row['Dia Para Rodar'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo Base Inicial'].encode('utf-8') is None else row['Tipo Base Inicial'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Rango desfase'].encode('utf-8') is None else row['Rango desfase'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultima Gestion Adminfo'].encode('utf-8') is None else row['Fecha Ultima Gestion Adminfo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Ultimo Codigo De Gestion Adminfo'].encode('utf-8') is None else row['Desc Ultimo Codigo De Gestion Adminfo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Mora Base Inicio'].encode('utf-8') is None else row['Mora Base Inicio'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Mora Bini Definitiva'].encode('utf-8') is None else row['Mora Bini Definitiva'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Base ini Inter'].encode('utf-8') is None else row['Base ini Inter'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cosecha'].encode('utf-8') is None else row['Cosecha'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Trimestre'].encode('utf-8') is None else row['Trimestre'].encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row

    
    filename = FECHA_CARGUE +  ".csv"
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-bridge")

    flowAnswer = bridge_beam.run(table)

# Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-bridge')
    blob = bucket.blob(filename)

    time.sleep(float(timer)) #1hora y 20 minutos para que cierre la conexion  de mssql
    # Eliminar el archivo en la variable
    # blob.delete()
    conn.close()
    return TABLE_DB + flowAnswer




#####################################################################################################################################
######################################################### BD_NTILES #################################################################
#####################################################################################################################################
#####################################################################################################################################


@bridge_api.route("/bridge2", methods=['GET'])
def bridge2():
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
# Parametros GET para modificar la consulta segun los parametros entregados
    table = request.args.get('bdmssql')
    timer = request.args.get('time')
    if timer is None:
        timer = 600
    elif timer == "":
        timer = 600
    else: 
        timer

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
        text_row += '' + "|" if row['Consecutivo Documento Deudor'].encode('utf-8') is None else row['Consecutivo Documento Deudor'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Valor Cuota']).encode('utf-8') is None else str(row['Valor Cuota']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Nit'].encode('utf-8') is None else row['Nit'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombres'].encode('utf-8') is None else row['Nombres'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Numero Documento'].encode('utf-8') is None else row['Numero Documento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Pago Cuota'].encode('utf-8') is None else row['Fecha Pago Cuota'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre De Producto'].encode('utf-8') is None else row['Nombre De Producto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha De Perfeccionamiento'].encode('utf-8') is None else row['Fecha De Perfeccionamiento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Numero Cuotas'].encode('utf-8') is None else row['Numero Cuotas'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dia De Vencimiento De Cuota'].encode('utf-8') is None else row['Dia De Vencimiento De Cuota'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Valor Obligacion']).encode('utf-8') is None else str(row['Valor Obligacion']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Valor Vencido']).encode('utf-8') is None else str(row['Valor Vencido']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Saldo Activo']).encode('utf-8') is None else str(row['Saldo Activo']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Saldo Orden'].encode('utf-8') is None else row['Saldo Orden'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Ciudad'].encode('utf-8') is None else row['Ciudad'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Nombre Abogado'].encode('utf-8') is None else row['Nombre Abogado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultima Gestion Prejuridica'].encode('utf-8') is None else row['Fecha Ultima Gestion Prejuridica'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') is None else row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Cod  Anterior de Gestion Prejuridica'].encode('utf-8') is None else row['Desc Cod  Anterior de Gestion Prejuridica'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultimo Pago'].encode('utf-8') is None else row['Fecha Ultimo Pago'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dias Mora'].encode('utf-8') is None else row['Dias Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Calificacion'].encode('utf-8') is None else row['Calificacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fondo Nacional Garantias'].encode('utf-8') is None else row['Fondo Nacional Garantias'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Region'].encode('utf-8') is None else row['Region'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Segmento'].encode('utf-8') is None else row['Segmento'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Calificacion Real'].encode('utf-8') is None else row['Calificacion Real'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Ultima Facturacion'].encode('utf-8') is None else row['Fecha Ultima Facturacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cuadrante'].encode('utf-8') is None else row['Cuadrante'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Causal'].encode('utf-8') is None else row['Causal'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Sector Economico'].encode('utf-8') is None else row['Sector Economico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha De Promesa'].encode('utf-8') is None else row['Fecha De Promesa'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Endeudamiento']).encode('utf-8') is None else str(row['Endeudamiento']).encode('utf-8') + "|"
        text_row += '' + "|" if row[32].encode('utf-8') is None else row[32].encode('utf-8') + "|"
        text_row += '' + "|" if row[33].encode('utf-8') is None else row[33].encode('utf-8') + "|"
        text_row += '' + "|" if row['Mono   Multi'].encode('utf-8') is None else row['Mono   Multi'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Grupo'].encode('utf-8') is None else row['Grupo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo TC'].encode('utf-8') is None else row['Tipo TC'].encode('utf-8') + "|"
        text_row += '' + "|" if row['CICLO'].encode('utf-8') is None else row['CICLO'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Estrategia Cliente'].encode('utf-8') is None else row['Estrategia Cliente'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Impacto'].encode('utf-8') is None else row['Impacto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Rango Mora'].encode('utf-8') is None else row['Rango Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo Base Inicial'].encode('utf-8') is None else row['Tipo Base Inicial'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Rango desfase'].encode('utf-8') is None else row['Rango desfase'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Mora Base Inicio'].encode('utf-8') is None else row['Mora Base Inicio'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Mora Bini Definitiva'].encode('utf-8') is None else row['Mora Bini Definitiva'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Base ini Inter'].encode('utf-8') is None else row['Base ini Inter'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cosecha'].encode('utf-8') is None else row['Cosecha'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Trimestre'].encode('utf-8') is None else row['Trimestre'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha_Retiro'].encode('utf-8') is None else row['Fecha_Retiro'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contacto']).encode('utf-8') is None else str(row['Contacto']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Tipo Gest']).encode('utf-8') is None else str(row['Tipo Gest']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Cta_Dia']).encode('utf-8') is None else str(row['Cta_Dia']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Sem']).encode('utf-8') is None else str(row['Sem']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Decil_DM'].encode('utf-8') is None else row['Decil_DM'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Decil_VrVenc'].encode('utf-8') is None else row['Decil_VrVenc'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Clasif_Vr_Oblig'].encode('utf-8') is None else row['Clasif_Vr_Oblig'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Percentil_VrOblig'].encode('utf-8') is None else row['Percentil_VrOblig'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Clasif_Vr_Venc'].encode('utf-8') is None else row['Clasif_Vr_Venc'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Percentil_VrVenc'].encode('utf-8') is None else row['Percentil_VrVenc'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Cta'].encode('utf-8') is None else row['Cta'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Asignado'].encode('utf-8') is None else row['Asignado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Gestionado'].encode('utf-8') is None else row['Gestionado'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Contacto General'].encode('utf-8') is None else row['Contacto General'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Contacto Directo'].encode('utf-8') is None else row['Contacto Directo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Productivo'].encode('utf-8') is None else row['Productivo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Efectivo'].encode('utf-8') is None else row['Efectivo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Efectivo/Productivo'].encode('utf-8') is None else row['Efectivo/Productivo'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Clasif'].encode('utf-8') is None else row['Clasif'].encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row

    
    filename = FECHA_CARGUE + "_2" + ".csv"
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-bridge")

    flowAnswer = bridge_beam2.run(table)

# Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-bridge')
    blob = bucket.blob(filename)

    time.sleep(float(timer)) #1hora y 20 minutos para que cierre la conexion  de mssql
    # Eliminar el archivo en la variable
    # blob.delete()
    conn.close()
    return TABLE_DB + flowAnswer





#####################################################################################################################################
##################################################### CONS_GEST #####################################################################
#####################################################################################################################################
#####################################################################################################################################


@bridge_api.route("/bridge3", methods=['GET'])
def bridge3():
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
# Parametros GET para modificar la consulta segun los parametros entregados
    table = request.args.get('bdmssql')
    timer = request.args.get('time')
    if timer is None:
        timer = 600
    elif timer == "":
        timer = 600
    else: 
        timer

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
        text_row += '' + "|" if row['Nit'].encode('utf-8') is None else row['Nit'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Fecha Gestion']).encode('utf-8') is None else str(row['Fecha Gestion']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Nota'].encode('utf-8') is None else row['Nota'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Grabador'].encode('utf-8') is None else row['Grabador'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') is None else row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['T_entrada'].encode('utf-8') is None else row['T_entrada'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Hora Grabacion'].encode('utf-8') is None else row['Hora Grabacion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Consdocdeu'].encode('utf-8') is None else row['Consdocdeu'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Regional'].encode('utf-8') is None else row['Regional'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Dias De Mora'].encode('utf-8') is None else row['Dias De Mora'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Duracion'].encode('utf-8') is None else row['Duracion'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Fecha Promesa'].encode('utf-8') is None else row['Fecha Promesa'].encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row

    
    filename = FECHA_CARGUE + "_3" + ".csv"
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-bridge")

    flowAnswer = bridge_beam3.run(table)

# Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-bridge')
    blob = bucket.blob(filename)

    
    time.sleep(float(timer)) #1hora y 20 minutos para que cierre la conexion  de mssql
    # Eliminar el archivo en la variable
    # blob.delete()
    conn.close()
    return TABLE_DB + "flowAnswer"




#####################################################################################################################################
############################################################ CTA_DIA ################################################################
#####################################################################################################################################
#####################################################################################################################################


@bridge_api.route("/bridge4", methods=['GET'])
def bridge4():
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
# Parametros GET para modificar la consulta segun los parametros entregados
    table = request.args.get('bdmssql')
    timer = request.args.get('time')
    if timer is None:
        timer = 600
    elif timer == "":
        timer = 600
    else: 
        timer

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
    # Debido a que los registros en esta tabla pueden tener saltos de 
    # linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += '' + "|" if row[0].encode('utf-8') is None else row[0].encode('utf-8') + "|"
        text_row += '' + "|" if row[1].encode('utf-8') is None else row[1].encode('utf-8') + "|"
        text_row += '' + "|" if row[2].encode('utf-8') is None else row[2].encode('utf-8') + "|"
        text_row += '' + "|" if row[3].encode('utf-8') is None else row[3].encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row

    
    filename = FECHA_CARGUE + "_4" + ".csv"
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-bridge")

    flowAnswer = bridge_beam4.run(table)

# Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-bridge')
    blob = bucket.blob(filename)

    time.sleep(float(timer)) #1hora y 20 minutos para que cierre la conexion  de mssql
    # Eliminar el archivo en la variable
    # blob.delete()
    conn.close()
    return TABLE_DB + flowAnswer
    



#####################################################################################################################################
#################################################### DETALLE_CONTACTABILIDAD ########################################################
#####################################################################################################################################
#####################################################################################################################################


@bridge_api.route("/bridge5", methods=['GET'])
def bridge5():
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
# Parametros GET para modificar la consulta segun los parametros entregados
    table = request.args.get('bdmssql')
    timer = request.args.get('time')
    if timer is None:
        timer = 600
    elif timer == "":
        timer = 600
    else: 
        timer

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
        text_row += '' + "|" if row['Nit'].encode('utf-8') is None else row['Nit'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Consdocdeu'].encode('utf-8') is None else row['Consdocdeu'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Clasif']).encode('utf-8') is None else str(row['Clasif']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Grupo']).encode('utf-8') is None else str(row['Grupo']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Segmento']).encode('utf-8') is None else str(row['Segmento']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contacto']).encode('utf-8') is None else str(row['Contacto']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Valor Obligacion']).encode('utf-8') is None else str(row['Valor Obligacion']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Valor Vencido']).encode('utf-8') is None else str(row['Valor Vencido']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[8]).encode('utf-8') is None else str(row[8]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Cant Pagos']).encode('utf-8') is None else str(row['Cant Pagos']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Vr Pagado']).encode('utf-8') is None else str(row['Vr Pagado']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Cta_Dia']).encode('utf-8') is None else str(row['Cta_Dia']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[12]).encode('utf-8') is None else str(row[12]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Asignado']).encode('utf-8') is None else str(row['Asignado']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Contactabilidad'].encode('utf-8') is None else row['Contactabilidad'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Foco Plan Trabajo'].encode('utf-8') is None else row['Foco Plan Trabajo'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Ult Contacto Dir']).encode('utf-8') is None else str(row['Ult Contacto Dir']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Dia Ult_contacto']).encode('utf-8') is None else str(row['Dia Ult_contacto']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['T  Ult contacto']).encode('utf-8') is None else str(row['T  Ult contacto']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[19]).encode('utf-8') is None else str(row[19]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Total Gest']).encode('utf-8') is None else str(row['Total Gest']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Total Contactos']).encode('utf-8') is None else str(row['Total Contactos']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Total SMS']).encode('utf-8') is None else str(row['Total SMS']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[23]).encode('utf-8') is None else str(row[23]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Gest Sab-Dom']).encode('utf-8') is None else str(row['Gest Sab-Dom']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[25]).encode('utf-8') is None else str(row[25]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[26]).encode('utf-8') is None else str(row[26]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[27]).encode('utf-8') is None else str(row[27]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[28]).encode('utf-8') is None else str(row[28]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[29]).encode('utf-8') is None else str(row[29]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[30]).encode('utf-8') is None else str(row[30]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Sab-Dom']).encode('utf-8') is None else str(row['Contactab Sab-Dom']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Lun']).encode('utf-8') is None else str(row['Contactab Lun']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Mar']).encode('utf-8') is None else str(row['Contactab Mar']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Mier']).encode('utf-8') is None else str(row['Contactab Mier']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Jue']).encode('utf-8') is None else str(row['Contactab Jue']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Vier']).encode('utf-8') is None else str(row['Contactab Vier']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Sab']).encode('utf-8') is None else str(row['Contactab Sab']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Dom']).encode('utf-8') is None else str(row['Contactab Dom']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[39]).encode('utf-8') is None else str(row[39]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[40]).encode('utf-8') is None else str(row[40]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Tarde']).encode('utf-8') is None else str(row['Contactab Tarde']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Contactab Noche']).encode('utf-8') is None else str(row['Contactab Noche']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[43]).encode('utf-8') is None else str(row[43]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[44]).encode('utf-8') is None else str(row[44]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[45]).encode('utf-8') is None else str(row[45]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[46]).encode('utf-8') is None else str(row[46]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[47]).encode('utf-8') is None else str(row[47]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[48]).encode('utf-8') is None else str(row[49]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[49]).encode('utf-8') is None else str(row[50]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[50]).encode('utf-8') is None else str(row[51]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[51]).encode('utf-8') is None else str(row[52]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[52]).encode('utf-8') is None else str(row[53]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[53]).encode('utf-8') is None else str(row[54]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[54]).encode('utf-8') is None else str(row[55]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[55]).encode('utf-8') is None else str(row[56]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[56]).encode('utf-8') is None else str(row[57]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[57]).encode('utf-8') is None else str(row[58]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[58]).encode('utf-8') is None else str(row[59]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Gest 6 AM']).encode('utf-8') is None else row['Gest 6 AM'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row[60]).encode('utf-8') is None else str(row[60]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[61]).encode('utf-8') is None else str(row[61]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Gest Tarde']).encode('utf-8') is None else str(row['Gest Tarde']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Gest Noche']).encode('utf-8') is None else str(row['Gest Noche']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Lun']).encode('utf-8') is None else str(row['Lun']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Mar']).encode('utf-8') is None else str(row['Mar']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Mier']).encode('utf-8') is None else str(row['Mier']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Jue']).encode('utf-8') is None else str(row['Jue']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Vier']).encode('utf-8') is None else str(row['Vier']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Sab']).encode('utf-8') is None else str(row['Sab']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Dom']).encode('utf-8') is None else str(row['Dom']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['06 00 a m ']).encode('utf-8') is None else str(row['06 00 a m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['07 00 a m ']).encode('utf-8') is None else str(row['07 00 a m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['08 00 a m ']).encode('utf-8') is None else str(row['08 00 a m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['09 00 a m ']).encode('utf-8') is None else str(row['09 00 a m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['10 00 a m ']).encode('utf-8') is None else str(row['10 00 a m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['11 00 a m ']).encode('utf-8') is None else str(row['11 00 a m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['12 00 a m ']).encode('utf-8') is None else str(row['12 00 a m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['01 00 p m ']).encode('utf-8') is None else str(row['01 00 p m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['02 00 p m ']).encode('utf-8') is None else str(row['02 00 p m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['03 00 p m ']).encode('utf-8') is None else str(row['03 00 p m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['04 00 p m ']).encode('utf-8') is None else str(row['04 00 p m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['05 00 p m ']).encode('utf-8') is None else str(row['05 00 p m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['06 00 p m ']).encode('utf-8') is None else str(row['06 00 p m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['07 00 p m ']).encode('utf-8') is None else str(row['07 00 p m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['08 00 p m ']).encode('utf-8') is None else str(row['08 00 p m ']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['09 00 p m ']).encode('utf-8') is None else str(row['09 00 p m ']).encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row

    
    filename = FECHA_CARGUE + "_5" + ".csv"
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-bridge")

    flowAnswer = bridge_beam5.run(table)

# Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-bridge')
    blob = bucket.blob(filename)

    time.sleep(float(timer)) #1hora y 20 minutos para que cierre la conexion  de mssql
    # Eliminar el archivo en la variable
    # blob.delete()
    conn.close()
    return TABLE_DB + flowAnswer
  



#####################################################################################################################################
#################################################### INFORME_CONTACTABILIDAD ########################################################
#####################################################################################################################################
#####################################################################################################################################


@bridge_api.route("/bridge6", methods=['GET'])
def bridge6():
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
# Parametros GET para modificar la consulta segun los parametros entregados
    table = request.args.get('bdmssql')
    timer = request.args.get('time')
    week = request.args.get('sem')
    if timer is None:
        timer = 600
    elif timer == "":
        timer = 600
    else: 
        timer

    SERVER="192.168.20.63\DELTA"
    USER="DP_USER"
    PASSWORD="Contento2018"
    DATABASE="Contactabilidad"
    TABLE_DB = "dbo." + str(table)
    FECHA_CARGUE = str(datetime.date.today())
    Fecha = datetime.datetime.today().strftime('%Y-%m-%d')    


    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)

    # Realizamos la consulta de los datos en MSSQL
    conn.execute_query("SELECT * FROM " + TABLE_DB + " WHERE SUBSTRING(Sem,5,1) = '" + week + "'")
    
    cloud_storage_rows = ""
    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += '' + "|" if str(row[0]).encode('utf-8') is None else str(row[0]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Fecha Gestion']).encode('utf-8') is None else str(row['Fecha Gestion']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') is None else row['Desc Ultimo Codigo De Gestion Prejuridico'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Grabador'].encode('utf-8') is None else row['Grabador'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Consdocdeu']).encode('utf-8') is None else str(row['Consdocdeu']).encode('utf-8') + "|"
        text_row += '' + "|" if row['Contacto'].encode('utf-8') is None else row['Contacto'].encode('utf-8') + "|"
        text_row += '' + "|" if row['Tipo Gest'].encode('utf-8') is None else row['Tipo Gest'].encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Prioridad']).encode('utf-8') is None else str(row['Prioridad']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Dias Mora']).encode('utf-8') is None else str(row['Dias Mora']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Rango Mora']).encode('utf-8') is None else str(row['Rango Mora']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Valor Obligacion']).encode('utf-8') is None else str(row['Valor Obligacion']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Valor Vencido']).encode('utf-8') is None else str(row['Valor Vencido']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Endeudamiento']).encode('utf-8') is None else str(row['Endeudamiento']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Valor Cuota']).encode('utf-8') is None else str(row['Valor Cuota']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[14]).encode('utf-8') is None else str(row[14]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[15]).encode('utf-8') is None else str(row[15]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Grupo']).encode('utf-8') is None else str(row['Grupo']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[17]).encode('utf-8') is None else str(row[17]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Region']).encode('utf-8') is None else str(row['Region']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Segmento']).encode('utf-8') is None else str(row['Segmento']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[20]).encode('utf-8') is None else str(row[20]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Cuadrante']).encode('utf-8') is None else str(row['Cuadrante']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Causal']).encode('utf-8') is None else str(row['Causal']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Sector Economico']).encode('utf-8') is None else str(row['Sector Economico']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Cosecha']).encode('utf-8') is None else str(row['Cosecha']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row[25]).encode('utf-8') is None else str(row[25]).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Nombre Asesor']).encode('utf-8') is None else str(row['Nombre Asesor']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Producto']).encode('utf-8') is None else str(row['Producto']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Sede']).encode('utf-8') is None else str(row['Sede']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Fecha_CtaDia']).encode('utf-8') is None else str(row['Fecha_CtaDia']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['DiaSem']).encode('utf-8') is None else str(row['DiaSem']).encode('utf-8') + "|"
        text_row += '' + "|" if str(row['Sem']).encode('utf-8') is None else str(row['Sem']).encode('utf-8') + "|"
        text_row += "\n"

        cloud_storage_rows += text_row

    
    filename = FECHA_CARGUE + "_6" + ".csv"
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-bridge")

    flowAnswer = bridge_beam6.run(table)

# Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-bridge')
    blob = bucket.blob(filename)

    time.sleep(float(timer))
    # Eliminar el archivo en la variable
    blob.delete()
    conn.close()
    return TABLE_DB + flowAnswer