from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from flask import request
from google.auth.transport.requests import AuthorizedSession
import dataflow_pipeline.gestion_humana.gestiones_beam as gestiones_beam
import dataflow_pipeline.gestion_humana.documentos_pendientes_beam as documentos_pendientes_beam
import dataflow_pipeline.gestion_humana.parametros_dias_beam as parametros_dias_beam
import dataflow_pipeline.gestion_humana.e_d_preguntas_beam as e_d_preguntas_beam
import dataflow_pipeline.gestion_humana.e_d_tipo_respuestas_beam as e_d_tipo_respuestas_beam
import dataflow_pipeline.gestion_humana.e_d_competencia_beam as e_d_competencia_beam
import dataflow_pipeline.gestion_humana.e_d_detalle_evaluacion_beam as e_d_detalle_evaluacion_beam
import dataflow_pipeline.gestion_humana.e_d_rel_competencia_beam as e_d_rel_competencia_beam
import dataflow_pipeline.gestion_humana.e_d_historico_evaluacion_beam as e_d_historico_evaluacion_beam
import dataflow_pipeline.gestion_humana.e_d_rel_usuarios_beam as e_d_rel_usuarios_beam
import dataflow_pipeline.gestion_humana.e_d_tb_usuarios_beam as e_d_tb_usuarios_beam
import dataflow_pipeline.gestion_humana.e_d_centro_costos_beam as e_d_centro_costos_beam
import dataflow_pipeline.gestion_humana.e_d_uen_beam as e_d_uen_beam
import dataflow_pipeline.gestion_humana.e_d_cargos_beam as e_d_cargos_beam
import dataflow_pipeline.gestion_humana.e_d_perfiles_beam as e_d_perfiles_beam
import dataflow_pipeline.gestion_humana.e_d_base_beam as e_d_base_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import dataflow_pipeline.massive as pipeline
from google.oauth2 import service_account
import os
import time
import socket
import _mssql
import datetime
import sys

#coding: utf-8 

gto_api = Blueprint('gto_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@gto_api.route("/gestiones")
def gestiones():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Gestiones_EPS"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT Id_Gestion,Persona_que_reporta,Sede,Cedula,Nombre_completo,Eps,Cod_Centro_Costos,Nombre_Centro_Costos,Tipo_incapacidad,Buscar_diagnostico,Codigo_diagnostico,Nombre_diagnostico,Numero_incapacidad,Dias_incapacidad,Fecha_inicial_liquidacion,Fecha_real_inicial,Fecha_final_incapacidad,Ajuste_incapacidad_salario_minimo,Prorroga,Documento_prorroga,Accidente_transito,IBC_mes_anterior,IBC_Cotizacion_especifico,Fecha_recibido_incapacidad,Mes_aplicaco_nomina,VoBo_Ejecucion_RPA,Tiene_Transcripcion,Documentacion_transcripcion,Documento_pendiente,Correo_responsable,Fecha_Notificacion_Docs_Incompletos,Fecha_Envio_Docs_Incompletos,Fecha_Envio_Docs_Correcto,Documentacion_Completa,Nro_Incapacidad,Insert_date,Transcrito_Por,Fecha_Sol_Transcripcion,Fecha_Transcripcion,Fecha_Max_Transcripcion,Fecha_Sol_Cobro,Fecha_Cobro,Fecha_Max_Cobro,Valor_pagado,Fecha_pago,Fecha_Max_Pago,Estado_Gossem,Marca_Gosem,Fecha_Proceso_Gossem,Duracion_Gossem FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Gestion']).encode('utf-8') + "|"
        text_row += str(row['Persona_que_reporta']).encode('utf-8') + "|"
        text_row += str(row['Sede']).encode('utf-8') + "|"
        text_row += str(row['Cedula']).encode('utf-8') + "|"
        text_row += str(row['Nombre_completo']).encode('utf-8') + "|"
        text_row += str(row['Eps']).encode('utf-8') + "|"
        text_row += str(row['Cod_Centro_Costos']).encode('utf-8') + "|"
        text_row += str(row['Nombre_Centro_Costos']).encode('utf-8') + "|"
        text_row += str(row['Tipo_incapacidad']).encode('utf-8') + "|"
        text_row += str(row['Buscar_diagnostico']).encode('utf-8') + "|"
        text_row += str(row['Codigo_diagnostico']).encode('utf-8') + "|"
        text_row += str(row['Nombre_diagnostico']).encode('utf-8') + "|"
        text_row += str(row['Numero_incapacidad']).encode('utf-8') + "|"
        text_row += str(row['Dias_incapacidad']).encode('utf-8') + "|"
        text_row += str(row['Fecha_inicial_liquidacion']).encode('utf-8') + "|"
        text_row += str(row['Fecha_real_inicial']).encode('utf-8') + "|"
        text_row += str(row['Fecha_final_incapacidad']).encode('utf-8') + "|"
        text_row += str(row['Ajuste_incapacidad_salario_minimo']).encode('utf-8') + "|"
        text_row += str(row['Prorroga']).encode('utf-8') + "|"
        text_row += str(row['Documento_prorroga']).encode('utf-8') + "|"
        text_row += str(row['Accidente_transito']).encode('utf-8') + "|"
        text_row += str(row['IBC_mes_anterior']).encode('utf-8') + "|"
        text_row += str(row['IBC_Cotizacion_especifico']).encode('utf-8') + "|"
        text_row += str(row['Fecha_recibido_incapacidad']).encode('utf-8') + "|"
        text_row += str(row['Mes_aplicaco_nomina']).encode('utf-8') + "|"
        text_row += str(row['VoBo_Ejecucion_RPA']).encode('utf-8') + "|"
        text_row += str(row['VoBo_Ejecucion_RPA_Seguimiento']).encode('utf-8') + "|"
        text_row += str(row['Tiene_Transcripcion']).encode('utf-8') + "|"
        text_row += str(row['Documentacion_transcripcion']).encode('utf-8') + "|"
        text_row += str(row['Documento_pendiente']).encode('utf-8') + "|"
        text_row += str(row['Correo_responsable']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Notificacion_Docs_Incompletos']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Envio_Docs_Incompletos']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Envio_Docs_Correcto']).encode('utf-8') + "|"
        text_row += str(row['Documentacion_Completa']).encode('utf-8') + "|" 
        text_row += str(row['Nro_Incapacidad']).encode('utf-8') + "|"
        text_row += str(row['Insert_date']).encode('utf-8') + "|"
        text_row += str(row['Transcrito_Por']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Sol_Transcripcion']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Transcripcion']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Max_Transcripcion']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Sol_Cobro']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Cobro']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Max_Cobro']).encode('utf-8') + "|"
        text_row += str(row['Valor_pagado']).encode('utf-8') + "|"
        text_row += str(row['Fecha_pago']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Max_Pago']).encode('utf-8') + "|"
        text_row += str(row['Estado_Gossem']).encode('utf-8') + "|"
        text_row += str(row['Marca_Gosem']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Proceso_Gossem']).encode('utf-8') + "|"
        text_row += str(row['Duracion_Gossem']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "gestiones" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.Gestiones` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = gestiones_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("gestiones" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "Cargue exitoso Tabla de gestiones " + flowAnswer 



######################################## DOCUMENTOS PENDIENTES ######################################

@gto_api.route("/d_pendientes")
def d_pendientes():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "D_pendientes"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT MAD_Mail_Documentos,MAD_Id_Gestion,MAD_Correo_Responsble,MAD_Nombre_Completo,MAD_Documento,MAD_Fecha_Real_Inicial,MAD_Documento_Pendiente,MAD_Insert_Date FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['MAD_Mail_Documentos']).encode('utf-8') + "|"
        text_row += str(row['MAD_Id_Gestion']).encode('utf-8') + "|"
        text_row += str(row['MAD_Correo_Responsble']).encode('utf-8') + "|"
        text_row += str(row['MAD_Nombre_Completo']).encode('utf-8') + "|"
        text_row += str(row['MAD_Documento']).encode('utf-8') + "|"
        text_row += str(row['MAD_Fecha_Real_Inicial']).encode('utf-8') + "|"
        text_row += str(row['MAD_Documento_Pendiente']).encode('utf-8') + "|"
        text_row += str(row['MAD_Insert_Date']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "d_pendientes" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.D_pendientes` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = documentos_pendientes_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("d_pendientes" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "Cargue exitoso Tabla de documentos pendientes " + flowAnswer 


####################################### DIAS HABILES #####################################

@gto_api.route("/dias")
def dias():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Info_EPS"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT Eps,Url,User_Eps,Pass,Correo_Notificaciones,Estado,Tiempo_Transcripcion_Empresa,Tiempo_Cobro_Empresa,Tiempo_Transcripcion_EPS,Tiempo_Pago_EPS,Insert_Date  FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Eps']).encode('utf-8') + "|"
        text_row += str(row['Url']).encode('utf-8') + "|"
        text_row += str(row['User_Eps']).encode('utf-8') + "|"
        text_row += str(row['Pass']).encode('utf-8') + "|"
        text_row += str(row['Correo_Notificaciones']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        text_row += str(row['Tiempo_Transcripcion_Empresa']).encode('utf-8') + "|"
        text_row += str(row['Tiempo_Cobro_Empresa']).encode('utf-8') + "|"
        text_row += str(row['Tiempo_Transcripcion_EPS']).encode('utf-8') + "|"
        text_row += str(row['Tiempo_Pago_EPS']).encode('utf-8') + "|"
        text_row += str(row['Insert_Date']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "dias" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.Parametros_dias` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = parametros_dias_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("dias" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "Cargue exitoso Tabla de parametros dias " + flowAnswer 


##################################### INFORME EVALUACION DE DESEMPENO ###########################################

############################################## TABLA PREGUNTAS ##################################################


@gto_api.route("/preguntas")
def preguntas():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Preguntas"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_pregunta,nombre_pregunta,estado,fecha_modif,user_modif FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_pregunta']).encode('utf-8') + "|"
        text_row += str(row['nombre_pregunta']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['user_modif']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/preguntas" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_Preguntas` WHERE id_pregunta > 1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_preguntas_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/preguntas" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()
    

    return "Cargue exitoso Tabla de preguntas " + flowAnswer 


################################ TIPO REPUESTA #################################

@gto_api.route("/tipo_respuestas")
def respuestas():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Tipo_respuesta"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_tipo_respuesta,nombre_respuesta,estado,fecha_modif,usuario_modif FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_tipo_respuesta']).encode('utf-8') + "|"
        text_row += str(row['nombre_respuesta']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/tipo_respuestas" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_tipo_respuesta` WHERE id_tipo_respuesta <> "" "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_tipo_respuestas_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/tipo_respuestas" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de tipo_respuestas " + flowAnswer 


############################ TB COMPETENCIA #################################

@gto_api.route("/competencia")
def competencia():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Competencia"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_competencia,nombre_competencia,id_cargo,estado,fecha_modif,usuario_modif,peso FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_competencia']).encode('utf-8') + "|"
        text_row += str(row['nombre_competencia']).encode('utf-8') + "|"
        text_row += str(row['id_cargo']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8') + "|"
        text_row += str(row['peso']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/competencia" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_competencia` WHERE id_competencia <> "" "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_competencia_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/competencia" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de competencia " + flowAnswer 


############################ TB EVALUACION DETALLE ############################

@gto_api.route("/detalle_eva")
def detalle_eva():

    dateini = request.args.get('dateini')
    dateend = request.args.get('dateend')

    if dateini is None: 
        dateini = ""
    else:
        dateini = dateini    

    if dateend is None: 
        dateend = ""
    else:
        dateend = dateend  

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Detalle_evaluacion"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros

    # if dateini == "":
    #     conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    #     conn.execute_query('SELECT id_evaluacion_detalle,notaXPregunta,estado,fecha_modif,usuario_modif,id_relcompetencia,pesoXCompetencia,respuestaAbierta,id_evaluacion_historico  FROM ' + tabla_bd + ' WHERE CONVERT(DATE, fecha_modif) = CONVERT(DATE,GETDATE())') 

    if dateini == "":
        conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        conn.execute_query('SELECT id_evaluacion_detalle,notaXPregunta,estado,fecha_modif,usuario_modif,id_relcompetencia,pesoXCompetencia,respuestaAbierta,id_evaluacion_historico  FROM ' + tabla_bd )

    else:
        conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        conn.execute_query('SELECT id_evaluacion_detalle,notaXPregunta,estado,fecha_modif,usuario_modif,id_relcompetencia,pesoXCompetencia,respuestaAbierta,id_evaluacion_historico  FROM ' + tabla_bd  + ' WHERE CONVERT(DATE, fecha_modif)'  ' between ' + "'" + dateini + "'" +" and " + "'" + dateend + "'"  )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_evaluacion_detalle']).encode('utf-8') + "|"
        text_row += str(row['notaXPregunta']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8') + "|"
        text_row += str(row['id_relcompetencia']).encode('utf-8') + "|"
        text_row += str(row['pesoXCompetencia']).encode('utf-8') + "|"
        text_row += str(row['respuestaAbierta']).encode('utf-8') + "|"
        text_row += str(row['id_evaluacion_historico']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/detalle_eva" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        if dateini == "":
            # deleteQuery = 'DELETE FROM `contento-bi.gestion_humana.E_D_detalle_eva` WHERE CAST(SUBSTR(fecha_modif,0,10) AS DATE) = CURRENT_DATE()'

            deleteQuery = 'DELETE FROM `contento-bi.gestion_humana.E_D_detalle_eva` WHERE 1 = 1'

            client = bigquery.Client()
            query_job = client.query(deleteQuery)
            query_job.result()
        else:
            deleteQuery2 = 'DELETE FROM `contento-bi.gestion_humana.E_D_detalle_eva` WHERE CAST(SUBSTR(fecha_modif,0,10) AS DATE) between ' + "'" + dateini + "'" +" and " + "'" + dateend + "'"
            client = bigquery.Client()
            query_job = client.query(deleteQuery2)
            query_job.result()            
    except:
        print("no se pudo eliminar")


    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_detalle_evaluacion_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/detalle_eva" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de competencia " + flowAnswer 

################################### REL COMPETENCIA ##############################


@gto_api.route("/rel_competencia")
def rel_competencia():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Rel_competencia"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_relcompetencia,id_competencia,id_pregunta,id_tipo_respuesta,estado,fecha_modif,usuario_modif FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_relcompetencia']).encode('utf-8') + "|"
        text_row += str(row['id_competencia']).encode('utf-8') + "|"
        text_row += str(row['id_pregunta']).encode('utf-8') + "|"
        text_row += str(row['id_tipo_respuesta']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/rel_competencia" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_rel_competencia` WHERE 1 = 1 "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_rel_competencia_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/rel_competencia" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de competencia " + flowAnswer 


################################### HISTORICO EVALUACIONES ##############################


@gto_api.route("/historico_eva")
def historico_eva():

    dateini = request.args.get('dateini')
    dateend = request.args.get('dateend')

    if dateini is None: 
        dateini = ""
    else:
        dateini = dateini    

    if dateend is None: 
        dateend = ""
    else:
        dateend = dateend      

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Historico_evaluacion"')
    query_job = client.query(QUERY)
    rows = query_job.result()
    data = ""

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd
    

    reload(sys)
    sys.setdefaultencoding('utf8')
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    # Nos conectamos a la BD y obtenemos los registros
  


    # if dateini == "":    
    #     conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    #     conn.execute_query('SELECT id_evaluacion_historico,observaciones,documento_evaluador,id_cargo_evaluador,documento_evaluado,id_cargo_evaluado,id_centrocosto,fecha_cierre,fecha_evaluacion FROM ' + tabla_bd + ' WHERE CONVERT(DATE, fecha_evaluacion) = CONVERT(DATE,GETDATE())') 
    #     cloud_storage_rows = ""

    if dateini == "":    
        conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        conn.execute_query('SELECT id_evaluacion_historico,observaciones,documento_evaluador,id_cargo_evaluador,documento_evaluado,id_cargo_evaluado,id_centrocosto,fecha_cierre,fecha_evaluacion FROM ' + tabla_bd )
        cloud_storage_rows = ""


    else:
        conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
        conn.execute_query('SELECT id_evaluacion_historico,observaciones,documento_evaluador,id_cargo_evaluador,documento_evaluado,id_cargo_evaluado,id_centrocosto,fecha_cierre,fecha_evaluacion FROM ' + tabla_bd + ' WHERE CONVERT(DATE, fecha_evaluacion)'  ' between ' + "'" + dateini + "'" +" and " + "'" + dateend + "'"  )   
        cloud_storage_rows = ""



    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_evaluacion_historico']).encode('utf-8') + "|"
        text_row += str(row['observaciones']).encode('utf-8') + "|"
        text_row += str(row['documento_evaluador']).encode('utf-8') + "|"
        text_row += str(row['id_cargo_evaluador']).encode('utf-8') + "|"
        text_row += str(row['documento_evaluado']).encode('utf-8') + "|"
        text_row += str(row['id_cargo_evaluado']).encode('utf-8') + "|"
        text_row += str(row['id_centrocosto']).encode('utf-8') + "|"
        text_row += str(row['fecha_cierre']).encode('utf-8') + "|"
        text_row += str(row['fecha_evaluacion']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/historico_eva" + ".csv"
     #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")


    try:
        if dateini == "":
            # deleteQuery = 'DELETE FROM `contento-bi.gestion_humana.E_D_historico_eva` WHERE CAST(SUBSTR(fecha_evaluacion,0,10) AS DATE) = CURRENT_DATE()'
            deleteQuery = 'DELETE FROM `contento-bi.gestion_humana.E_D_historico_eva` WHERE 1=1'

            client = bigquery.Client()
            query_job = client.query(deleteQuery)
            query_job.result()
        else:
            deleteQuery2 = 'DELETE FROM `contento-bi.gestion_humana.E_D_historico_eva` WHERE CAST(SUBSTR(fecha_evaluacion,0,10) AS DATE) between ' + "'" + dateini + "'" +" and " + "'" + dateend + "'"
            client = bigquery.Client()
            query_job = client.query(deleteQuery2)
            query_job.result()            
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_historico_evaluacion_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/historico_eva" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "tabla historico de evaluacion cargada" + flowAnswer

############################### REL USUARIOS #############################

@gto_api.route("/rel_usuarios")
def rel_usuarios():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Rel_usuarios"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_Relusuario,documento,id_Centrocosto,id_cargo,id_perfil,estado,fecha_modf,usuario_modif FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_Relusuario']).encode('utf-8') + "|"
        text_row += str(row['documento']).encode('utf-8') + "|"
        text_row += str(row['id_Centrocosto']).encode('utf-8') + "|"
        text_row += str(row['id_cargo']).encode('utf-8') + "|"
        text_row += str(row['id_perfil']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modf']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/rel_usuarios" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_rel_usuarios` WHERE  1 = 1 "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_rel_usuarios_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/rel_usuarios" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de rel ususarios " + flowAnswer 


############################### TB USUSARIOS ###############################


@gto_api.route("/tb_usuarios")
def tb_usuarios():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Usuarios"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT documento,nombre_usuario,da,estado,fecha_modf,usuario_modif,fecha_ingreso  FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['documento']).encode('utf-8') + "|"
        text_row += str(row['nombre_usuario']).encode('utf-8') + "|"
        text_row += str(row['da']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modf']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8') + "|"
        text_row += str(row['fecha_ingreso']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/tb_usuarios" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_tb_usuarios` WHERE 1 = 1 "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_tb_usuarios_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/tb_usuarios" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de tb ususarios " + flowAnswer 


################################# TB CENTRO DE COSTOS ################################


@gto_api.route("/centro_costos")
def centro_costos():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Centros_de_costos"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_Centrocosto,nombre_centro,id_uen,sede,estado,fecha_modif,usuario_modif  FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_Centrocosto']).encode('utf-8') + "|"
        text_row += str(row['nombre_centro']).encode('utf-8') + "|"
        text_row += str(row['id_uen']).encode('utf-8') + "|"
        text_row += str(row['sede']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/centro_costos" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_tb_centro_costos` WHERE 1 = 1 "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_centro_costos_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/centro_costos" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de tb centro costos " + flowAnswer 


############################## TB UEN #############################

@gto_api.route("/uen")
def uen():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "uen"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_uen,nombre_uen,estado,fecha_modif,user_modif  FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_uen']).encode('utf-8') + "|"
        text_row += str(row['nombre_uen']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['user_modif']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/uen" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_uen` WHERE 1 = 1 "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_uen_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/uen" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de uen " + flowAnswer 


################################# TB CARGO ##############################

@gto_api.route("/cargos")
def cargos():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Cargos"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_cargo,nombre_cargo,estado,fecha_modif,usuario_modif    FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_cargo']).encode('utf-8') + "|"
        text_row += str(row['nombre_cargo']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/cargos" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_cargos` WHERE cast(id_cargo as int64) > 0 "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_cargos_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/cargos" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de cargos " + flowAnswer 

####################################### PERFIL ####################################

@gto_api.route("/perfil")
def perfil():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Perfil"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_perfil,nombre_perfil,estado,fecha_modif,usuario_modif,id_perfil_padre  FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_perfil']).encode('utf-8') + "|"
        text_row += str(row['nombre_perfil']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8') + "|"
        text_row += str(row['fecha_modif']).encode('utf-8') + "|"
        text_row += str(row['usuario_modif']).encode('utf-8') + "|"
        text_row += str(row['id_perfil_padre']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/perfil" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_perfil` WHERE id_perfil <> "" "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_perfiles_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/perfil" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla de perfiles " + flowAnswer 


############################## BASE ##################################


@gto_api.route("/base")
def base():

    client = bigquery.Client()
    QUERY1 = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Base"')
    query_job = client.query(QUERY1)
    rows = query_job.result()
    # data = ""
      

    for row in rows:
        servidor = row.servidor
        usuario = row.usuario
        contrasena = row.contrasena
        data_base = row.data_base 
        tabla_bd = row.tabla_bd

    reload(sys)
    sys.setdefaultencoding('utf8')
    # HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_cargue,documento,nombre_usuario,id_uen,id_Centrocosto,id_cargo,ciudad,fecha_ingreso,id_perfil,Marca FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_cargue']).encode('utf-8') + "|"
        text_row += str(row['documento']).encode('utf-8') + "|"
        text_row += str(row['nombre_usuario']).encode('utf-8') + "|"
        text_row += str(row['id_uen']).encode('utf-8') + "|"
        text_row += str(row['id_Centrocosto']).encode('utf-8') + "|"
        text_row += str(row['id_cargo']).encode('utf-8') + "|"
        text_row += str(row['ciudad']).encode('utf-8') + "|"
        text_row += str(row['fecha_ingreso']).encode('utf-8') + "|"
        text_row += str(row['id_perfil']).encode('utf-8') + "|"
        text_row += str(row['Marca']).encode('utf-8') 
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "evaluacion_desempeno/base" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-gto")

    try:
        deleteQuery = "DELETE FROM `contento-bi.gestion_humana.E_D_base` WHERE id_cargue > 0 "
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = e_d_base_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-gto')
    blob = bucket.blob("evaluacion_desempeno/base" + ".csv")
    # Eliminar el archivo en la variable
    # blob.delete()    

    return "Cargue exitoso Tabla base " + flowAnswer 