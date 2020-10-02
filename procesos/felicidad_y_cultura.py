from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
from flask import request
from google.auth.transport.requests import AuthorizedSession
import dataflow_pipeline.felicidad_y_cultura.tabla_personal_beam as tabla_personal_beam
import dataflow_pipeline.felicidad_y_cultura.tabla_calificaciones_beam as tabla_calificaciones_beam
import dataflow_pipeline.felicidad_y_cultura.tabla_afirmaciones_beam as tabla_afirmaciones_beam
import dataflow_pipeline.felicidad_y_cultura.tabla_integracion_beam as tabla_integracion_beam
import dataflow_pipeline.felicidad_y_cultura.tabla_subcategoria_beam as tabla_subcategoria_beam
import dataflow_pipeline.felicidad_y_cultura.tabla_criterio_beam as tabla_criterio_beam
import dataflow_pipeline.felicidad_y_cultura.tabla_categoria_beam as tabla_categoria_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import sys

#coding: utf-8 

clima_api = Blueprint('clima_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@clima_api.route("/personal")
def personal():

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Clima_personal"')
    query_job = client.bigquery(QUERY)
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
    conn.execute_query('SELECT id_registro,documento,nombres,sexo,proceso,codigo,centro_costos,gerente,cargo,ciudad,fecha_ingreso,empleador,tipo_contrato,usuario_da,lider,estado FROM ' + tabla_bd )
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_registro']).encode('utf-8') + "|"
        text_row += str(row['documento']).encode('utf-8') + "|"
        text_row += str(row['nombres']).encode('utf-8') + "|"
        text_row += str(row['sexo']).encode('utf-8') + "|"
        text_row += str(row['proceso']).encode('utf-8') + "|"
        text_row += str(row['codigo']).encode('utf-8') + "|"
        text_row += str(row['centro_costos']).encode('utf-8') + "|"
        text_row += str(row['gerente']).encode('utf-8') + "|"
        text_row += str(row['cargo']).encode('utf-8') + "|"
        text_row += str(row['ciudad']).encode('utf-8') + "|"
        text_row += str(row['fecha_ingreso']).encode('utf-8') + "|"
        text_row += str(row['empleador']).encode('utf-8') + "|"
        text_row += str(row['tipo_contrato']).encode('utf-8') + "|"
        text_row += str(row['usuario_da']).encode('utf-8') + "|"
        text_row += str(row['lider']).encode('utf-8') + "|"
        text_row += str(row['estado']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Clima/personal" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-felicidad_y_cultura")

    try:
        deleteQuery = "DELETE FROM `contento-bi.Felicidad_y_Cultura.Personal` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = tabla_personal_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-felicidad_y_cultura')
    blob = bucket.blob("Clima/personal" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "Cargue exitoso Tabla del personal" + flowAnswer 


################################################ TABLA CALIFICACIONES ###########################################

@clima_api.route("/calificaciones")
def calificaciones():

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Clima_calificaciones"')
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

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_registro,documento,id_afirmacion,nota,fecha_registro FROM ' + tabla_bd)
    # conn.execute_query('SELECT id_registro,documento,id_afirmacion,nota,fecha_registro FROM ' + TABLE_DB +  'WHERE documento = "1017246086"')
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_registro']).encode('utf-8') + "|"
        text_row += str(row['documento']).encode('utf-8') + "|"
        text_row += str(row['id_afirmacion']).encode('utf-8') + "|"
        text_row += str(row['nota']).encode('utf-8') + "|"
        text_row += str(row['fecha_registro']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Clima/calificaciones" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-felicidad_y_cultura")

    try:
        # deleteQuery = "DELETE FROM `contento-bi.Felicidad_y_Cultura.Calificaciones` WHERE SUBSTR(fecha_registro,0,10) > '2020-09-30' "
    #         
        deleteQuery = "DELETE FROM `contento-bi.Felicidad_y_Cultura.Calificaciones` WHERE id_afirmacion is not null"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = tabla_calificaciones_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-felicidad_y_cultura')
    blob = bucket.blob("Clima/calificaciones" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    return " Cargue exitoso Tabla de calificaciones" + "flowAnswer" 


####################################### TABLA AFIRMACIONES ##########################################

@clima_api.route("/afirmaciones")
def afirmaciones():

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Clima_afirmaciones"')
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

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_afirmacion,desc_afirmacion FROM ' + tabla_bd)
    #    conn.execute_query('SELECT id_registro,documento,id_afirmacion,nota,fecha_registro FROM ' + TABLE_DB + " WHERE CONVERT(DATE, fecha_registro) = CONVERT(DATE,GETDATE())")
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_afirmacion']).encode('utf-8') + "|"
        text_row += str(row['desc_afirmacion']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Clima/afirmaciones" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-felicidad_y_cultura")

    try:
        deleteQuery = "DELETE FROM `contento-bi.Felicidad_y_Cultura.Afirmaciones` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = tabla_afirmaciones_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-felicidad_y_cultura')
    blob = bucket.blob("Clima/afirmaciones" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    return " Cargue exitoso Tabla de afirmaciones" + "flowAnswer" 

######################################## TABLA REL INTEGRA ##############################################

@clima_api.route("/integra")
def integra():

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Clima_integra"')
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

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_rel_integra,id_criterio,id_categoria,id_subcategoria,id_afirmacion FROM ' + tabla_bd)
    #    conn.execute_query('SELECT id_registro,documento,id_afirmacion,nota,fecha_registro FROM ' + TABLE_DB + " WHERE CONVERT(DATE, fecha_registro) = CONVERT(DATE,GETDATE())")
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_rel_integra']).encode('utf-8') + "|"
        text_row += str(row['id_criterio']).encode('utf-8') + "|"
        text_row += str(row['id_categoria']).encode('utf-8') + "|"
        text_row += str(row['id_subcategoria']).encode('utf-8') + "|"
        text_row += str(row['id_afirmacion']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Clima/integracion" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-felicidad_y_cultura")

    try:
        deleteQuery = "DELETE FROM `contento-bi.Felicidad_y_Cultura.Integracion` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = tabla_integracion_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-felicidad_y_cultura')
    blob = bucket.blob("Clima/integracion" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    return " Cargue exitoso Tabla de integracion" + "flowAnswer" 


######################################### TABLA SUBCATEGORIA ########################################

@clima_api.route("/subcategoria")
def subcategoria():

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Clima_subcategoria"')
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

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_subcategoria,desc_subcategoria FROM ' + tabla_bd)
    #    conn.execute_query('SELECT id_registro,documento,id_afirmacion,nota,fecha_registro FROM ' + TABLE_DB + " WHERE CONVERT(DATE, fecha_registro) = CONVERT(DATE,GETDATE())")
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_subcategoria']).encode('utf-8') + "|"
        text_row += str(row['desc_subcategoria']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Clima/subcategoria" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-felicidad_y_cultura")

    try:
        deleteQuery = "DELETE FROM `contento-bi.Felicidad_y_Cultura.Subcategoria` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = tabla_subcategoria_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-felicidad_y_cultura')
    blob = bucket.blob("Clima/subcategoria" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    return " Cargue exitoso Tabla de subcategoria" + "flowAnswer" 


############################################### TABLA CRITERIO ######################################

@clima_api.route("/criterio")
def criterio():

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Clima_criterio"')
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

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_criterio,descripcion_criterio FROM ' + tabla_bd)
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_criterio']).encode('utf-8') + "|"
        text_row += str(row['descripcion_criterio']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Clima/criterio" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-felicidad_y_cultura")

    try:
        deleteQuery = "DELETE FROM `contento-bi.Felicidad_y_Cultura.Criterio` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = tabla_criterio_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-felicidad_y_cultura')
    blob = bucket.blob("Clima/criterio" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    return " Cargue exitoso Tabla de criterios" + "flowAnswer" 


############################################### TABLA CATEGORIA ###########################################

@clima_api.route("/categoria")
def categoria():

    client = bigquery.Client()
    QUERY = ('select Tabla,servidor,usuario,contrasena,data_base,tabla_bd from `contento-bi.Contento.Usuario_conexion_bases` where Tabla = "Clima_categoria"')
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

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=servidor, user=usuario, password=contrasena, database=data_base)
    conn.execute_query('SELECT id_categoria,desc_categoria FROM ' + tabla_bd)
    
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['id_categoria']).encode('utf-8') + "|"
        text_row += str(row['desc_categoria']).encode('utf-8')
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Clima/categoria" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-felicidad_y_cultura")

    try:
        deleteQuery = "DELETE FROM `contento-bi.Felicidad_y_Cultura.Categoria` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = tabla_categoria_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-felicidad_y_cultura')
    blob = bucket.blob("Clima/categoria" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    return " Cargue exitoso Tabla de categoria" + "flowAnswer" 