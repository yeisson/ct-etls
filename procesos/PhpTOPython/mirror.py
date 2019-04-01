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

mirror_api = Blueprint('mirror_api', __name__)
fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]



#####################################################################################################################################
##################################################### BD_CONSOLIDADO ################################################################
#####################################################################################################################################
#####################################################################################################################################



@mirror_api.route("/delete", methods=['GET'])
def delete():

#Parametros GET para modificar la consulta segun los parametros entregados
    id_cliente = request.args.get('id_cliente')
    producto = request.args.get('producto')
    sub_producto = request.args.get('sub_producto')


    return " Esta informacion corresponde a python: id = "  + id_cliente + " Producto = " + producto + " Sub Producto = " + sub_producto
