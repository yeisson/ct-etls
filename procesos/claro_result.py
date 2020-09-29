from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import os
import socket
import procesos.descargas as descargas
import requests
from flask import request


fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

claro_result = Blueprint('claro_result', __name__)
@claro_result.route("/descargar", methods=['POST','GET'])

def Descarga_Encuesta():
       
    dateini= request.args.get('desde')
    dateend= request.args.get('hasta')
   
  
    myRoute = '/BI_Archivos/GOOGLE/Claro/result/'+dateini+'_'+dateend+'.csv'  
    myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'    
    myHeader = ["id_call", "periodo", "agent_identification", "agent_name", "id_customer", "nombre_cliente", "id_campaing", "date", "final_llamada", "tel_number", "duration", "typing_code", "descri_typing_code", "comment", "captura1", "captura2", "captura3", "captura4", "opt1", "opt2", "opt3", "opt4", "opt5", "opt6", "opt7", "opt8", "opt9", "opt10", "opt11", "opt12", "RANK"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 


