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

metlife_result = Blueprint('metlife_result', __name__)
@metlife_result.route("/descargar", methods=['POST','GET'])

def Descarga_Resultado():
       
    dateini= request.args.get('desde')
    dateend= request.args.get('hasta')
    operacion= request.args.get('modulo_ipdial')

  
    myRoute = '/BI_Archivos/GOOGLE/Metlife/result/'+dateini+'_'+dateend+'_'+operacion+'.csv'
    # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   
    myQuery ='SELECT * FROM `contento-bi.MetLife.Exportable_Duraciones` where (date between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+') AND ipdial_code ='+'"'+operacion+'"'    

    print (myQuery)
    myHeader = ["date", "hora_llamada", "id_agent", "cod_act", "duration", "id_call", "id_customer", "ipdial_code"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 


 

