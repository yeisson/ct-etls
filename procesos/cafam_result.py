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

cafam_result = Blueprint('cafam_result', __name__)

@cafam_result.route("/descarga_diaria_cafam", methods=['POST','GET'])

def Descarga_Resultado_diario_cafam():
       
    dateini= request.args.get('desde')
    dateend= request.args.get('hasta')
    ##operacion= request.args.get('modulo_ipdial')

  
    myRoute = '/BI_Archivos/GOOGLE/Cafam/result_diario/'+dateini+'_'+dateend+'_'+"cafam"+'.csv'
    # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   
    myQuery ='SELECT * FROM `contento-bi.cafam.Exportable_cafam_diario` where Fecha_Gestion between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'

    print (myQuery)
    myHeader = ["id_customer", "nombre", "cuenta_gestion", "cuenta_base", "codigo_tipificacion", "fecha_gestion", "campana", "agent_name", "wpc", "rpc", "sin_contacto", "hit", "source", "intentos", "gestionable", "ipdial_code", "producto"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)

##############################################################################################################################################################################3

@cafam_result.route("/descarga_consolidada_cafam", methods=['POST','GET'])

def Descarga_Resultado_Consolidado_cafam():
       
    ##dateini= request.args.get('desde')
   ## dateend= request.args.get('hasta')
    ##operacion= request.args.get('modulo_ipdial')

  
    myRoute = '/BI_Archivos/GOOGLE/Cafam/result_consolidado/'"gestion_consolidada_cafam"'.csv'
    # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   
    myQuery ='SELECT * FROM `contento-bi.cafam.Exportable_cafam_consolidado`'

    print (myQuery)
    myHeader = ["id_customer", "nombre", "cuenta_gestion", "cuenta_base", "codigo_tipificacion", "fecha_gestion", "campana", "agent_name", "wpc", "rpc", "sin_contacto", "hit", "source", "intentos", "gestionable", "ipdial_code", "producto"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 




