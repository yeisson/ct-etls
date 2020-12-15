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

liberty_result = Blueprint('liberty_result', __name__)
@liberty_result.route("/descargar", methods=['POST','GET'])

def Descarga_Resultado():
       
    dateini= request.args.get('desde')
    dateend= request.args.get('hasta')
    ##operacion= request.args.get('modulo_ipdial')

  
    myRoute = '/BI_Archivos/GOOGLE/Liberty/result_diario/'+dateini+'_'+dateend+'.csv'
    # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   
    myQuery ='SELECT * FROM `contento-bi.Liberty.Exportable_Liberty_diario` where Fecha_Gestion between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'

    print (myQuery)
    myHeader = ["Id", "Nombre", "Cuenta_Gestiones", "Base", "Codigo_Tipificacion", "Fecha_Gestion", "Campana", "Agent_name", "wpc", "rpc", "sin_contacto", "hit", "Source", "Intentos", "Gestionable", "Ipdial_code", "Producto"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 
##############################################################################################################################################################################3

@liberty_result.route("/descarga_consolidada", methods=['POST','GET'])

def Descarga_Resultado_Consolidado():
       
    ##dateini= request.args.get('desde')
   ## dateend= request.args.get('hasta')
    ##operacion= request.args.get('modulo_ipdial')

  
    myRoute = '/BI_Archivos/GOOGLE/Liberty/result_consolidado/.csv'
    # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   
    myQuery ='SELECT * FROM `contento-bi.Liberty.Exportable_Liberty_Consolidado`'

    print (myQuery)
    myHeader = ["Id", "Nombre", "Cuenta_Gestiones", "Base", "Codigo_Tipificacion", "Fecha_Gestion", "Campana", "Agent_name", "wpc", "rpc", "sin_contacto", "hit", "Source", "Intentos", "Gestionable", "Ipdial_code", "Producto"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 

