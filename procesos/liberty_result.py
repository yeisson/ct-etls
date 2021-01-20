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
    myHeader = ["Id", "Nombre", "Cuenta_Gestiones", "Base", "Codigo_Tipificacion", "tel_number","Fecha_Gestion", "Campana", "Agent_name", "wpc", "rpc", "sin_contacto", "hit", "Source", "Intentos", "Gestionable", "Ipdial_code", "Producto"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 
##############################################################################################################################################################################3

@liberty_result.route("/descarga_consolidada", methods=['POST','GET'])

def Descarga_Resultado_Consolidado():
       
    dateini= request.args.get('mes_inicio')
    dateend= request.args.get('mes_final')

    if dateini is None: 
        dateini = ""
    else:
        dateini = dateini    

    if dateend is None: 
        dateend = ""
    else:
        dateend = dateend    
    # operacion= request.args.get('modulo_ipdial')

  
    myRoute = '/BI_Archivos/GOOGLE/Liberty/result_consolidado/'"gestion_consolidada"'.csv'
    # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   

    if dateini == "": 
        myQuery ='SELECT * FROM `contento-bi.Liberty.Exportable_Liberty_Consolidado`'
        
    else: 
        myQuery ='SELECT * FROM `contento-bi.Liberty.Exportable_Liberty_Consolidado`where mes_base between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'

    print (myQuery)
    myHeader = ["id_customer", "nombre", "cuenta_gestion", "cuenta_base", "codigo_tipificacion", "tel_number", "mes_base", "fecha_getion_efectiva","hora_getion_efectiva", "campana", "agent_name", "wpc", "rpc", "sin_contacto", "source", "intentos", "gestionable", "ipdial_code", "producto"
]
       

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 

 ##############################################################################################################################################################################

@liberty_result.route("/descarga_diaria_pichincha", methods=['POST','GET'])

def Descarga_Resultado_diario_pichincha():
       
    dateini= request.args.get('desde')
    dateend= request.args.get('hasta')
    ##operacion= request.args.get('modulo_ipdial')

  
    myRoute = '/BI_Archivos/GOOGLE/Liberty/result_diario/'+dateini+'_'+dateend+'_'+"pichincha"+'.csv'
    # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   
    myQuery ='SELECT * FROM `contento-bi.Liberty.Exportable_pichincha_bdo_Dirario` where Fecha_Gestion between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'

    print (myQuery)
    myHeader = ["id_customer", "placa", "nombre", "mes_de_campana", "cuenta_gestion", "cuenta_base", "codigo_tipificacion", "fecha_gestion", "campana", "agent_name", "wpc", "rpc", "sin_contacto", "hit", "source", "barridos", "gestionable", "ipdial_code", "producto"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)

##############################################################################################################################################################################3

@liberty_result.route("/descarga_consolidada_pichincha", methods=['POST','GET'])

def Descarga_Resultado_Consolidado_pichincha():
       
    ##dateini= request.args.get('desde')
   ## dateend= request.args.get('hasta')
    ##operacion= request.args.get('modulo_ipdial')

  
    myRoute = '/BI_Archivos/GOOGLE/Liberty/result_consolidado/'"gestion_consolidada_pichincha"'.csv'
    # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   
    myQuery ='SELECT * FROM `contento-bi.Liberty.Exportable_pichincha_bdo_consolidado`'

    print (myQuery)
    myHeader = ["id_customer", "placa", "nombre", "mes_de_campana", "cuenta_gestion", "cuenta_base", "codigo_tipificacion", "fecha_gestion", "campana", "agent_name", "wpc", "rpc", "sin_contacto", "hit", "source", "barridos", "gestionable", "ipdial_code", "producto"
]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 


# ############################# DESCARGA GESTIONES DIARIAS LIBERTY ########################

# @liberty_result.route("/gestiones_diarias", methods=['POST','GET'])

# def Descarga_gestiones():
       
#     dateini= request.args.get('desde')
#     dateend= request.args.get('hasta')
#     ##operacion= request.args.get('modulo_ipdial')

  
#     myRoute = '/BI_Archivos/GOOGLE/Liberty/Canales digitales/'+"gestiones"+dateini+'_'+dateend+'.csv'
#     # myQuery ='SELECT * FROM `contento-bi.telefonia_vistas.claro_fija_cons` where periodo between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+'AND'+'"'operacion'"''   
#     myQuery ='SELECT * FROM `contento-bi.Liberty.descargue_gestiones_diarias` where cast(Fecha_Getion as date) between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'

#     print (myQuery)
#     myHeader = ["Id_custumer", "Nombre", "Cuenta_Gestion", "cuenta_Base", "Codigo_Tipificacion","Fecha_Gestion","Hora_gestion","numero_telefono", "Campana", "Agent_name", "wpc", "rpc", "sin_contacto", "hit", "Source", "Intentos", "Gestionable", "Ipdial_code", "Producto"
# ]
    
    

#     return descargas.descargar_csv(myRoute, myQuery, myHeader)

