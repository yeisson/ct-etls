from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.metlife.Metlife_BM_beam as Metlife_BM_beam
import dataflow_pipeline.metlife.metlife_seguimiento_beam as metlife_seguimiento_beam
import os
import socket
import procesos.descargas as descargas
import requests
from flask import request


fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

Metlife_Rep_Wolkvox_api = Blueprint('Metlife_Rep_Wolkvox_api', __name__)
@Metlife_Rep_Wolkvox_api.route("/descargar", methods=['POST','GET'])

def Metlife_Rep_descarga_Base():

    operacion=request.args.get('operacion')
    idcampana= request.args.get('idcampana')
    dateini= request.args.get('desde')
    dateend= request.args.get('hasta')
   
  
    myRoute = '/BI_Archivos/GOOGLE/Metlife/Bases_Wolkvox/'+operacion+'_'+ idcampana +'_'+dateini+'_'+dateend+'.csv'  
    myQuery ='SELECT * FROM `contento-bi.MetLife.Descarga_Bases_Wolkvox` where (Fecha_gestion between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'+') AND id_campana = '+'"'+idcampana+'"'+'AND ipdial_code ='+'"'+operacion+'"'    
    myHeader = ["nombre_cliente","apellido_cliente","tipo_doc","id_doc_cliente","sexo","pais","departamento","ciudad","zona","direccion","opt1","opt2","opt3","opt4","opt5","opt6","opt7","opt8","opt9","opt10","opt11","opt12","tel1","tel2","tel3","tel4","tel5","tel6","tel7","tel8","tel9","tel10","tel_extra","id_agent","fecha","llamadas","id_call","rellamada","resultado","cod_rslt1","cod_rslt2","rellamada_count","id_cliente","ipdial_code","rand_token","hora","id_campana","fecha_cargue","Fecha_gestion"]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 


