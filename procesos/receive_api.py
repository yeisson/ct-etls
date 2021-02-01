from flask import Blueprint
from google.cloud import bigquery
from flask import request

import time
import json


general_receive_api = Blueprint('receive_api', __name__)
@general_receive_api.route("/wolkvox", methods=['POST','GET'])

def receive_api():
       
    datos = request.args.get('dato') 
    origen = request.args.get('origen')    
    fecha = time.strftime("%Y-%m-%d %H:%M:%S")

         
    MyQuery = "INSERT INTO `contento-bi.telefonia.api_receive` VALUES ('"+ datos +"','"+ origen +"','"+ fecha + "')"

    client = bigquery.Client()
    query_job = client.query(MyQuery)
    query_job.result()

    return 'Registro Ingresado con Exito'



            
 
 


