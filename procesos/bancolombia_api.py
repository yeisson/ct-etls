# -*- coding: utf-8 -*-

from flask import Blueprint
from flask import jsonify
from flask import request
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from google.cloud import datastore
from google.cloud import bigquery
import os
import socket
import datetime
import time
import json
import sys
import requests

bancolombia_api2 = Blueprint('bancolombia_api2', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

#######################################################################################################################
#Espíritu santo de DIOS, que sean tus manos tirando este código. en el nombre de JESÚS. amén y amén
#######################################################################################################################

##URL DE INVOCACIÓN:
# http://contentobps.contentobi.com:5000/bancolombia_adm_api/api
# PARÁMETROS:
# cedula = número de consecutivo de deudor en ADMINFO
# token = solo puede acceder al api quién tenga el TOKEN de autorización


@bancolombia_api2.route("/api", methods=['POST','GET'])
def api():

    client = bigquery.Client()
    fecha = time.strftime('%Y%m%d')
    # dinip= request.args.get('dateini')
    # dendp= request.args.get('dateend')
    token= request.args.get('token')
    tokenq = "AFRV786989182391827898-2312"
    id_cliente = request.args.get('cedula')
    ip = request.remote_addr
    ip_allowed = ['192.168.8.189','181.129.43.106','179.18.8.255']
    mensaje_ip_no_autorizada = " No está autorizada para ingresar a esta API"
    token_incorrecto = "Ingrese un token válido"


    if ip not in ip_allowed:
        return ("La ip:" + ip + mensaje_ip_no_autorizada)

    # if dinip is None:
    #     dinip = fecha
    # else:
    #     dinip = request.args.get('dateini')
        
    # if dendp is None:
    #     dendp = fecha
    # else:
    #     dendp = request.args.get('dateend')

    if id_cliente is None:
        return("Por favor ingrese una cédula")
    else:
        queryt = ' FROM `bancolombia_admin.bm` where consecutivo_documento_deudor = "' + id_cliente + '"'

    if token <> tokenq:
        return(token_incorrecto)
    else:        
        QUERY = ('SELECT * ' + queryt)
        query_job = client.query(QUERY)
        rows = query_job.result()

        items = []
        i = 0
        for row in rows:
            i = i+1
            items.append({
                'fecha': row[1],
                'consecutivo_documento_deudor': row[2],
                'valor_cuota': row[3],
                'clasificacion_producto': row[4],
                'nit': row[5].replace('1','4').replace('2','7').replace('3','0').replace('4','2').replace('5','9').replace('6','3').replace('7','5').replace('8','6').replace('9','1').replace('0','8'),
                'nombres': row[6],
                'numero_documento': row[7],
                'tipo_producto': row[8],
                'fecha_pago_cuota': row[9],
                'modalidad': row[10],
                'nombre_de_producto': row[11],
                'plan': row[12],
                'fecha_de_perfeccionamiento': row[13],
                'fecha_vencimiento_def': row[14],
                'numero_cuotas': row[15],
                'cant_oblig': row[16],
                'cuotas_en_mora': row[17],
                'dia_de_vencimiento_de_cuota': row[18],
                'valor_obligacion': row[19],
                'valor_vencido': row[20],
                'saldo_activo': row[21],
                'saldo_orden': row[22],
                'regional': row[23],
                'ciudad': row[24],
                'oficina_radicacion': row[25],
                'grabador': row[26],
                'nombre_asesor': row[27],
                'codigo_agente': row[28],
                'nombre_abogado': row[29],
                'codigo_abogado': row[30],
                'fecha_ultima_gestion_prejuridica': row[31],
                'ultimo_codigo_de_gestion_prejuridico': row[32],
                'ultimo_codigo_de_gestion_paralelo': row[33],
                'desc_ultimo_codigo_de_gestion_prejuridico': row[34],
                'codigo_anterior_de_gestion_prejuridico': row[35],
                'desc_codigo_anterior_de_gestion_prejuridico': row[36],
                'ultima_fecha_pago': row[37],
                'ultima_fecha_de_actuacion_juridica': row[38],
                'fecha_ultima_gestion_juridica': row[39],
                'ejec_ultimo_codigo_de_gestion_juridico': row[40],
                'rest_ultimo_codigo_gestion_juridico': row[41],
                'desc_ultimo_codigo_de_gestion_juridico': row[42],
                'tipo_de_cartera': row[43],
                'dias_mora': row[44],
                'calificacion': row[45],
                'radicacion': row[46],
                'estado_de_la_obligacion': row[47],
                'fondo_nacional_garantias': row[48],
                'region': row[49],
                'segmento': row[50],
                'fecha_importacion': row[51],
                'titular_universal': row[52],
                'negocio_titutularizado': row[53],
                'red': row[54],
                'fecha_traslado_para_cobro': row[55],
                'calificacion_real': row[56],
                'fecha_ultima_facturacion': row[57],
                'cuadrante': row[58],
                'causal': row[59],
                'sector_economico': row[60],
                'fecha_promesa': row[61],
                'endeudamiento': row[62],
                'probabilidad_de_propension_de_pago': row[63],
                'priorizacion_final': row[64],
                'grupo_de_priorizacion': row[65],
                'ultimo_abogado': row[66],
                'credito_cobertura_frech': row[67],
                'endeudamiento_sufi': row[68],
                'mono_multi': row[69],
                'grupo': row[70],
                'tipo_tc': row[71],
                'ciclo': row[72],
                'estrategia_cliente': row[73],
                'impacto': row[74],
                'provisiona': row[75],
                'rango_mora': row[76],
                'rango_gestion': row[77],
                'dia_para_rodar': row[78],
                'tipo_base': row[79],
                'rango_desfase': row[80],
                'fecha_ultima_gestion_adminfo': row[81],
                'desc_ultimo_codigo_de_gestion_adminfo': row[82],
                'mora_base_inicio': row[83],
                'estrategia_clasificacion': row[84],
                'estado_bini': row[85],
                'uvr_pesos': row[86],
                'reestruc_personas': row[87],
                'reestruct_micropyme': row[88],
                'tareas_estrategia': row[89],
                'alternativa': row[90],
                'hipo_titularizada': row[91],
                'inconsistencias': row[92],
                'no_gestionar': row[93],
                'excluir_bases': row[94],
                'priorizacion_piloto': row[95],
                'analisis_contactab': row[96],
                'grupo_de_priorizacion2': row[97],
                'prob_pago': row[98],
                'foco_piloto': row[99],
                'prioridad_banco_piloto': row[100],
                'marcar_referencias': row[101],
                'refuerzo_gestion': row[102],
                'franja_contacto': row[103],
                'hora_contacto': row[104],
                'dia_contacto': row[105],
                'cel': row[106],
                'contact': row[107],
                'clientes': row[108],
                'estrategia_micro': row[109],
                'estrategia_final': row[110],
                'region_final': row[111],
                'estrategia_clasificacion_final': row[112],
                'desfase_lotes': row[113],
                'debug': row[114],
                'nivel_base': row[115],
                'tarea': row[116],
                'marcacion': row[117],
                'z_consecutivo': i
                })

    
    return  jsonify(items)