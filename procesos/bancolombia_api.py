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
#Espiritu santo de Dios, que sean tus manos tirando este codigo. en el nombre de JESUS. amen y amen
#######################################################################################################################

@bancolombia_api2.route("/api", methods=['POST','GET'])
def api():

    client = bigquery.Client()
    fecha = time.strftime('%Y%m%d')
    dinip= request.args.get('dateini')
    dendp= request.args.get('dateend')
    token= request.args.get('token')
    tokenq = "AFRV786989182391827898-2312"
    limit = request.args.get('limit')
    ip = request.remote_addr
    ip_allowed = ['127.0.0.1','192.168.108.2','192.168.8.189']
    mensaje_ip_no_autorizada = " No está autorizada para ingresar a esta API"
    token_incorrecto = "Ingrese un token válido"


    if ip not in ip_allowed:
        return ("La ip:" + ip + mensaje_ip_no_autorizada)

    if limit is None:
        queryt = ' FROM `bancolombia_admin.bm` where fecha = "' + str(dinip) + '" and fecha = "' + str(dendp) + '"'
    else:
        queryt = ' FROM `bancolombia_admin.bm` where fecha = "' + str(dinip) + '" and fecha = "' + str(dendp) + '" LIMIT ' + str(limit)

    if dinip is None:
        dinip = fecha
    else:
        dinip = request.args.get('dateini')
        
    if dendp is None:
        dendp = fecha
    else:
        dendp = request.args.get('dateend')
    
    if token <> tokenq:
        return(token_incorrecto)
    else:        
        QUERY = (
            'SELECT \
                fecha, \
                consecutivo_documento_deudor, \
                valor_cuota, \
                clasificacion_producto, \
                nit, \
                nombres, \
                numero_documento, \
                tipo_producto, \
                fecha_pago_cuota, \
                modalidad, \
                nombre_de_producto, \
                plan, \
                fecha_de_perfeccionamiento, \
                fecha_vencimiento_def, \
                numero_cuotas, \
                cant_oblig, \
                cuotas_en_mora, \
                dia_de_vencimiento_de_cuota, \
                valor_obligacion, \
                valor_vencido, \
                saldo_activo, \
                saldo_orden, \
                regional, \
                ciudad, \
                oficina_radicacion, \
                grabador, \
                nombre_asesor, \
                codigo_agente, \
                nombre_abogado, \
                codigo_abogado, \
                fecha_ultima_gestion_prejuridica, \
                ultimo_codigo_de_gestion_prejuridico, \
                ultimo_codigo_de_gestion_paralelo, \
                desc_ultimo_codigo_de_gestion_prejuridico, \
                codigo_anterior_de_gestion_prejuridico, \
                desc_codigo_anterior_de_gestion_prejuridico, \
                ultima_fecha_pago, \
                ultima_fecha_de_actuacion_juridica, \
                fecha_ultima_gestion_juridica, \
                ejec_ultimo_codigo_de_gestion_juridico, \
                rest_ultimo_codigo_gestion_juridico, \
                desc_ultimo_codigo_de_gestion_juridico, \
                tipo_de_cartera, \
                dias_mora, \
                calificacion, \
                radicacion, \
                estado_de_la_obligacion, \
                fondo_nacional_garantias, \
                region, \
                segmento, \
                fecha_importacion, \
                titular_universal, \
                negocio_titutularizado, \
                red, \
                fecha_traslado_para_cobro, \
                calificacion_real, \
                fecha_ultima_facturacion, \
                cuadrante, \
                causal, \
                sector_economico, \
                fecha_promesa, \
                endeudamiento, \
                probabilidad_de_propension_de_pago, \
                priorizacion_final, \
                grupo_de_priorizacion, \
                ultimo_abogado, \
                credito_cobertura_frech, \
                endeudamiento_sufi, \
                mono_multi, \
                grupo, \
                tipo_tc, \
                ciclo, \
                estrategia_cliente, \
                impacto, \
                provisiona, \
                rango_mora, \
                rango_gestion, \
                dia_para_rodar, \
                tipo_base, \
                rango_desfase, \
                fecha_ultima_gestion_adminfo, \
                desc_ultimo_codigo_de_gestion_adminfo, \
                mora_base_inicio, \
                estrategia_clasificacion, \
                estado_bini, \
                uvr_pesos, \
                reestruc_personas, \
                reestruct_micropyme, \
                tareas_estrategia, \
                alternativa, \
                hipo_titularizada, \
                inconsistencias, \
                no_gestionar, \
                excluir_bases, \
                priorizacion_piloto, \
                analisis_contactab, \
                grupo_de_priorizacion2, \
                prob_pago, \
                foco_piloto, \
                prioridad_banco_piloto, \
                marcar_referencias, \
                refuerzo_gestion, \
                franja_contacto, \
                hora_contacto, \
                dia_contacto, \
                cel, \
                contact, \
                clientes, \
                estrategia_micro, \
                estrategia_final, \
                region_final, \
                estrategia_clasificacion_final, \
                desfase_lotes, \
                debug, \
                nivel_base, \
                tarea, \
                marcacion  \
            ' + ' FROM `bancolombia_admin.bm` where fecha = "' + dinip + '" and fecha = "' + dendp + '" LIMIT ' + limit)
        query_job = client.query(QUERY)
        rows = query_job.result()

        items = []
        i = 0
        for row in rows:
            i = i+1
            items.append({
                'fecha': row[0],
                'consecutivo_documento_deudor': row[1],
                'valor_cuota': row[2],
                'clasificacion_producto': row[3],
                'nit': row[4],
                'nombres': row[5],
                'numero_documento': row[6],
                'tipo_producto': row[7],
                'fecha_pago_cuota': row[8],
                'modalidad': row[9],
                'nombre_de_producto': row[10],
                'plan': row[11],
                'fecha_de_perfeccionamiento': row[12],
                'fecha_vencimiento_def': row[13],
                'numero_cuotas': row[14],
                'cant_oblig': row[15],
                'cuotas_en_mora': row[16],
                'dia_de_vencimiento_de_cuota': row[17],
                'valor_obligacion': row[18],
                'valor_vencido': row[19],
                'saldo_activo': row[20],
                'saldo_orden': row[21],
                'regional': row[22],
                'ciudad': row[23],
                'oficina_radicacion': row[24],
                'grabador': row[25],
                'nombre_asesor': row[26],
                'codigo_agente': row[27],
                'nombre_abogado': row[28],
                'codigo_abogado': row[29],
                'fecha_ultima_gestion_prejuridica': row[30],
                'ultimo_codigo_de_gestion_prejuridico': row[31],
                'ultimo_codigo_de_gestion_paralelo': row[32],
                'desc_ultimo_codigo_de_gestion_prejuridico': row[33],
                'codigo_anterior_de_gestion_prejuridico': row[34],
                'desc_codigo_anterior_de_gestion_prejuridico': row[35],
                'ultima_fecha_pago': row[36],
                'ultima_fecha_de_actuacion_juridica': row[37],
                'fecha_ultima_gestion_juridica': row[38],
                'ejec_ultimo_codigo_de_gestion_juridico': row[39],
                'rest_ultimo_codigo_gestion_juridico': row[40],
                'desc_ultimo_codigo_de_gestion_juridico': row[41],
                'tipo_de_cartera': row[42],
                'dias_mora': row[43],
                'calificacion': row[44],
                'radicacion': row[45],
                'estado_de_la_obligacion': row[46],
                'fondo_nacional_garantias': row[47],
                'region': row[48],
                'segmento': row[49],
                'fecha_importacion': row[50],
                'titular_universal': row[51],
                'negocio_titutularizado': row[52],
                'red': row[53],
                'fecha_traslado_para_cobro': row[54],
                'calificacion_real': row[55],
                'fecha_ultima_facturacion': row[56],
                'cuadrante': row[57],
                'causal': row[58],
                'sector_economico': row[59],
                'fecha_promesa': row[60],
                'endeudamiento': row[61],
                'probabilidad_de_propension_de_pago': row[62],
                'priorizacion_final': row[63],
                'grupo_de_priorizacion': row[64],
                'ultimo_abogado': row[65],
                'credito_cobertura_frech': row[66],
                'endeudamiento_sufi': row[67],
                'mono_multi': row[68],
                'grupo': row[69],
                'tipo_tc': row[70],
                'ciclo': row[71],
                'estrategia_cliente': row[72],
                'impacto': row[73],
                'provisiona': row[74],
                'rango_mora': row[75],
                'rango_gestion': row[76],
                'dia_para_rodar': row[77],
                'tipo_base': row[78],
                'rango_desfase': row[79],
                'fecha_ultima_gestion_adminfo': row[80],
                'desc_ultimo_codigo_de_gestion_adminfo': row[81],
                'mora_base_inicio': row[82],
                'estrategia_clasificacion': row[83],
                'estado_bini': row[84],
                'uvr_pesos': row[85],
                'reestruc_personas': row[86],
                'reestruct_micropyme': row[87],
                'tareas_estrategia': row[88],
                'alternativa': row[89],
                'hipo_titularizada': row[90],
                'inconsistencias': row[91],
                'no_gestionar': row[92],
                'excluir_bases': row[93],
                'priorizacion_piloto': row[94],
                'analisis_contactab': row[95],
                'grupo_de_priorizacion2': row[96],
                'prob_pago': row[97],
                'foco_piloto': row[98],
                'prioridad_banco_piloto': row[99],
                'marcar_referencias': row[100],
                'refuerzo_gestion': row[101],
                'franja_contacto': row[102],
                'hora_contacto': row[103],
                'dia_contacto': row[104],
                'cel': row[105],
                'contact': row[106],
                'clientes': row[107],
                'estrategia_micro': row[108],
                'estrategia_final': row[109],
                'region_final': row[110],
                'estrategia_clasificacion_final': row[111],
                'desfase_lotes': row[112],
                'debug': row[113],
                'nivel_base': row[114],
                'tarea': row[115],
                'marcacion': row[116],
                'z_consecutivo': i
                })

    
    return  jsonify(items)