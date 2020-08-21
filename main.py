# -*- coding: utf-8 -*-
from __future__ import print_function, absolute_import
from flask import Flask, request, jsonify, redirect
from flask_cors import CORS
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from google.cloud import datastore
from google.cloud import bigquery
import logging
import uuid
import json
import urllib3
import requests
import os
import dataflow_pipeline.massive as pipeline
import cloud_storage_controller.cloud_storage_controller as gcscontroller

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'OAuth2Credential.json'
app = Flask(__name__, static_url_path='/')
CORS(app)


# Telefonía <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<INICIO>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

from procesos.Telefonia.agent_detail import agent_detail_api
from procesos.Telefonia.login_logout import login_logout_api
from procesos.Telefonia.csat import csat_api
from procesos.Telefonia.agent_status import agent_status_api
from procesos.Telefonia.cdr import cdr_api
from procesos.Telefonia.remover import remover_api
from procesos.Telefonia.skill_detail import skill_detail_api
from procesos.Telefonia.tester import tester_api
from procesos.Telefonia.campaign import webpage_api
from procesos.Telefonia.chats import chats_api
from procesos.Telefonia.sms import sms_api
from procesos.Telefonia.ad_consumo import ad_consumo_api
from procesos.Telefonia.ad_productos import ad_productos_api
from procesos.Telefonia.agent_detail import agent_detail_api


app.register_blueprint(agent_detail_api, url_prefix='/telefonia')
app.register_blueprint(login_logout_api, url_prefix='/telefonia')
app.register_blueprint(csat_api, url_prefix='/telefonia')
app.register_blueprint(agent_status_api, url_prefix='/telefonia')
app.register_blueprint(cdr_api, url_prefix='/telefonia')
app.register_blueprint(remover_api, url_prefix='/telefonia')
app.register_blueprint(skill_detail_api, url_prefix='/telefonia')
app.register_blueprint(tester_api, url_prefix='/telefonia')
app.register_blueprint(webpage_api, url_prefix='/telefonia')
app.register_blueprint(agent_detail_api, url_prefix='/telefonia')
app.register_blueprint(ad_consumo_api, url_prefix='/telefonia')
app.register_blueprint(ad_productos_api, url_prefix='/telefonia')



# Telefonía <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<FIN>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# BI <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<INICIO>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
from procesos.bancolombia import bancolombia_api
from procesos.avon import avon_api
from procesos.avon_2 import avon_2_api
from procesos.negociadores import negociadores_api
from procesos.leonisa import leonisa_api
from procesos.bancolombia_castigada import bancolombia_castigada_api
from procesos.tuya import tuya_api
from procesos.bancamia import bancamia_api
from procesos.linead import linead_api
from procesos.avalcreditos import avalcreditos_api
from procesos.epm import epm_api
from procesos.agaval import agaval_api
from procesos.crediorbe import crediorbe_api
from procesos.adeinco_juridico import adeinco_juridico_api
from procesos.refinancia import refinancia_api
from procesos.cotrafa import cotrafa_api
from procesos.universidad_cooperativa_col import universidad_cooperativa_col_api
from procesos.descargas import descargas_api
from procesos.fanalca_agendamientos import fanalca_agendamientos_api
from procesos.fanalca import fanalca_api
from procesos.cesde import cesde_api
from procesos.rappi import rappi_api
from procesos.pyg import pyg_api
from procesos.metlife import Metlife_BM_api
# from procesos.Prueba import Prueba_api
from procesos.metlife import Metlife_BM_descarga_api
from procesos.refinancia import Refinancia_descarga_api
from metlife_base_marcada.server import metlife_base_marcada_api
from refinancia_base_marcada.server import refinancia_base_marcada_api
from procesos.ucc import ucc_api
from procesos.Hermeco import Hermeco_api

from procesos.Bridge.bridge import bridge_api
from procesos.PhpTOPython.mirror import mirror_api
from WebPage.inicio import webpage_app
from procesos.bancolombia_api import bancolombia_api2
from ui import ui_api
from procesos.mobility import mobility_api
from procesos.unificadas import unificadas_api

from procesos.turnos import turnos_api
from procesos.sensus import sensus_api
from procesos.presupuesto import presupuesto_api
from procesos.dispersion import dispersion_api
from procesos.proteccion import proteccion_api

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'OAuth2Credential.json'

app = Flask(__name__, static_url_path='/')
CORS(app)

app.register_blueprint(login_logout_api, url_prefix='/telefonia')
app.register_blueprint(csat_api, url_prefix='/telefonia')
app.register_blueprint(agent_status_api, url_prefix='/telefonia')
app.register_blueprint(cdr_api, url_prefix='/telefonia')
app.register_blueprint(remover_api, url_prefix='/telefonia')
app.register_blueprint(skill_detail_api, url_prefix='/telefonia')
app.register_blueprint(tester_api, url_prefix='/telefonia')
app.register_blueprint(webpage_api, url_prefix='/telefonia')
app.register_blueprint(chats_api, url_prefix='/telefonia')
app.register_blueprint(sms_api, url_prefix='/telefonia')
app.register_blueprint(agent_detail_api, url_prefix='/telefonia')

app.register_blueprint(bancolombia_api, url_prefix='/bancolombia')
app.register_blueprint(avon_api, url_prefix='/avon')
app.register_blueprint(avon_2_api, url_prefix='/avon_2')
app.register_blueprint(negociadores_api, url_prefix='/negociadores')
app.register_blueprint(leonisa_api, url_prefix='/leonisa')
app.register_blueprint(bancolombia_castigada_api, url_prefix='/bancolombia_castigada')
app.register_blueprint(tuya_api, url_prefix='/tuya')
app.register_blueprint(bancamia_api, url_prefix='/bancamia')
app.register_blueprint(linead_api, url_prefix='/linead')
app.register_blueprint(avalcreditos_api, url_prefix='/avalcreditos')
app.register_blueprint(epm_api, url_prefix='/epm')
app.register_blueprint(agaval_api, url_prefix='/agaval')
app.register_blueprint(crediorbe_api, url_prefix='/crediorbe')
app.register_blueprint(adeinco_juridico_api, url_prefix='/adeinco_juridico')
app.register_blueprint(refinancia_api, url_prefix='/refinancia')
app.register_blueprint(cotrafa_api, url_prefix='/cotrafa')
app.register_blueprint(universidad_cooperativa_col_api, url_prefix='/universidad_cooperativa_col')
app.register_blueprint(descargas_api, url_prefix='/descargas')
app.register_blueprint(fanalca_api, url_prefix='/fanalca')
app.register_blueprint(fanalca_agendamientos_api, url_prefix='/fanalca_agendamientos')
app.register_blueprint(cesde_api, url_prefix='/cesde')
app.register_blueprint(rappi_api, url_prefix='/rappi')
app.register_blueprint(pyg_api, url_prefix='/pyg')
app.register_blueprint(Metlife_BM_api, url_prefix='/metlife')
# app.register_blueprint(Prueba_api, url_prefix='/Prueba')
app.register_blueprint(Metlife_BM_descarga_api, url_prefix='/metlife')
app.register_blueprint(Refinancia_descarga_api, url_prefix='/refinancia')
app.register_blueprint(metlife_base_marcada_api, url_prefix='/metlife_base_marcada')
app.register_blueprint(refinancia_base_marcada_api, url_prefix='/refinancia_base_marcada')


app.register_blueprint(bancolombia_api2, url_prefix='/bancolombia_adm_api')
app.register_blueprint(ui_api, url_prefix='/ui')
app.register_blueprint(mobility_api, url_prefix='/mobility')
app.register_blueprint(unificadas_api, url_prefix='/unificadas')
# app.register_blueprint(ucc_api, url_prefix='/ucc')
app.register_blueprint(proteccion_api, url_prefix='/proteccion')
app.register_blueprint(ucc_api, url_prefix='/ucc')
app.register_blueprint(Hermeco_api, url_prefix='/hermeco')



# BI <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<FIN>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# Dirección Leonel Henao <<<<<<<<<<<<<<<<<<<<<<<<<<<<INICIO>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

from procesos.turnos import turnos_api
from procesos.sensus import sensus_api
from procesos.presupuesto import presupuesto_api
from procesos.dispersion import dispersion_api
from procesos.bdsensus import bdsensus_api
from procesos.Telefonia.ivr import ivr_api
from procesos.Telefonia.iwdetail import iwdetail_api
from procesos.facturas import facturas_api
from procesos.Zendesk.historico import historico_api
from procesos.Zendesk.organizations import organizations_api
from procesos.Zendesk.user import user_api
from procesos.Zendesk.Ofima import Ofima_api
from procesos.Zendesk.Tickets import Tickets_api

app.register_blueprint(turnos_api, url_prefix='/turnos')
app.register_blueprint(sensus_api, url_prefix='/sensus')
app.register_blueprint(presupuesto_api, url_prefix='/presupuesto')
app.register_blueprint(dispersion_api, url_prefix='/dispersion')
app.register_blueprint(bdsensus_api, url_prefix='/bd')
app.register_blueprint(ivr_api, url_prefix='/telefonia')
app.register_blueprint(iwdetail_api, url_prefix='/telefonia')
app.register_blueprint(facturas_api, url_prefix='/facturas')
app.register_blueprint(Ofima_api, url_prefix='/Ofima')
app.register_blueprint(Tickets_api, url_prefix='/Ofima')
app.register_blueprint(historico_api, url_prefix='/Ofima')
app.register_blueprint(organizations_api, url_prefix='/Ofima')
app.register_blueprint(user_api, url_prefix='/Ofima')
app.secret_key=os.urandom(24) 


# Dirección Leonel Henao <<<<<<<<<<<<<<<<<<<<<<<<<<<<FIN>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<



# TECH <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<INICIO>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
from procesos.Bridge.bridge import bridge_api
from procesos.PhpTOPython.mirror import mirror_api
from WebPage.inicio import webpage_app
from procesos.Contento_tech.Tof.tof import tof_api
from procesos.Contento_tech.BancolombiaSac.BancolombiaSac import bancosac_api
from procesos.Contento_tech.proyectoFC.profitto import profitto_api


app.register_blueprint(bridge_api, url_prefix='/bridge')
app.register_blueprint(mirror_api, url_prefix='/PhpTOPython')
app.register_blueprint(webpage_app, url_prefix='/webpage_app')
app.register_blueprint(tof_api, url_prefix='/tof')
app.register_blueprint(bancosac_api, url_prefix='/bancosac')
app.register_blueprint(profitto_api, url_prefix='/profitto')

#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<FIN>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


@app.route("/", methods=['GET', 'POST'])
def raiz():

    response = {}
    response["code"] = 200
    response["description"] = "Usa los Endpoints de cada servicio de acuerdo a la documentacion"

    return jsonify(response), 200

@app.route("/balance", methods=['GET', 'POST'])
def start_dataflow():

    #Obtenemos los parametros.
    filename = request.args.get('filename', default = '', type = str)

    if filename == '':
        response = {}
        response["code"] = 400
        response["description"] = "El valor del fichero en cloud storage (filename) es obligatorio"
        return jsonify(response), 400

    try:
        pipeline.run(filename)

        response = {}
        response["code"] = 200
        response["description"] = "Proceso iniciado correctamente, la sincronizacion terminara en unos momentos"
        response["input"] = filename

        return jsonify(response), 200
    except Exception as e:
        logging.exception(e)
        
        response = {}
        response["code"] = 500
        response["description"] = "Error: <pre>{}</pre>".format(e)

        return jsonify(response), 500

@app.errorhandler(500)
def server_error(e):
    logging.exception('Un error ha ocurrido durante la ejecucion')
    return """
    Un error a ocurrido durante la ejecucion: <pre>{}</pre>
    Visualiza los logs para tener una trama completa.
    """.format(e), 500

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
