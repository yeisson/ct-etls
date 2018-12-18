from flask import Blueprint
import dataflow_pipeline.avon.avon_prejuridico_beam as avon_prejuridico_beam

avon_api = Blueprint('avon_api', __name__)

@avon_api.route("/")
def inicio():
    return "Apis de Avon"

@avon_api.route("/prejuridico")
def prejuridico():
    mensaje = avon_prejuridico_beam.run()
    return "Corriendo : " + mensaje

@avon_api.route("/probando")
def probando():
    #mensaje = avon_pro_beam.run()
    return "Corriendo : "