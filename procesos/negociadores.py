from flask import Blueprint
# import dataflow_pipeline.personal_contento.carga_negociadores_metas_beam as carga_negociadores_metas_beam
import dataflow_pipeline.personal.negociadores_beam as negociadores_beam
import dataflow_pipeline.personal.negociadores_2_beam as negociadores_2_beam

negociadores_api = Blueprint('negociadores_api', __name__)


@negociadores_api.route("/asignacion")
def asignacion():
    mensaje = negociadores_beam.run()
    return "Corriendo : " + mensaje

@negociadores_api.route("/asignacion2")
def asignacion2():
    mensaje = negociadores_2_beam.run()
    return "Corriendo : " + mensaje


