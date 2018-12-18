from __future__ import print_function, absolute_import

import logging
import re
import json
import requests
import uuid
import time
import os
import argparse
import uuid
import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
	#'idkey:STRING, '
	#'fecha:DATETIME, '
	'id_negociador:STRING, '
	'nombre_negociador:STRING, '
	'id_grabador:STRING, '
	'id_team_leader:STRING, '
	'nombre_team_leader:STRING, '
	'id_ejecutivo:STRING, '
	'nombre_ejecutivo:STRING, '
	'id_gerente:STRING, '
	'nombre_gerente:STRING, '
	'suboperacion:STRING, '
	'ciudad:STRING, '
	'operacion:STRING, '
	'nombre_producto:STRING, '
	'meta_gestiones_hora:STRING '
)
# ?
class formatearData(beam.DoFn):
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {#'idkey' : str(uuid.uuid4()),
				#'fecha' : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),	#datetime.datetime.today().strftime('%Y-%m-%d'),
				'id_negociador': arrayCSV[0],
				'nombre_negociador': arrayCSV[1],
				'id_grabador': arrayCSV[2],
				'id_team_leader': arrayCSV[3],
				'nombre_team_leader': arrayCSV[4],
				'id_ejecutivo': arrayCSV[5],
				'nombre_ejecutivo': arrayCSV[6],
				'id_gerente': arrayCSV[7],
				'nombre_gerente': arrayCSV[8],
				'suboperacion': arrayCSV[9],
				'ciudad': arrayCSV[10],
				'operacion': arrayCSV[11],
				'nombre_producto': arrayCSV[12],
				'meta_gestiones_hora': arrayCSV[13]
				}
		
		return [tupla]



def run():

	gcs_path = "gs://ct-bancolombia" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	pipeline =  beam.Pipeline(runner="DirectRunner")
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT")
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText("archivos/ASIGNACION_NEGOCIADORES_CONTENTO.csv", skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_negociadores", file_name_suffix='.csv',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/prejuridico/info_carga_banco_prej",file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
	# 	gcs_project + ":bancolombia_admin.prejuridico", 
	# 	schema=TABLE_SCHEMA, 
	# 	create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
	# 	write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
	# 	)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")



