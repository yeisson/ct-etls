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
import socket

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText, textio
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
	'idkey:STRING, '
	'fecha:STRING, '
	'CREDITO_NRO:STRING, '
	'NIT:STRING, '
	'FECHA_GESTION:STRING, '
	'NOTA:STRING, '
	'GRABADOR:STRING, '
	'ESTADO_DE_COBRO:STRING, '
	'TELEFONO:STRING, '
	'ESTADO_CARTERA:STRING '

)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),	#datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'CREDITO_NRO' : arrayCSV[0],
				'NIT' : arrayCSV[1],
				'FECHA_GESTION' : arrayCSV[2],
				'NOTA' : arrayCSV[3],
				'GRABADOR' : arrayCSV[4],
				'ESTADO_DE_COBRO' : arrayCSV[5],
				'TELEFONO' : arrayCSV[6],
				'ESTADO_CARTERA' : arrayCSV[7]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-agaval" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	# pipeline =  beam.Pipeline(runner="DirectRunner")
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
        # "--num_workers", "30",
        # "--autoscaling_algorithm", "NONE"
	])
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT")
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/prejuridico/info_carga_banco_prej",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Agaval' >> beam.io.WriteToBigQuery(
		gcs_project + ":agaval.gestiones", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



