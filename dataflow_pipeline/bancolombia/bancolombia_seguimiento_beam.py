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
from apache_beam.io import WriteToText
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
	'idkey:STRING, '
	'fecha:STRING, '
	'consecutivo_obligacion:STRING, '
	'nit:STRING, '
	'fecha_gestion:STRING, '
	'grabador:STRING, '
	'desc_ultimo_codigo_de_gestion_prejuridico:STRING, '
	'hora_grabacion:TIME, '
	'codigo_de_gestion:STRING, '
	't_igraba:TIME, '
	'dias_mora:STRING, '
	'duracion:TIME, '
	'regional:STRING, '
	'consecutivo_gestion:STRING, '
	'nro_documento:STRING, '
	'fecha_promesa:STRING, '
	'codigo_abogado:STRING, '
	'nombre_abogado:STRING, '
	't_entrada:STRING, '
	'codigo_de_cobro_anterior:STRING, '
	'fecha_corte:STRING, '
	'nota:STRING '

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
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'consecutivo_obligacion': arrayCSV[0],
				'nit': arrayCSV[1],
				'fecha_gestion': arrayCSV[2],
				'grabador': arrayCSV[3],
				'desc_ultimo_codigo_de_gestion_prejuridico': arrayCSV[4],
				'hora_grabacion': arrayCSV[5],
				'codigo_de_gestion': arrayCSV[6],
				't_igraba': arrayCSV[7],
				'dias_mora': arrayCSV[8],
				'duracion': arrayCSV[9].replace(' ','0:00:00'),
				'regional': arrayCSV[10],
				'consecutivo_gestion': arrayCSV[11],
				'nro_documento': arrayCSV[12],
				'fecha_promesa': arrayCSV[13],
				'codigo_abogado': arrayCSV[14],
				'nombre_abogado': arrayCSV[15],
				't_entrada': arrayCSV[16],
				'codigo_de_cobro_anterior': arrayCSV[17],
				'fecha_corte': arrayCSV[18],
				'nota': arrayCSV[19].replace('|',',')

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runer = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runer, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"	
	])
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)
	# transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))
	# transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
	# 	gcs_project + ":bancolombia_admin.seguimiento", 
	# 	schema=TABLE_SCHEMA, 
	# 	create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
	# 	write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
	# 	)

	jobObject = pipeline.run()
	return ("Corrio Full HD")



