#coding: utf-8 
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

SCHEMA_BASE = (
	'idkey:STRING, '
	'fecha:STRING, '
)

SCHEMA_PRUEBA_A = (
	'CAMPO_A:STRING'
)

SCHEMA_PRUEBA_B = (
	'CAMPO_B:STRING, '
	'CAMPO_B1:STRING'
)

TABLE_SCHEMA = ('idkey:STRING, '
				'fecha:STRING, '
				'CONSECUTIVO_GESTION:STRING, '
				'CONSECUTIVO_OBLIGACION:STRING, '
				'NIT:STRING, '
				'NRO_DOCUMENTO:STRING, '
				'FECHA_GESTION:STRING, '
				'GRABADOR:STRING, '
				'CODIGO_DE_GESTION:STRING, '
				'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO:STRING '
				)

class formatearData(beam.DoFn):

	def __init__(self, mifecha, variable):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha': self.mifecha,
				'CONSECUTIVO_GESTION' : arrayCSV[0],
				'CONSECUTIVO_OBLIGACION' : arrayCSV[1]
				}
		
		return [tupla]

# def run(archivo, mifecha):
def run(archivo, mifecha, variable):

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
        # "--num_workers", "30",
        # "--autoscaling_algorithm", "NONE"		
	])
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha, variable)))

	# transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
	# 	gcs_project + ":bancolombia_castigada.debitos", 
	# 	schema=TABLE_SCHEMA, 
	# 	create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
	# 	write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
	# 	)

	# jobObject = pipeline.run()

	SCHEMA_FINAL = SCHEMA_BASE

	if variable == 'a':
		SCHEMA_FINAL = SCHEMA_BASE + SCHEMA_PRUEBA_A
	else:
		SCHEMA_FINAL = SCHEMA_BASE + SCHEMA_PRUEBA_B

	# return ("Corrio Full HD")
	# return SCHEMA_FINAL
	return transformed(0)



