from __future__ import print_function, absolute_import
import logging
import re
import json
import requests
import uuid
import time
import os
import socket
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

####################### PARAMETROS DE LA TABLA EN BQ ##########################

TABLE_SCHEMA = (
	'date:STRING,'
	'id_agent:STRING,'
	'name:STRING,'
	'id_queue:STRING,'
	'type_call:STRING,'
	'tel_number:STRING,'
	'cod_act:STRING,'
	'id_customer:STRING,'
	'comment:STRING,'
	'duration:STRING,'
	'hung_up:STRING,'
	'cost:STRING,'
	'id_campaing:STRING,'
	'id_call:STRING,'
	'id_cliente:STRING,'
	'ipdial_code:STRING,'
	'cartera:STRING'
)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'date': arrayCSV[0],
				'id_agent': arrayCSV[1],
				'name': arrayCSV[2],
				'id_queue': arrayCSV[3],
				'type_call': arrayCSV[4],
				'tel_number': arrayCSV[5],
				'cod_act': arrayCSV[6],
				'id_customer': arrayCSV[7],
				'comment': arrayCSV[8],
				'duration': arrayCSV[9],
				'hung_up': arrayCSV[10],
				'cost': arrayCSV[11],
				'id_campaing': arrayCSV[12],
				'id_call': arrayCSV[13],
				'id_cliente': arrayCSV[14],
				'ipdial_code': arrayCSV[15],
				'cartera': arrayCSV[16]
				}
		return [tupla]

############################ CODIGO DE EJECUCION ###################################

def run(output,KEY_REPORT):

	gcs_path = 'gs://ct-telefonia' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "15",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(output)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	
	transformed | 'Escritura a BigQuery Telefonia' >> beam.io.WriteToBigQuery(
		gcs_project + ":telefonia." + KEY_REPORT, 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")