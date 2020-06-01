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
	'chat_id:STRING,'
	'channel:STRING,'
	'chat_date:STRING,'
	'user_name:STRING,'
	'user_email:STRING,'
	'user_phone:STRING,'
	'user_chat_chars:STRING,'
	'agent_id:STRING,'
	'agent_name:STRING,'
	'agent_chat_chars:STRING,'
	'chat_duration:STRING,'
	'cod_act:STRING,'
	'comment:STRING,'
	'id_customer:STRING,'
	'agent_skill:STRING,'
	'user_id:STRING,'
	'id_cliente:STRING,'
	'cartera:STRING'
)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'chat_id': arrayCSV[0],
				'channel': arrayCSV[1],
				'chat_date': arrayCSV[2],
				'user_name': arrayCSV[3],
				'user_email': arrayCSV[4],
				'user_phone': arrayCSV[5],
				'user_chat_chars': arrayCSV[6],
				'agent_id': arrayCSV[7],
				'agent_name': arrayCSV[8],
				'agent_chat_chars': arrayCSV[9],
				'chat_duration': arrayCSV[10],
				'cod_act': arrayCSV[11],
				'comment': arrayCSV[12],
				'id_customer': arrayCSV[13],
				'agent_skill': arrayCSV[14],
				'user_id': arrayCSV[15],
				'id_cliente': arrayCSV[16],
				'cartera': arrayCSV[17]
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