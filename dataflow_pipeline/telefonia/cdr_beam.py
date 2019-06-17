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
	'id_call:STRING,'
	'type_call:STRING,'
	'talk_time:STRING,'
	'id_agent:STRING,'
	'agent_name:STRING,'
	'agent_identification:STRING,'
	'skill:STRING,'
	'date:STRING,'
	'hour:STRING,'
	'day_of_week:STRING,'
	'typing_code:STRING,'
	'descri_typing_code:STRING,'
	'typing_code2:STRING,'
	'descri_typing_code2:STRING,'
	'hit:STRING,'
	'telephone_destination:STRING,'
	'telephone_costs:STRING,'
	'telephone_number:STRING,'
	'who_hangs_up:STRING,'
	'customer_identification:STRING,'
	'month:STRING,'
	'screen_recording:STRING,'
	'operation:STRING,'
	'ring:STRING,'
	'abandon:STRING,'
	'id_cliente:STRING,'
	'cartera:STRING'
)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'id_call': arrayCSV[0],
				'type_call': arrayCSV[1],
				'talk_time': arrayCSV[2],
				'id_agent': arrayCSV[3],
				'agent_name': arrayCSV[4],
				'agent_identification': arrayCSV[5],
				'skill': arrayCSV[6],
				'date': arrayCSV[7],
				'hour': arrayCSV[8],
				'day_of_week': arrayCSV[9],
				'typing_code': arrayCSV[10],
				'descri_typing_code': arrayCSV[11],
				'typing_code2': arrayCSV[12],
				'descri_typing_code2': arrayCSV[13],
				'hit': arrayCSV[14],
				'telephone_destination': arrayCSV[15],
				'telephone_costs': arrayCSV[16],
				'telephone_number': arrayCSV[17],
				'who_hangs_up': arrayCSV[18],
				'customer_identification': arrayCSV[19],
				'month': arrayCSV[20],
				'screen_recording': arrayCSV[21],
				'operation': arrayCSV[22],
				'ring': arrayCSV[23],
				'abandon': arrayCSV[24],
				'id_cliente': arrayCSV[25],
				'cartera': arrayCSV[26]
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