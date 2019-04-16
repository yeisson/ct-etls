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
	'operation:STRING,'
	'id_call:STRING,'
	'type_call:STRING,'
	'date:STRING,'
	'id_agent:STRING,'
	'name_agent:STRING,'
	'skill:STRING,'
	'wait_time:STRING,'
	'calls_ans_10:STRING,'
	'calls_ans_20:STRING,'
	'calls_ans_30:STRING,'
	'calls_ans_40:STRING,'
	'calls_ans_50:STRING,'
	'skill_result:STRING,'
	'ani:STRING,'
	'dnis:STRING,'
	'id_cliente:STRING,'
	'cartera:STRING'
)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'operation': arrayCSV[0],
				'id_call': arrayCSV[1],
				'type_call': arrayCSV[2],
				'date': arrayCSV[3],
				'id_agent': arrayCSV[4],
				'name_agent': arrayCSV[5],
				'skill': arrayCSV[6],
				'wait_time': arrayCSV[7],
				'calls_ans_10': arrayCSV[8],
				'calls_ans_20': arrayCSV[9],
				'calls_ans_30': arrayCSV[10],
				'calls_ans_40': arrayCSV[11],
				'calls_ans_50': arrayCSV[12],
				'skill_result': arrayCSV[13],
				'ani': arrayCSV[14],
				'dnis': arrayCSV[15],
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
        "--max_num_workers", "10",
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