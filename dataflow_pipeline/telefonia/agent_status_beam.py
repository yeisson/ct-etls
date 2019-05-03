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
	'date:STRING,'
	'hour:STRING,'
	'id_agent:STRING,'
	'agent_identification:STRING,'
	'agent_name:STRING,'
	'CALLS:STRING,'
	'CALLS_INBOUND:STRING,'
	'CALLS_OUTBOUND:STRING,'
	'CALLS_INTERNAL:STRING,'
	'READY_TIME:STRING,'
	'INBOUND_TIME:STRING,'
	'OUTBOUND_TIME:STRING,'
	'NOT_READY_TIME:STRING,'
	'RING_TIME:STRING,'
	'LOGIN_TIME:STRING,'
	'AHT:STRING,'
	'OCUPANCY:STRING,'
	'AUX_TIME:STRING,'
	'id_cliente:STRING,'
	'cartera:STRING'
)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('@')
		tupla= {
				'operation': arrayCSV[0],
				'date': arrayCSV[1],
				'hour': arrayCSV[2],
				'id_agent': arrayCSV[3],
				'agent_identification': arrayCSV[4],
				'agent_name': arrayCSV[5],
				'CALLS': arrayCSV[6],
				'CALLS_INBOUND': arrayCSV[7],
				'CALLS_OUTBOUND': arrayCSV[8],
				'CALLS_INTERNAL': arrayCSV[9],
				'READY_TIME': arrayCSV[10],
				'INBOUND_TIME': arrayCSV[11],
				'OUTBOUND_TIME': arrayCSV[12],
				'NOT_READY_TIME': arrayCSV[13],
				'RING_TIME': arrayCSV[14],
				'LOGIN_TIME': arrayCSV[15],
				'AHT': arrayCSV[16],
				'OCUPANCY': arrayCSV[17],
				'AUX_TIME': arrayCSV[18],
				'id_cliente': arrayCSV[19],
				'cartera': arrayCSV[20]
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