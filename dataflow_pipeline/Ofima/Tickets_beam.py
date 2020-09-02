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
	'URL:STRING, '
	'ID:STRING, '
	'SUBJECT:STRING, '
	'REQUESTER_ID:STRING, '
	'SUBMITTER_ID:STRING, '
	'ORGANIZATION_ID:STRING, '
	'CREATED_AT:STRING, '
	'STATUS:STRING, '
	'CUSTOM_FIELDS:STRING, '
	'GROUP_ID:STRING '

	
)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'URL' : arrayCSV[0],
				'ID' : arrayCSV[1],
				'SUBJECT' : arrayCSV[2],
				'REQUESTER_ID' : arrayCSV[3],
				'SUBMITTER_ID' : arrayCSV[4],
				'ORGANIZATION_ID' : arrayCSV[5],
				'CREATED_AT' : arrayCSV[6],
				'STATUS' : arrayCSV[7],
				'CUSTOM_FIELDS' : arrayCSV[8],
				'GROUP_ID' : arrayCSV[9]

				}
		return [tupla]

############################ CODIGO DE EJECUCION ###################################

def run(output,KEY_REPORT):

	gcs_path = 'gs://ct-ofima_sac' #Definicion de la raiz del bucket
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
	
	transformed | 'Escritura a BigQuery Tickets' >> beam.io.WriteToBigQuery(
		gcs_project + ":Ofima_sac." + KEY_REPORT, 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")