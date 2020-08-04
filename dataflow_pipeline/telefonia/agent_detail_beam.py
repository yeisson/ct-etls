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
	'OPERATION:STRING, '
	'ID_CALL:STRING, '
	'TYPE_CALL:STRING, '
	'DATE:STRING, '
	'ID_AGENT:STRING, '
	'NAME_AGENT:STRING, '
	'SKILL:STRING, '
	'DURATION:STRING, '
	'COD_ACT:STRING, '
	'COD_ACT2:STRING, '
	'TELEPHONE:STRING, '
	'HUNG_UP:STRING, '
	'COMMENT:STRING, '
	'SCREECBOX:STRING, '
	'AGENT_STATE:STRING, '
	'ID_CUSTOMER:STRING, '
	'DESTINY:STRING, '
	'TIME:STRING, '
	'ID_CLIENTE:STRING, '
	'CARTERA:STRING '


)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'OPERATION' : arrayCSV[0],
				'ID_CALL' : arrayCSV[1],
				'TYPE_CALL' : arrayCSV[2],
				'DATE' : arrayCSV[3],
				'ID_AGENT' : arrayCSV[4],
				'NAME_AGENT' : arrayCSV[5],
				'SKILL' : arrayCSV[6],
				'DURATION' : arrayCSV[7],
				'COD_ACT' : arrayCSV[8],
				'COD_ACT2' : arrayCSV[9],
				'TELEPHONE' : arrayCSV[10],
				'HUNG_UP' : arrayCSV[11],
				'COMMENT' : arrayCSV[12],
				'SCREECBOX' : arrayCSV[13],
				'AGENT_STATE' : arrayCSV[14],
				'ID_CUSTOMER' : arrayCSV[15],
				'DESTINY' : arrayCSV[16],
				'TIME' : arrayCSV[17],
				'ID_CLIENTE' : arrayCSV[18],
				'CARTERA' : arrayCSV[19]





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