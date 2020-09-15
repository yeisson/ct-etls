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
	'id_agent_ipdial:STRING,'
	'skill:STRING,'
	'date:DATETIME,'
	'id_call:STRING,'
	'ANI:STRING,'
	'id_customer:STRING,'
	'q01:STRING,'
	'q02:STRING,'
	'q03:STRING,'
	'q04:STRING,'
	'q05:STRING,'
	'q06:STRING,'
	'q07:STRING,'
	'q08:STRING,'
	'q09:STRING,'
	'q10:STRING,'
	'duration:STRING,'
	'type_call:STRING,'
	'result:STRING,'
	'id_cliente:STRING,'
	'cartera:STRING'
)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'operation': arrayCSV[0],
				'id_agent_ipdial': arrayCSV[1],
				'skill': arrayCSV[2],
				'date': arrayCSV[3],
				'id_call': arrayCSV[4],
				'ANI': arrayCSV[5],
				'id_customer': arrayCSV[6],
				'q01': arrayCSV[7],
				'q02': arrayCSV[8],
				'q03': arrayCSV[9],
				'q04': arrayCSV[10],
				'q05': arrayCSV[11],
				'q06': arrayCSV[12],
				'q07': arrayCSV[13],
				'q08': arrayCSV[14],
				'q09': arrayCSV[15],
				'q10': arrayCSV[16],
				'duration': arrayCSV[17],
				'type_call': arrayCSV[18],
				'result': arrayCSV[19],
				'id_cliente': arrayCSV[20],
				'cartera': arrayCSV[21]
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