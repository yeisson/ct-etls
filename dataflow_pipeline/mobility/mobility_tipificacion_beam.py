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

TABLE_SCHEMA = (
		'idkey:STRING, '
		'fecha:STRING, '
		'ID_CALL:STRING, '
		'ID_AGENT:STRING, '
		'AGENT_NAME:STRING, '
		'SKILL:STRING, '
		'DATE:STRING, '
		'COD1:STRING, '
		'COD2:STRING, '
		'CALL_COMMENTS:STRING, '
		'TYPE_OF_CALL:STRING, '
		'TELEPHONE:STRING, '
		'DESTINATION:STRING, '
		'DURATION_SEG_:STRING, '
		'WHO_HUNG_UP:STRING, '
		'ID_CUSTOMER:STRING, '
		'ID_CAMPAING:STRING '









)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha': self.mifecha,
				'ID_CALL' : arrayCSV[0],
				'ID_AGENT' : arrayCSV[1],
				'AGENT_NAME' : arrayCSV[2],
				'SKILL' : arrayCSV[3],
				'DATE' : arrayCSV[4],
				'COD1' : arrayCSV[5],
				'COD2' : arrayCSV[6],
				'CALL_COMMENTS' : arrayCSV[7],
				'TYPE_OF_CALL' : arrayCSV[8],
				'TELEPHONE' : arrayCSV[9],
				'DESTINATION' : arrayCSV[10],
				'DURATION_SEG_' : arrayCSV[11],
				'WHO_HUNG_UP' : arrayCSV[12],
				'ID_CUSTOMER' : arrayCSV[13],
				'ID_CAMPAING' : arrayCSV[14]








				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-pto" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runer = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runer, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "10",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
        # "--num_workers", "30",
        # "--autoscaling_algorithm", "NONE"		
	])
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181206 1100.csv", skip_header_lines=1)
	#lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181129 0800.csv", skip_header_lines=1)
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_seg", file_name_suffix='.csv',shard_name_template='')
	#transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/info-segumiento/info_carga_banco_seg",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Mobility' >> beam.io.WriteToBigQuery(
		gcs_project + ":Auteco_Mobility.tipificacion", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



