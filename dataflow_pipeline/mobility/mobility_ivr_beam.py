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
		'ID_AGENT:STRING, '
		'QUEUE:STRING, '
		'DATE:STRING, '
		'ID_CALL:STRING, '
		'ANI:STRING, '
		'ID_CUSTOMER:STRING, '
		'Q01:STRING, '
		'Q02:STRING, '
		'Q03:STRING, '
		'Q04:STRING, '
		'Q05:STRING, '
		'Q06:STRING, '
		'Q07:STRING, '
		'Q08:STRING, '
		'Q09:STRING, '
		'Q10:STRING, '
		'VOICE_MESSAGE_DURATION_SEG_:STRING, '
		'TYPE_CALL:STRING, '
		'RESULT:STRING, '
		'TOTAL_DURATION:STRING '










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
				'ID_AGENT' : arrayCSV[0],
				'QUEUE' : arrayCSV[1],
				'DATE' : arrayCSV[2],
				'ID_CALL' : arrayCSV[3],
				'ANI' : arrayCSV[4],
				'ID_CUSTOMER' : arrayCSV[5],
				'Q01' : arrayCSV[6],
				'Q02' : arrayCSV[7],
				'Q03' : arrayCSV[8],
				'Q04' : arrayCSV[9],
				'Q05' : arrayCSV[10],
				'Q06' : arrayCSV[11],
				'Q07' : arrayCSV[12],
				'Q08' : arrayCSV[13],
				'Q09' : arrayCSV[14],
				'Q10' : arrayCSV[15],
				'VOICE_MESSAGE_DURATION_SEG_' : arrayCSV[16],
				'TYPE_CALL' : arrayCSV[17],
				'RESULT' : arrayCSV[18],
				'TOTAL_DURATION' : arrayCSV[19]









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
		gcs_project + ":Auteco_Mobility.ivr", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



