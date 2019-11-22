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
	'IDKEY:STRING, ' 
	'FECHA:STRING, ' 
	'MES:STRING, '
	'MONITOREOS:STRING, '
	'ID_CC:STRING, '
	'NEGOCIADOR:STRING, '
	'TEAM_LEADER:STRING, '
	'EJECUTIVO_DE_CUENTA:STRING, '
	'GERENTE:STRING, '
	'OPERACION:STRING, '
	'SEDE:STRING, '
	'CATEGORIA:STRING, '
	'UEN:STRING, '
	'PRODUCTO:STRING, '
	'EVALUADOR:STRING, '
	'PEC:STRING, '
	'PENC:STRING, '
	'GOS:STRING, '
	'FECHA_REGISTRO:STRING, '
	'HORA_REGISTRO:STRING, '
	'SEMANA:STRING, '
	'CONSECUTIVO:STRING, '
	'HORA:STRING, '
	'MINUTOS:STRING, '
	'DIASH:STRING, '
	'CDIAS:STRING, '
	'ESTADO:STRING '









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
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha': self.mifecha,
				'MES' : arrayCSV[0],
				'MONITOREOS' : arrayCSV[1],
				'ID_CC' : arrayCSV[2],
				'NEGOCIADOR' : arrayCSV[3],
				'TEAM_LEADER' : arrayCSV[4],
				'EJECUTIVO_DE_CUENTA' : arrayCSV[5],
				'GERENTE' : arrayCSV[6],
				'OPERACION' : arrayCSV[7],
				'SEDE' : arrayCSV[8],
				'CATEGORIA' : arrayCSV[9],
				'UEN' : arrayCSV[10],
				'PRODUCTO' : arrayCSV[11],
				'EVALUADOR' : arrayCSV[12],
				'PEC' : arrayCSV[13],
				'PENC' : arrayCSV[14],
				'GOS' : arrayCSV[15],
				'FECHA_REGISTRO' : arrayCSV[16],
				'HORA_REGISTRO' : arrayCSV[17],
				'SEMANA' : arrayCSV[18],
				'CONSECUTIVO' : arrayCSV[19],
				'HORA' : arrayCSV[20],
				'MINUTOS' : arrayCSV[21],
				'DIASH' : arrayCSV[22],
				'CDIAS' : arrayCSV[23],
				'ESTADO' : arrayCSV[24]







				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-sensus" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runer = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runer, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5",
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

	transformed | 'Escritura a BigQuery seguimiento' >> beam.io.WriteToBigQuery(
		gcs_project + ":sensus.seguimiento", 
		schema=TABLE_SCHEMA, 	
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")