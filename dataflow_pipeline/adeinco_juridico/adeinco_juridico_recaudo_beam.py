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
from apache_beam.io import WriteToText, textio
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
	'idkey:STRING, '
	'fecha:STRING, '
	'CONCESIONARIO:STRING, '
	'CLIENTE:STRING, '
	'CEDULA_CLTE:STRING, '
	'CLASIFICACION_CARTERA:STRING, '
	'RECIBO:STRING, '
	'CREDITO:STRING, '
	'FECHA_SLF:STRING, '
	'PAGO_GENERADO:STRING, '
	'HONORARIOS:STRING, '
	'NEGOCIADOR:STRING, '
	'VALOR_RECIBO:STRING, '
	'NOMBRE_CARTERA:STRING, '
	'PAGO_A_GRABADOR:STRING, '
	'SEMANA:STRING, '
	'TIPO_DE_CARTERA:STRING, '
	'NR:STRING, '
	'FRANJA:STRING '


)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),	#datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'CONCESIONARIO' : arrayCSV[0],
				'CLIENTE' : arrayCSV[1],
				'CEDULA_CLTE' : arrayCSV[2],
				'CLASIFICACION_CARTERA' : arrayCSV[3],
				'RECIBO' : arrayCSV[4],
				'CREDITO' : arrayCSV[5],
				'FECHA_SLF' : arrayCSV[6],
				'PAGO_GENERADO' : arrayCSV[7],
				'HONORARIOS' : arrayCSV[8],
				'NEGOCIADOR' : arrayCSV[9],
				'VALOR_RECIBO' : arrayCSV[10],
				'NOMBRE_CARTERA' : arrayCSV[11],
				'PAGO_A_GRABADOR' : arrayCSV[12],
				'SEMANA' : arrayCSV[13],
				'TIPO_DE_CARTERA' : arrayCSV[14],
				'NR' : arrayCSV[15],
				'FRANJA' : arrayCSV[16]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-adeinco_juridico" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"
	# fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	# pipeline =  beam.Pipeline(runner="DirectRunner")
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
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
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT")
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("archivos/BANCOLOMBIA_BM_20181203.csv", skip_header_lines=1)
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Base_Marcada_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/resultado_archivos_bm/Resultado_Base_Marcada", file_name_suffix='.csv',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/bm/Base_Marcada",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Adeinco' >> beam.io.WriteToBigQuery(
		gcs_project + ":adeinco_juridico.recaudo", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()
	
	# jobObject.wait_until_finish()
	return ("Corrio Full HD")



