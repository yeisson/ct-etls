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
	'CENTRO_DE_COSTOS:STRING, '
	'ANO:STRING, '
	'MES:STRING, '
	'UEN:STRING, '
	'RESPONSABLE_HISTORICO:STRING, '
	'RESPONSABLE_ACTUAL:STRING, '
	'PROCESO:STRING, '
	'NOMBRE_CENTRO_DE_COSTOS:STRING, '
	'COD_CUENTA:STRING, '
	'CASO_1:STRING, '
	'CASO_2:STRING, '
	'CASO_4:STRING, '
	'CASO_6:STRING, '
	'CASO_8:STRING, '
	'SALDO_FINAL:STRING, '
	'CONCEPTO:STRING '


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
				'CENTRO_DE_COSTOS' : arrayCSV[0],
				'ANO' : arrayCSV[1],
				'MES' : arrayCSV[2],
				'UEN' : arrayCSV[3],
				'RESPONSABLE_HISTORICO' : arrayCSV[4],
				'RESPONSABLE_ACTUAL' : arrayCSV[5],
				'PROCESO' : arrayCSV[6],
				'NOMBRE_CENTRO_DE_COSTOS' : arrayCSV[7],
				'COD_CUENTA' : arrayCSV[8],
				'CASO_1' : arrayCSV[9],
				'CASO_2' : arrayCSV[10],
				'CASO_4' : arrayCSV[11],
				'CASO_6' : arrayCSV[12],
				'CASO_8' : arrayCSV[13],
				'SALDO_FINAL' : arrayCSV[14],
				'CONCEPTO' : arrayCSV[15]

				
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-presupuesto" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery presupuesto' >> beam.io.WriteToBigQuery(
		gcs_project + ":presupuesto.pyg", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



