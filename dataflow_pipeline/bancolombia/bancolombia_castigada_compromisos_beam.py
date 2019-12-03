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
	'nombre_archivo:STRING, '
	'NO_DE_GESTION:STRING, '
	'CEDULA:STRING, '
	'FECHA_GENERACION:STRING, '
	'CODIGO_DE_ABOGADO:STRING, '
	'FECHA_COMPROMISO:STRING, '
	'NO_DE_OBLIGACION:STRING, '
	'ESTADO:STRING, '
	'ID_GRABADOR:STRING, '
	'CDIGO_DE_GESTIN:STRING, '
	'CODIGO_CIERRE_COMPROMISO:STRING, '
	'DESCRIPCIN_CDIGO_DE_GESTIN:STRING, '
	'VALOR_PACTADO:NUMERIC, '
	'VALOR_PAGADO:STRING '
)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha, tipo):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
		self.tipo = tipo

	
	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha': self.mifecha,
				'nombre_archivo': self.tipo,
				'NO_DE_GESTION' : arrayCSV[0],
				'CEDULA' : arrayCSV[1],
				'FECHA_GENERACION' : arrayCSV[2],
				'CODIGO_DE_ABOGADO' : arrayCSV[3],
				'FECHA_COMPROMISO' : arrayCSV[4],
				'NO_DE_OBLIGACION' : arrayCSV[5],
				'ESTADO' : arrayCSV[6],
				'ID_GRABADOR' : arrayCSV[7],
				'CDIGO_DE_GESTIN' : arrayCSV[8],
				'CODIGO_CIERRE_COMPROMISO' : arrayCSV[9],
				'DESCRIPCIN_CDIGO_DE_GESTIN' : arrayCSV[10],
				'VALOR_PACTADO' : arrayCSV[11],
				'VALOR_PAGADO' : arrayCSV[12]

				}
		
		return [tupla]



def run(archivo, mifecha, tipo):

	gcs_path = "gs://ct-bancolombia_castigada" #Definicion de la raiz del bucket
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

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha, tipo)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_seg", file_name_suffix='.csv',shard_name_template='')
	#transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/info-segumiento/info_carga_banco_seg",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_castigada.compromisos", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



