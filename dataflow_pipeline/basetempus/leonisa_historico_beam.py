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
		'DOCUMENTO:STRING, '
		'NOMBRE_CLIENTE:STRING, '
		'ZONA:STRING, '
		'NOMBRE_CAMPANA:STRING, '
		'NUM_OBLIGACION:STRING, '
		'ID_COD_GESTION:STRING, '
		'NOMBRE_CODIGO:STRING, '
		'ID_COD_CAUSAL:STRING, '
		'NOMBRE_CAUSAL:STRING, '
		'OBSERVACION:STRING, '
		'VLR_PROMESA:STRING, '
		'FECHA_PROMESA:STRING, '
		'NUM_CUOTAS:STRING, '
		'FECHA_GESTION:STRING, '
		'CUADRANTE:STRING, '
		'MODALIDAD_PAGO:STRING, '
		'NOMBRE_USUARIO:STRING, '
		'ABOGADO:STRING, '
		'VLR_SALDO_CARTERA:STRING '




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
				'DOCUMENTO' : arrayCSV[0],
				'NOMBRE_CLIENTE' : arrayCSV[1],
				'ZONA' : arrayCSV[2],
				'NOMBRE_CAMPANA' : arrayCSV[3],
				'NUM_OBLIGACION' : arrayCSV[4],
				'ID_COD_GESTION' : arrayCSV[5],
				'NOMBRE_CODIGO' : arrayCSV[6],
				'ID_COD_CAUSAL' : arrayCSV[7],
				'NOMBRE_CAUSAL' : arrayCSV[8],
				'OBSERVACION' : arrayCSV[9],
				'VLR_PROMESA' : arrayCSV[10],
				'FECHA_PROMESA' : arrayCSV[11],
				'NUM_CUOTAS' : arrayCSV[12],
				'FECHA_GESTION' : arrayCSV[13],
				'CUADRANTE' : arrayCSV[14],
				'MODALIDAD_PAGO' : arrayCSV[15],
				'NOMBRE_USUARIO' : arrayCSV[16],
				'ABOGADO' : arrayCSV[17],
				'VLR_SALDO_CARTERA' : arrayCSV[18]


				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-unificadas" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Leonisa Estrategia' >> beam.io.WriteToBigQuery(
		gcs_project + ":unificadas.historico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



