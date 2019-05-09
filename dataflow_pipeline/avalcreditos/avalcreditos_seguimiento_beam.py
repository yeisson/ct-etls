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
	'empresa_gestiona:STRING, '
	'empresa_reporta:STRING, '
	'nit:STRING, '
	'nombre_cliente:STRING, '
	'razon_social:STRING, '
	'gestor:STRING, '
	'nro_credito:STRING, '
	'telefono:STRING, '
	'resultado:STRING, '
	'resena:STRING, '
	'hora_inicial:STRING, '
	'hora_final:STRING, '
	'duracion:STRING, '
	'grabacion:STRING, '
	'edad:STRING, '
	'valor_cobrado:STRING, '
	'tipificacion:STRING, '
	'ficticia:STRING, '
	'ano_venci_cliente:STRING, '
	'causal:STRING, '
	'hora:STRING '
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
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'empresa_gestiona' : arrayCSV[0].replace('"',''),
				'empresa_reporta' : arrayCSV[1].replace('"',''),
				'nit' : arrayCSV[2].replace('"',''),
				'nombre_cliente' : arrayCSV[3].replace('"',''),
				'razon_social' : arrayCSV[4].replace('"',''),
				'gestor' : arrayCSV[5].replace('"',''),
				'nro_credito' : arrayCSV[6].replace('"',''),
				'telefono' : arrayCSV[7].replace('"',''),
				'resultado' : arrayCSV[8].replace('"',''),
				'resena' : arrayCSV[9].replace('"',''),
				'hora_inicial' : arrayCSV[10].replace('"',''),
				'hora_final' : arrayCSV[11].replace('"',''),
				'duracion' : arrayCSV[12].replace('"',''),
				'grabacion' : arrayCSV[13].replace('"',''),
				'edad' : arrayCSV[14].replace('"',''),
				'valor_cobrado' : arrayCSV[15].replace('"',''),
				'tipificacion' : arrayCSV[16].replace('"',''),
				'ficticia' : arrayCSV[17].replace('"',''),
				'ano_venci_cliente' : arrayCSV[18].replace('"',''),
				'causal' : arrayCSV[19].replace('"',''),
				'hora' : arrayCSV[20].replace('"','')
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-avalcreditos" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery avalcreditos' >> beam.io.WriteToBigQuery(
		gcs_project + ":avalcreditos.seguimiento", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



