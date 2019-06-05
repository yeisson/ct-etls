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
	'CONSDOCDEU:STRING, '
	'NIT:STRING, '
	'NOMBRE:STRING, '
	'PAGARE:STRING, '
	'VALOR_RECAUDO:STRING, '
	'FECHA_RECAUDO:STRING, '
	'LINEA_NEGOCIO:STRING, '
	'COD_ABOG_PPAL:STRING, '
	'COD_ABOG_PARA:STRING, '
	'REGION_GESTOR:STRING, '
	'PERFIL:STRING, '
	'NOMBRE_ABOGADO:STRING, '
	'DESC_PROD:STRING, '
	'APLICATIVO:STRING, '
	'OFIC_PREST:STRING, '
	'REGION_OFI_PRESTAMO:STRING, '
	'SEGMENTO:STRING, '
	'GRUPO_FINAL:STRING, '
	'USUARIO_TAREA:STRING, '
	'USUARIO_TAREA_PARA:STRING, '
	'DIAS_MORA:STRING, '
	'FECHA_CASTIGO:STRING '
)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'IDKEY' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'FECHA': self.mifecha,
				'CONSDOCDEU' : arrayCSV[0],
				'NIT' : arrayCSV[1],
				'NOMBRE' : arrayCSV[2],
				'PAGARE' : arrayCSV[3],
				'VALOR_RECAUDO' : arrayCSV[4],
				'FECHA_RECAUDO' : arrayCSV[5],
				'LINEA_NEGOCIO' : arrayCSV[6],
				'COD_ABOG_PPAL' : arrayCSV[7],
				'COD_ABOG_PARA' : arrayCSV[8],
				'REGION_GESTOR' : arrayCSV[9],
				'PERFIL' : arrayCSV[10],
				'NOMBRE_ABOGADO' : arrayCSV[11],
				'DESC_PROD' : arrayCSV[12],
				'APLICATIVO' : arrayCSV[13],
				'OFIC_PREST' : arrayCSV[14],
				'REGION_OFI_PRESTAMO' : arrayCSV[15],
				'SEGMENTO' : arrayCSV[16],
				'GRUPO_FINAL' : arrayCSV[17],
				'USUARIO_TAREA' : arrayCSV[18],
				'USUARIO_TAREA_PARA' : arrayCSV[19],
				'DIAS_MORA' : arrayCSV[20],
				'FECHA_CASTIGO' : arrayCSV[21]
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_castigada.pagos", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



