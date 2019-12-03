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
'NOMBRE_CAMPANANA:STRING, '
'FECHA_GESTION:STRING, '
'NOMBRES:STRING, '
'APELLIDOS:STRING, '
'CODIGO:STRING, '
'ESTADO:STRING, '
'MOTIVO:STRING, '
'OTRA_UNIVERSIDAD:STRING, '
'JORNADA:STRING, '
'CONDICION_DE_INGRESO:STRING, '
'PROGRAMA_DE_INTERES:STRING, '
'PAGO:STRING, '
'CEDULA_CLIENTE:STRING, '
'EMAIL:STRING, '
'CELULAR:STRING, '
'DATE_1:STRING, '
'DATE_2:STRING, '
'DATE_3:STRING, '
'DATE_4:STRING, '
'DATE_5:STRING, '
'DATE_6:STRING, '
'DATE_7:STRING, '
'DATE_8:STRING, '
'DATE_9:STRING, '
'DATE_10:STRING, '
'DATE_11:STRING, '
'DATE_12:STRING, '
'DATE_13:STRING, '
'DATE_14:STRING, '
'DATE_15:STRING '
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
				'NOMBRE_CAMPANANA' : arrayCSV[0],
				'FECHA_GESTION' : arrayCSV[1],
				'NOMBRES' : arrayCSV[2],
				'APELLIDOS' : arrayCSV[3],
				'CODIGO' : arrayCSV[4],
				'ESTADO' : arrayCSV[5],
				'MOTIVO' : arrayCSV[6],
				'OTRA_UNIVERSIDAD' : arrayCSV[7],
				'JORNADA' : arrayCSV[8],
				'CONDICION_DE_INGRESO' : arrayCSV[9],
				'PROGRAMA_DE_INTERES' : arrayCSV[10],
				'PAGO' : arrayCSV[11],
				'CEDULA_CLIENTE' : arrayCSV[12],
				'EMAIL' : arrayCSV[13],
				'CELULAR' : arrayCSV[14],
				'DATE_1' : arrayCSV[15],
				'DATE_2' : arrayCSV[16],
				'DATE_3' : arrayCSV[17],
				'DATE_4' : arrayCSV[18],
				'DATE_5' : arrayCSV[19],
				'DATE_6' : arrayCSV[20],
				'DATE_7' : arrayCSV[21],
				'DATE_8' : arrayCSV[22],
				'DATE_9' : arrayCSV[23],
				'DATE_10' : arrayCSV[24],
				'DATE_11' : arrayCSV[25],
				'DATE_12' : arrayCSV[26],
				'DATE_13' : arrayCSV[27],
				'DATE_14' : arrayCSV[28],
				'DATE_15' : arrayCSV[29]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-ucc" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery ucc' >> beam.io.WriteToBigQuery(
		gcs_project + ":ucc.base_campanas", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_ucc.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")