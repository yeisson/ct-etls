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
	'NIT:STRING, '
	'CREDITO_NRO:STRING, '
	'NOMBRES:STRING, '
	'GRABADOR:STRING, '
	'ESTADO_DE_COBRO:STRING, '
	'FECHA_ULTIMA_GESTION_PREJURIDICA:STRING, '
	'FECHA_DE_COMPROMISO:STRING, '
	'FECHA_ULTIMO_ABONO:STRING, '
	'TIPO_DE_CLIENTE:STRING, '
	'CALIFICACION_ANTERIOR:STRING, '
	'TOTAL_A_PAGAR:STRING, '
	'VALOR_DEL_CAPITAL:STRING, '
	'NUMELULAR:STRING, '
	'TELEFONO_RESIDENCIA:STRING, '
	'DIAS_MORA:STRING, '
	'ESTADO_CARTERA:STRING, '
	'FECHA_DE_TRASLADO:STRING, '
	'INICIO_CREDITO:STRING '


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
				'NIT' : arrayCSV[0],
				'CREDITO_NRO' : arrayCSV[1],
				'NOMBRES' : arrayCSV[2],
				'GRABADOR' : arrayCSV[3],
				'ESTADO_DE_COBRO' : arrayCSV[4],
				'FECHA_ULTIMA_GESTION_PREJURIDICA' : arrayCSV[5],
				'FECHA_DE_COMPROMISO' : arrayCSV[6],
				'FECHA_ULTIMO_ABONO' : arrayCSV[7],
				'TIPO_DE_CLIENTE' : arrayCSV[8],
				'CALIFICACION_ANTERIOR' : arrayCSV[9],
				'TOTAL_A_PAGAR' : arrayCSV[10],
				'VALOR_DEL_CAPITAL' : arrayCSV[11],
				'NUMELULAR' : arrayCSV[12],
				'TELEFONO_RESIDENCIA' : arrayCSV[13],
				'DIAS_MORA' : arrayCSV[14],
				'ESTADO_CARTERA' : arrayCSV[15],
				'FECHA_DE_TRASLADO' : arrayCSV[16],
				'INICIO_CREDITO' : arrayCSV[17]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-agaval" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Agaval' >> beam.io.WriteToBigQuery(
		gcs_project + ":agaval.asignacion", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



