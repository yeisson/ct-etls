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
	'Nit:STRING, '
	'Nro_Documento:STRING, '
	'Codigo_Abogado:STRING, '
	'Nombre_Abogado:STRING, '
	'Fecha_Gestion:STRING, '
	'Grabador:STRING, '
	'Ultimo_Codigo_De_Gestion_Prejuridico:STRING, '
	'Desc_Ultimo_Codigo_De_Gestion_Prejuridico:STRING, '
	'Codigo_Anterior_De_Gestion_Prejuridico:STRING, '
	'Dias_De_Mora:STRING, '
	'Consecutivo_Obligacion:STRING, '
	'Hora_Inicio_Gestion:STRING, '
	'Hora_Fin_de_Gestion:STRING, '
	'Duracion:STRING, '
	'Valor_Obligacion:STRING, '
	'Valor_Vencido:STRING, '
	'Saldo_Activo:STRING, '
	'Fecha_Vencimiento:STRING, '
	'Ciclo_De_Facturacion:STRING, '
	'Fecha_Corte:STRING, '
	'Nombre_de_Causal:STRING, '
	'Consecutivo_Gestion:STRING, '
	'Riesgo:STRING, '
	'Nota:STRING '

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
				'Nit': arrayCSV[0],
				'Nro_Documento': arrayCSV[1],
				'Codigo_Abogado': arrayCSV[2],
				'Nombre_Abogado': arrayCSV[3],
				'Fecha_Gestion': arrayCSV[4],
				'Grabador': arrayCSV[5],
				'Ultimo_Codigo_De_Gestion_Prejuridico': arrayCSV[6],
				'Desc_Ultimo_Codigo_De_Gestion_Prejuridico': arrayCSV[7],
				'Codigo_Anterior_De_Gestion_Prejuridico': arrayCSV[8],
				'Dias_De_Mora': arrayCSV[9],
				'Consecutivo_Obligacion': arrayCSV[10],
				'Hora_Inicio_Gestion': arrayCSV[11],
				'Hora_Fin_de_Gestion': arrayCSV[12],
				'Duracion': arrayCSV[13],
				'Valor_Obligacion': arrayCSV[14],
				'Valor_Vencido': arrayCSV[15],
				'Saldo_Activo': arrayCSV[16],
				'Fecha_Vencimiento': arrayCSV[17],
				'Ciclo_De_Facturacion': arrayCSV[18],
				'Fecha_Corte': arrayCSV[19],
				'Nombre_de_Causal': arrayCSV[20],
				'Consecutivo_Gestion': arrayCSV[21],
				'Riesgo': arrayCSV[22],
				'Nota': arrayCSV[23]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-tuya" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Tuya' >> beam.io.WriteToBigQuery(
		gcs_project + ":tuya.seguimiento", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



