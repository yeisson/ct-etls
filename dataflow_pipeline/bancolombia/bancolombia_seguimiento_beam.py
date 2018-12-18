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
	'consecutivo_obligacion:STRING, '
	'nit:STRING, '
	'fecha_gestion:STRING, '
	'grabador:STRING, '
	'desc_ultimo_codigo_de_gestion_prejuridico:STRING, '
	'hora_grabacion:TIME, '
	'codigo_de_gestion:STRING, '
	't_igraba:TIME, '
	'dias_mora:STRING, '
	'duracion:TIME, '
	'regional:STRING, '
	'consecutivo_gestion:STRING, '
	'nro_documento:STRING, '
	'fecha_promesa:STRING, '
	'codigo_abogado:STRING, '
	'nombre_abogado:STRING, '
	't_entrada:STRING, '
	'codigo_de_cobro_anterior:STRING, '
	'fecha_corte:STRING, '
	'nota:STRING '

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
				'consecutivo_obligacion': arrayCSV[0],
				'nit': arrayCSV[1],
				'fecha_gestion': arrayCSV[2],
				'grabador': arrayCSV[3],
				'desc_ultimo_codigo_de_gestion_prejuridico': arrayCSV[4],
				'hora_grabacion': arrayCSV[5],
				'codigo_de_gestion': arrayCSV[6],
				't_igraba': arrayCSV[7],
				'dias_mora': arrayCSV[8],
				'duracion': arrayCSV[9],
				'regional': arrayCSV[10],
				'consecutivo_gestion': arrayCSV[11],
				'nro_documento': arrayCSV[12],
				'fecha_promesa': arrayCSV[13],
				'codigo_abogado': arrayCSV[14],
				'nombre_abogado': arrayCSV[15],
				't_entrada': arrayCSV[16],
				'codigo_de_cobro_anterior': arrayCSV[17],
				'fecha_corte': arrayCSV[18],
				'nota': arrayCSV[19]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	pipeline =  beam.Pipeline(runner="DirectRunner")
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181206 1100.csv", skip_header_lines=1)
	#lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181129 0800.csv", skip_header_lines=1)
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText("//192.168.20.87/aries/Inteligencia_Negocios/EQUIPO BI/nflorez/fuentes_seg/" + archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	#transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_seg", file_name_suffix='.csv',shard_name_template='')
	#transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/info-segumiento/info_carga_banco_seg",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_admin.seguimiento", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")



