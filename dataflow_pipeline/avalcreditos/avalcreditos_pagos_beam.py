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
	'cantidad_cuotas:STRING, '
	'cedula:STRING, '
	'credito:STRING, '
	'dias_mora:STRING, '
	'estado_cuota:STRING, '
	'ano_vencimiento:STRING, '
	'ano_colocacion:STRING, '
	'capital:STRING, '
	'aval:STRING, '
	'intereses:STRING, '
	'abonos:STRING, '
	'cuota:STRING, '
	'intereses_mora:STRING, '
	'valor_cuota:STRING, '
	'valor_cargar:STRING, '
	'saldo_pago:STRING, '
	'cancela_abona:STRING, '
	'honorarios_con_iva:STRING, '
	'honorarios_sin_iva:STRING, '
	'pago_bancos:STRING, '
	'valor_sac:STRING, '
	'entidad_cobro:STRING, '
	'casa_cobro:STRING, '
	'entidad_pc:STRING, '
	'fecha_ingreso_bancos:STRING, '
	'fecha_registro_sac:STRING, '
	'rango:STRING '
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
				'fecha' : self.mifecha,
				'cantidad_cuotas' : arrayCSV[0],
				'cedula' : arrayCSV[1],
				'credito' : arrayCSV[2],
				'dias_mora' : arrayCSV[3],
				'estado_cuota' : arrayCSV[4],
				'ano_vencimiento' : arrayCSV[5],
				'ano_colocacion' : arrayCSV[6],
				'capital' : arrayCSV[7].replace('$',''),
				'aval' : arrayCSV[8].replace('$',''),
				'intereses' : arrayCSV[9].replace('$',''),
				'abonos' : arrayCSV[10].replace('$',''),
				'cuota' : arrayCSV[11].replace('$',''),
				'intereses_mora' : arrayCSV[12].replace('$',''),
				'valor_cuota' : arrayCSV[13].replace('$',''),
				'valor_cargar' : arrayCSV[14],
				'saldo_pago' : arrayCSV[15].replace('$',''),
				'cancela_abona' : arrayCSV[16],
				'honorarios_con_iva' : arrayCSV[17].replace('$',''),
				'honorarios_sin_iva' : arrayCSV[18].replace('$',''),
				'pago_bancos' : arrayCSV[19].replace('$',''),
				'valor_sac' : arrayCSV[20].replace('.',''),
				'entidad_cobro' : arrayCSV[21],
				'casa_cobro' : arrayCSV[22],
				'entidad_pc' : arrayCSV[23],
				'fecha_ingreso_bancos' : arrayCSV[24],
				'fecha_registro_sac' : arrayCSV[25],
				'rango' : arrayCSV[26]
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
		gcs_project + ":avalcreditos.pagos", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")