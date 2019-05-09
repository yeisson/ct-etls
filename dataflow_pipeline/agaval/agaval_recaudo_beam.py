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
from apache_beam.io import WriteToText, textio
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
	'idkey:STRING, '
	'fecha:STRING, '
	'ANO:STRING, '
	'DIA:STRING, '
	'MES:STRING, '
	'FECHA_DE_GRABACION:STRING, '
	'TIPO_CLIENTE:STRING, '
	'NEGOCIADOR:STRING, '
	'CENTRO_DE_COSTOS:STRING, '
	'ESTADO_CARTERA:STRING, '
	'PROXIMO_A_REPORTE:STRING, '
	'VALOR_PAGADO:STRING, '
	'OBLIGACION:STRING, '
	'INICIO_DEL_CREDITO:STRING, '
	'TIPO_ACTIVIDAD:STRING, '
	'DIAS_MORA:STRING, '
	'FECHA_DE_PAGO:STRING, '
	'ESTADO_DE_COBRO:STRING, '
	'CAMPANA:STRING, '
	'SIGNO:STRING, '
	'NIT:STRING, '
	'ASESOR:STRING, '
	'VALOR_ABONO_INTERESES:STRING, '
	'VALOR_ABONO_CAPITAL:STRING, '
	'NOMBRES:STRING, '
	'FECHA_DE_GRABACION2:STRING, '
	'FECHA_DE_PAGO2:STRING, '
	'FECHA_TRASLADO:STRING, '
	'GASTOS_DE_COBRANZA:STRING, '
	'NOMBRE_CARTERA:STRING, '
	'PAGO_A_GRABADOR:STRING, '
	'SEMANA:STRING, '
	'HONORARIO:STRING, '
	'NR:STRING '

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
				# 'fecha' : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),	#datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'ANO' : arrayCSV[0],
				'DIA' : arrayCSV[1],
				'MES' : arrayCSV[2],
				'FECHA_DE_GRABACION' : arrayCSV[3],
				'TIPO_CLIENTE' : arrayCSV[4],
				'NEGOCIADOR' : arrayCSV[5],
				'CENTRO_DE_COSTOS' : arrayCSV[6],
				'ESTADO_CARTERA' : arrayCSV[7],
				'PROXIMO_A_REPORTE' : arrayCSV[8],
				'VALOR_PAGADO' : arrayCSV[9],
				'OBLIGACION' : arrayCSV[10],
				'INICIO_DEL_CREDITO' : arrayCSV[11],
				'TIPO_ACTIVIDAD' : arrayCSV[12],
				'DIAS_MORA' : arrayCSV[13],
				'FECHA_DE_PAGO' : arrayCSV[14],
				'ESTADO_DE_COBRO' : arrayCSV[15],
				'CAMPANA' : arrayCSV[16],
				'SIGNO' : arrayCSV[17],
				'NIT' : arrayCSV[18],
				'ASESOR' : arrayCSV[19],
				'VALOR_ABONO_INTERESES' : arrayCSV[20],
				'VALOR_ABONO_CAPITAL' : arrayCSV[21],
				'NOMBRES' : arrayCSV[22],
				'FECHA_DE_GRABACION2' : arrayCSV[23],
				'FECHA_DE_PAGO2' : arrayCSV[24],
				'FECHA_TRASLADO' : arrayCSV[25],
				'GASTOS_DE_COBRANZA' : arrayCSV[26],
				'NOMBRE_CARTERA' : arrayCSV[27],
				'PAGO_A_GRABADOR' : arrayCSV[28],
				'SEMANA' : arrayCSV[29],
				'HONORARIO' : arrayCSV[30],
				'NR' : arrayCSV[31]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-agaval" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"
	# fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	# pipeline =  beam.Pipeline(runner="DirectRunner")
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
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
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT")
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("archivos/BANCOLOMBIA_BM_20181203.csv", skip_header_lines=1)
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Base_Marcada_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/resultado_archivos_bm/Resultado_Base_Marcada", file_name_suffix='.csv',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/bm/Base_Marcada",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Agaval' >> beam.io.WriteToBigQuery(
		gcs_project + ":agaval.recaudo", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()
	
	# jobObject.wait_until_finish()
	return ("Corrio Full HD")



