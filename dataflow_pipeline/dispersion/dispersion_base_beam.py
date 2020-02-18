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
		'ANO:STRING, '
		'MES:STRING, '
		'NOMBRE_GERENTE:STRING, '
		'CEDULA:STRING, '
		'NOMBRE:STRING, '
		'CARGO:STRING, '
		'COMISION:STRING, '
		'COSTO:STRING, '
		'INGRESO:STRING, '
		'ASEGURA:STRING, '
		'META_ASEGURA:STRING, '
		'OCUPACION:STRING, '
		'META_RECAUDO:STRING, '
		'EJECUCION_RECAUDO:STRING, '
		'OPERACION:STRING, '
		'CENTRO_DE_COSTOS:STRING, '
		'CATEGORIA:STRING, '
		'TIPO_DE_OPERACION:STRING, '
		'CC_TEAM_LEADER:STRING, '
		'NOMBRE_TEAM_LEADER:STRING, '
		'CC_EJECUTIVO:STRING, '
		'NOMBRE_EJECUTIVO:STRING, '
		'CUMPLIMIENTO:STRING, '
		'AHT:STRING, '
		'ASEGURAMIENTO:STRING, '
		'PRODUCTIVIDAD_EFECTIVIDAD:STRING, '
		'NIVEL_DESEMPENO:STRING, '
		'CLASIFICACION:STRING, '
		'CLASIFICACION_BINARIZADA:STRING, '
		'NO_CLASIFICADOS_BINARIZADO:STRING, '
		'SOBRESALIENTE_B:STRING, '
		'OBJETIVO_B:STRING, '
		'FO_BINARIZADO:STRING, '
		'DEFICIENTE_B:STRING, '
		'CLASIFICA:STRING, '
		'NO_CLASIFICA:STRING '









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
				'ANO' : arrayCSV[0],
				'MES' : arrayCSV[1],
				'NOMBRE_GERENTE' : arrayCSV[2],
				'CEDULA' : arrayCSV[3],
				'NOMBRE' : arrayCSV[4],
				'CARGO' : arrayCSV[5],
				'COMISION' : arrayCSV[6],
				'COSTO' : arrayCSV[7],
				'INGRESO' : arrayCSV[8],
				'ASEGURA' : arrayCSV[9],
				'META_ASEGURA' : arrayCSV[10],
				'OCUPACION' : arrayCSV[11],
				'META_RECAUDO' : arrayCSV[12],
				'EJECUCION_RECAUDO' : arrayCSV[13],
				'OPERACION' : arrayCSV[14],
				'CENTRO_DE_COSTOS' : arrayCSV[15],
				'CATEGORIA' : arrayCSV[16],
				'TIPO_DE_OPERACION' : arrayCSV[17],
				'CC_TEAM_LEADER' : arrayCSV[18],
				'NOMBRE_TEAM_LEADER' : arrayCSV[19],
				'CC_EJECUTIVO' : arrayCSV[20],
				'NOMBRE_EJECUTIVO' : arrayCSV[21],
				'CUMPLIMIENTO' : arrayCSV[22],
				'AHT' : arrayCSV[23],
				'ASEGURAMIENTO' : arrayCSV[24],
				'PRODUCTIVIDAD_EFECTIVIDAD' : arrayCSV[25],
				'NIVEL_DESEMPENO' : arrayCSV[26],
				'CLASIFICACION' : arrayCSV[27],
				'CLASIFICACION_BINARIZADA' : arrayCSV[28],
				'NO_CLASIFICADOS_BINARIZADO' : arrayCSV[29],
				'SOBRESALIENTE_B' : arrayCSV[30],
				'OBJETIVO_B' : arrayCSV[31],
				'FO_BINARIZADO' : arrayCSV[32],
				'DEFICIENTE_B' : arrayCSV[33],
				'CLASIFICA' : arrayCSV[34],
				'NO_CLASIFICA' : arrayCSV[35]












				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-dispersion" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery base' >> beam.io.WriteToBigQuery(
		gcs_project + ":dispersion.base", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



