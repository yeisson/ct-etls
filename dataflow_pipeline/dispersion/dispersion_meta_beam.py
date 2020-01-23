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
		'GERENTE:STRING, '
		'CEDULA:STRING, '
		'NEGOCIADOR:STRING, '
		'CARGO:STRING, '
		'VARIABLE:STRING, '
		'COSTO:STRING, '
		'INGRESO_MES_ANTERIOR:STRING, '
		'META_INGRESO_FACTURA:STRING, '
		'META_RECAUDO_MES_ANTERIOR:STRING, '
		'EJECUCION_RECAUDO_MES_ANTERIOR:STRING, '
		'META_RECAUDO_ENERO_2020:STRING, '
		'EJECUCION_RECAUDO_ENERO_2020:STRING, '
		'ASEGURAMIENTO_ANTERIOR_META:STRING, '
		'ASEGURAMIENTO_ANTERIOR_EJEC:STRING, '
		'META_ASEGURAMIENTO:STRING, '
		'EJECUCION_ASEGURAMIENTO:STRING, '
		'OPERACION:STRING, '
		'CENTRO_DE_COSTOS:STRING, '
		'CATEGORIA:STRING, '
		'TIPO_DE_OPERACION:STRING, '
		'CC_TEAM_LEADER:STRING, '
		'NOMBRE_TEAM_LEADER:STRING, '
		'CC_EJECUTIVO:STRING, '
		'NOMBRE_EJECUTIVO:STRING, '
		'CUMPLIMIENTO_MES_ANTERIOR:STRING, '
		'CUMPLIMIENTO_META:STRING, '
		'AHT:STRING, '
		'ASEGURAMIENTO:STRING, '
		'PRODUCTIVIDAD_EFECTIVIDAD:STRING, '
		'CLASIFICACION:STRING, '
		'NIVEL_MES_ANTERIOR:STRING, '
		'NIVEL_META:STRING, '
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
				'GERENTE' : arrayCSV[2],
				'CEDULA' : arrayCSV[3],
				'NEGOCIADOR' : arrayCSV[4],
				'CARGO' : arrayCSV[5],
				'VARIABLE' : arrayCSV[6],
				'COSTO' : arrayCSV[7],
				'INGRESO_MES_ANTERIOR' : arrayCSV[8],
				'META_INGRESO_FACTURA' : arrayCSV[9],
				'META_RECAUDO_MES_ANTERIOR' : arrayCSV[10],
				'EJECUCION_RECAUDO_MES_ANTERIOR' : arrayCSV[11],
				'META_RECAUDO_ENERO_2020' : arrayCSV[12],
				'EJECUCION_RECAUDO_ENERO_2020' : arrayCSV[13],
				'ASEGURAMIENTO_ANTERIOR_META' : arrayCSV[14],
				'ASEGURAMIENTO_ANTERIOR_EJEC' : arrayCSV[15],
				'META_ASEGURAMIENTO' : arrayCSV[16],
				'EJECUCION_ASEGURAMIENTO' : arrayCSV[17],
				'OPERACION' : arrayCSV[18],
				'CENTRO_DE_COSTOS' : arrayCSV[19],
				'CATEGORIA' : arrayCSV[20],
				'TIPO_DE_OPERACION' : arrayCSV[21],
				'CC_TEAM_LEADER' : arrayCSV[22],
				'NOMBRE_TEAM_LEADER' : arrayCSV[23],
				'CC_EJECUTIVO' : arrayCSV[24],
				'NOMBRE_EJECUTIVO' : arrayCSV[25],
				'CUMPLIMIENTO_MES_ANTERIOR' : arrayCSV[26],
				'CUMPLIMIENTO_META' : arrayCSV[27],
				'AHT' : arrayCSV[28],
				'ASEGURAMIENTO' : arrayCSV[29],
				'PRODUCTIVIDAD_EFECTIVIDAD' : arrayCSV[30],
				'CLASIFICACION' : arrayCSV[31],
				'NIVEL_MES_ANTERIOR' : arrayCSV[32],
				'NIVEL_META' : arrayCSV[33],
				'NO_CLASIFICADOS_BINARIZADO' : arrayCSV[34],
				'SOBRESALIENTE_B' : arrayCSV[35],
				'OBJETIVO_B' : arrayCSV[36],
				'FO_BINARIZADO' : arrayCSV[37],
				'DEFICIENTE_B' : arrayCSV[38],
				'CLASIFICA' : arrayCSV[39],
				'NO_CLASIFICA' : arrayCSV[40]












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

	transformed | 'Escritura a BigQuery metas' >> beam.io.WriteToBigQuery(
		gcs_project + ":dispersion.metas", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



