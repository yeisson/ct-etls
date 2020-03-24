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
	'NOMBRE:STRING, '
	'APELLIDO:STRING, '
	'TIPOID:STRING, '
	'ID:STRING, '
	'EDAD:STRING, '
	'SEXO:STRING, '
	'PAIS:STRING, '
	'DEPARTAMENTO:STRING, '
	'CIUDAD:STRING, '
	'ZONA:STRING, '
	'DIRECCION:STRING, '
	'OPT1:STRING, '
	'OPT2:STRING, '
	'OPT3:STRING, '
	'OPT4:STRING, '
	'OPT5:STRING, '
	'OPT6:STRING, '
	'OPT7:STRING, '
	'OPT8:STRING, '
	'OPT9:STRING, '
	'OPT10:STRING, '
	'OPT11:STRING, '
	'OPT12:STRING, '
	'TEL1:STRING, '
	'TEL2:STRING, '
	'TEL3:STRING, '
	'TEL4:STRING, '
	'TEL5:STRING, '
	'TEL6:STRING, '
	'TEL7:STRING, '
	'TEL8:STRING, '
	'TEL9:STRING, '
	'TEL10:STRING, '
	'OTROSTEL:STRING, '
	'EMAIL:STRING, '
	'RECALL_INFO:STRING, '
	'AGENTE:STRING, '
	'RESULTADOREG:STRING, '
	'FECHAFINREG:STRING, '
	'LLAMADAS:STRING, '
	'IDCALL:STRING, '
	'COD01:STRING, '
	'COD02:STRING, '
	'COMENTARIOSACUMULADOS:STRING, '
	'DATE_RECALL:STRING, '
	'COUNT_RECALL:STRING, '
	'TEL_RECALL:STRING, '
	'LAST_DIAL_TEL:STRING, '
	'HISTORY_TEL:STRING '
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
				'NOMBRE' : arrayCSV[0],
				'APELLIDO' : arrayCSV[1],
				'TIPOID' : arrayCSV[2],
				'ID' : arrayCSV[3],
				'EDAD' : arrayCSV[4],
				'SEXO' : arrayCSV[5],
				'PAIS' : arrayCSV[6],
				'DEPARTAMENTO' : arrayCSV[7],
				'CIUDAD' : arrayCSV[8],
				'ZONA' : arrayCSV[9],
				'DIRECCION' : arrayCSV[10],
				'OPT1' : arrayCSV[11],
				'OPT2' : arrayCSV[12],
				'OPT3' : arrayCSV[13],
				'OPT4' : arrayCSV[14],
				'OPT5' : arrayCSV[15],
				'OPT6' : arrayCSV[16],
				'OPT7' : arrayCSV[17],
				'OPT8' : arrayCSV[18],
				'OPT9' : arrayCSV[19],
				'OPT10' : arrayCSV[20],
				'OPT11' : arrayCSV[21],
				'OPT12' : arrayCSV[22],
				'TEL1' : arrayCSV[23],
				'TEL2' : arrayCSV[24],
				'TEL3' : arrayCSV[25],
				'TEL4' : arrayCSV[26],
				'TEL5' : arrayCSV[27],
				'TEL6' : arrayCSV[28],
				'TEL7' : arrayCSV[29],
				'TEL8' : arrayCSV[30],
				'TEL9' : arrayCSV[31],
				'TEL10' : arrayCSV[32],
				'OTROSTEL' : arrayCSV[33],
				'EMAIL' : arrayCSV[34],
				'RECALL_INFO' : arrayCSV[35],
				'AGENTE' : arrayCSV[36],
				'RESULTADOREG' : arrayCSV[37],
				'FECHAFINREG' : arrayCSV[38],
				'LLAMADAS' : arrayCSV[39],
				'IDCALL' : arrayCSV[40],
				'COD01' : arrayCSV[41],
				'COD02' : arrayCSV[42],
				'COMENTARIOSACUMULADOS' : arrayCSV[43],
				'DATE_RECALL' : arrayCSV[44],
				'COUNT_RECALL' : arrayCSV[45],
				'TEL_RECALL' : arrayCSV[46],
				'LAST_DIAL_TEL' : arrayCSV[47],
				'HISTORY_TEL' : arrayCSV[48]
				}
		
		return [tupla]

def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia_castigada" #Definicion de la raiz del bucket
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
		gcs_project + ":bancolombia_castigada.tts", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



