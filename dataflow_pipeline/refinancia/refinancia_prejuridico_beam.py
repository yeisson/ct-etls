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
'IDENTIFICACION:STRING, '
'NOMBRECOMPLETO:STRING, '
'IDCLIENTE:STRING, '
'IDPORTAFOLIO:STRING, '
'PORTAFOLIO:STRING, '
'IDASESOR:STRING, '
'PERFILCLIENTE:STRING, '
'ESTADOCOMERCIAL:STRING, '
'TOTALSALDOCAPITAL:STRING, '
'FUNCIONARIO:STRING, '
'TOTALDEUDA:STRING, '
'FABRICA:STRING, '
'CANAL:STRING, '
'SEGMENTO:STRING, '
'SCORE:STRING, '
'CASA_COBRANZA:STRING, '
'TIPOASIGNACION:STRING '
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
				'IDENTIFICACION' : arrayCSV[0].replace('"',''),
				'NOMBRECOMPLETO' : arrayCSV[1].replace('"',''),
				'IDCLIENTE' : arrayCSV[2].replace('"',''),
				'IDPORTAFOLIO' : arrayCSV[3].replace('"',''),
				'PORTAFOLIO' : arrayCSV[4].replace('"',''),
				'IDASESOR' : arrayCSV[5].replace('"',''),
				'PERFILCLIENTE' : arrayCSV[6].replace('"',''),
				'ESTADOCOMERCIAL' : arrayCSV[7].replace('"',''),
				'TOTALSALDOCAPITAL' : arrayCSV[8].replace('"',''),
				'FUNCIONARIO' : arrayCSV[9].replace('"',''),
				'TOTALDEUDA' : arrayCSV[10].replace('"',''),
				'FABRICA' : arrayCSV[11].replace('"',''),
				'CANAL' : arrayCSV[12].replace('"',''),
				'SEGMENTO' : arrayCSV[13].replace('"',''),
				'SCORE' : arrayCSV[14].replace('"',''),
				'CASA_COBRANZA' : arrayCSV[15].replace('"',''),
				'TIPOASIGNACION' : arrayCSV[16].replace('"','')

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-refinancia" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery refinancia' >> beam.io.WriteToBigQuery(
		gcs_project + ":refinancia.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



