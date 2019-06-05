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
	'NIT:STRING, '
	'NOMBRES:STRING, '
	'NOOBLIGACION:STRING, '
	'CODIGO_ANTERIOR:STRING, '
	'CLASE_GESTION:STRING, '
	'RESPONSABLE_COBRO:STRING, '
	'GESTOR:STRING, '
	'CODIGO_GESTION:STRING, '
	'DESCRIPCION_CAUSAL:STRING, '
	'DIAS_MORA:STRING, '
	'TELEFONO:STRING, '
	'FECHA_GESTION:STRING, '
	'DURACION:STRING, '
	'HORA_DE_INICIO_DE_GRABACION:STRING, '
	'HORA_DE_GRABACION:STRING, '
	'VALOR_COMPROMISO:STRING, '
	'FECHA_COMPROMISO:STRING, '
	'CUOTAS_VENCIDAS:STRING, '
	'DIRECCION:STRING, '
	'FECHA_VENCIMIENTO:STRING, '
	'CODIGO_DE_CONTACTO:STRING, '
	'CONSDOCDEU:STRING, '
	'NOTA:STRING, '
	'GESTION_MOVIL:STRING, '
	'DESCRIPCION_ZONA:STRING, '
	'DESCRIPCION_NEGOCIO:STRING, '
	'CARTERA:STRING '

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
				# 'fecha' : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),	#datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'NIT' : arrayCSV[0],
				'NOMBRES' : arrayCSV[1],
				'NOOBLIGACION' : arrayCSV[2],
				'CODIGO_ANTERIOR' : arrayCSV[3],
				'CLASE_GESTION' : arrayCSV[4],
				'RESPONSABLE_COBRO' : arrayCSV[5],
				'GESTOR' : arrayCSV[6],
				'CODIGO_GESTION' : arrayCSV[7],
				'DESCRIPCION_CAUSAL' : arrayCSV[8],
				'DIAS_MORA' : arrayCSV[9],
				'TELEFONO' : arrayCSV[10],
				'FECHA_GESTION' : arrayCSV[11],
				'DURACION' : arrayCSV[12],
				'HORA_DE_INICIO_DE_GRABACION' : arrayCSV[13],
				'HORA_DE_GRABACION' : arrayCSV[14],
				'VALOR_COMPROMISO' : arrayCSV[15],
				'FECHA_COMPROMISO' : arrayCSV[16],
				'CUOTAS_VENCIDAS' : arrayCSV[17],
				'DIRECCION' : arrayCSV[18],
				'FECHA_VENCIMIENTO' : arrayCSV[19],
				'CODIGO_DE_CONTACTO' : arrayCSV[20],
				'CONSDOCDEU' : arrayCSV[21],
				'NOTA' : arrayCSV[22],
				'GESTION_MOVIL' : arrayCSV[23],
				'DESCRIPCION_ZONA' : arrayCSV[24],
				'DESCRIPCION_NEGOCIO' : arrayCSV[25],
				'CARTERA' : arrayCSV[26]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-adeinco_juridico" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

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
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/prejuridico/info_carga_banco_prej",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Adeinco' >> beam.io.WriteToBigQuery(
		gcs_project + ":adeinco_juridico.gestiones", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



