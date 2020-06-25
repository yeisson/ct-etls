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
		'ID:STRING, '
		'COMPLETADO:STRING, '
		'ULTIMA_PAGINA:STRING, '
		'LENGUAJE_INICIAL:STRING, '
		'FECHA_DE_INICIO:STRING, '
		'FECHA_DE_LA_ULTIMA_ACCION:STRING, '
		'OPERACION:STRING, '
		'AGENTE:STRING, '
		'ID_CORREO:STRING, '
		'FECHA_DE_RECEPCION:STRING, '
		'HORA_DE_RECEPCION:STRING, '
		'FECHA_DE_RESPUESTA:STRING, '
		'HORA_DE_RESPUESTA:STRING, '
		'REMITENTE:STRING, '
		'TIPO_CORREO:STRING, '
		'TIPO_CORREO__OTRO:STRING, '
		'CUAL:STRING '












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
				'fecha': self.mifecha,
				'ID' : arrayCSV[0],
				'COMPLETADO' : arrayCSV[1],
				'ULTIMA_PAGINA' : arrayCSV[2],
				'LENGUAJE_INICIAL' : arrayCSV[3],
				'FECHA_DE_INICIO' : arrayCSV[4],
				'FECHA_DE_LA_ULTIMA_ACCION' : arrayCSV[5],
				'OPERACION' : arrayCSV[6],
				'AGENTE' : arrayCSV[7],
				'ID_CORREO' : arrayCSV[8],
				'FECHA_DE_RECEPCION' : arrayCSV[9],
				'HORA_DE_RECEPCION' : arrayCSV[10],
				'FECHA_DE_RESPUESTA' : arrayCSV[11],
				'HORA_DE_RESPUESTA' : arrayCSV[12],
				'REMITENTE' : arrayCSV[13],
				'TIPO_CORREO' : arrayCSV[14],
				'TIPO_CORREO__OTRO' : arrayCSV[15],
				'CUAL' : arrayCSV[16]









				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-pto" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Mobility' >> beam.io.WriteToBigQuery(
		gcs_project + ":Auteco_Mobility.correop", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



