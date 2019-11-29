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
	'CAMPANA:STRING, '
	'NOMBRE_DE_CUENTA:STRING, '
	'NOMBRE_DE_COMUNICACION:STRING, '
	'PLANTILLA_DE_COMUNICACION:STRING, '
	'DE:STRING, '
	'A:STRING, '
	'TELEFONO:STRING, '
	'ID_DEL_MENSAJE:STRING, '
	'ENVIAR_EL:STRING, '
	'PREFIJO_DE_PAIS:STRING, '
	'NOMBRE_DE_PAIS:STRING, '
	'NOMBRE_DE_RED:STRING, '
	'PRECIO_DE_COMPRA:STRING, '
	'MONEDA_DE_COMPRA:STRING, '
	'PRECIO_DE_VENTA:STRING, '
	'MONEDA_DE_VENTA:STRING, '
	'TIPO_DE_MENSAJE:STRING, '
	'ESTADO:STRING, '
	'RAZON:STRING, '
	'ACCION:STRING, '
	'GRUPO_DE_ERROR:STRING, '
	'NOMBRE_DE_ERROR:STRING, '
	'HECHO_EL:STRING, '
	'TEXTO:STRING, '
	'MESSAGE_LENGTH:STRING, '
	'CONTEO_DE_MENSAJES:STRING, '
	'NOMBRE_DE_SERVICIO:STRING, '
	'NOMBRE_DE_USUARIO:STRING, '
	'ID_DE_MENSAJE_SINCRONIZADO:STRING, '
	'URL:STRING, '
	'URL_TOTAL_CLICKS:STRING, '
	'URL_FIRST_CLICK_DATE_TIME:STRING, '
	'DATA_PAYLOAD:STRING '

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
				'CAMPANA' : arrayCSV[0],
				'NOMBRE_DE_CUENTA' : arrayCSV[1],
				'NOMBRE_DE_COMUNICACION' : arrayCSV[2],
				'PLANTILLA_DE_COMUNICACION' : arrayCSV[3],
				'DE' : arrayCSV[4],
				'A' : arrayCSV[5],
				'TELEFONO' : arrayCSV[6],
				'ID_DEL_MENSAJE' : arrayCSV[7],
				'ENVIAR_EL' : arrayCSV[8],
				'PREFIJO_DE_PAIS' : arrayCSV[9],
				'NOMBRE_DE_PAIS' : arrayCSV[10],
				'NOMBRE_DE_RED' : arrayCSV[11],
				'PRECIO_DE_COMPRA' : arrayCSV[12],
				'MONEDA_DE_COMPRA' : arrayCSV[13],
				'PRECIO_DE_VENTA' : arrayCSV[14],
				'MONEDA_DE_VENTA' : arrayCSV[15],
				'TIPO_DE_MENSAJE' : arrayCSV[16],
				'ESTADO' : arrayCSV[17],
				'RAZON' : arrayCSV[18],
				'ACCION' : arrayCSV[19],
				'GRUPO_DE_ERROR' : arrayCSV[20],
				'NOMBRE_DE_ERROR' : arrayCSV[21],
				'HECHO_EL' : arrayCSV[22],
				'TEXTO' : arrayCSV[23],
				'MESSAGE_LENGTH' : arrayCSV[24],
				'CONTEO_DE_MENSAJES' : arrayCSV[25],
				'NOMBRE_DE_SERVICIO' : arrayCSV[26],
				'NOMBRE_DE_USUARIO' : arrayCSV[27],
				'ID_DE_MENSAJE_SINCRONIZADO' : arrayCSV[28],
				'URL' : arrayCSV[29],
				'URL_TOTAL_CLICKS' : arrayCSV[30],
				'URL_FIRST_CLICK_DATE_TIME' : arrayCSV[31],
				'DATA_PAYLOAD' : arrayCSV[32]
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-ucc" #Definicion de la raiz del bucket
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
		gcs_project + ":ucc.sms", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")