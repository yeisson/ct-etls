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
		'ENTRY_ID:STRING, '
		'NUMCASO:STRING, '
		'COMO_CALIFICAS_EL_SERVICIO_RECIBIDO_EN_NUESTRA_LINEA_DE_SERVICIO:STRING, '
		'ESCRIBE_AQUI:STRING, '
		'EL_ASESOR_ME_RESOLVIO_TODAS_MIS_DUDAS_Y_NECESIDADES:STRING, '
		'EL_CONOCIMIENTO_Y_CLARIDAD_DEL_ASESOR:STRING, '
		'LA_CALIDEZ_Y_ACTITUD_DEL_ASESOR_PARA_RESOLVER_TUS_DUDAS_O_NECESIDADES:STRING, '
		'EL_TIEMPO_DE_ESPERA_PARA_ATENDERTE:STRING, '
		'LA_FACILIDAD_PARA_MANEJAR_LAS_OPCIONES_DEL_MENU:STRING, '
		'DATE_CREATED:STRING, '
		'CREATED_BY:STRING, '
		'LAST_UPDATED:STRING, '
		'UPDATED_BY:STRING, '
		'IP_ADDRESS:STRING, '
		'LAST_PAGE_ACCESSED:STRING, '
		'COMPLETION_STATUS:STRING '



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
				'ENTRY_ID' : arrayCSV[0],
				'NUMCASO' : arrayCSV[1],
				'COMO_CALIFICAS_EL_SERVICIO_RECIBIDO_EN_NUESTRA_LINEA_DE_SERVICIO' : arrayCSV[2],
				'ESCRIBE_AQUI' : arrayCSV[3],
				'EL_ASESOR_ME_RESOLVIO_TODAS_MIS_DUDAS_Y_NECESIDADES' : arrayCSV[4],
				'EL_CONOCIMIENTO_Y_CLARIDAD_DEL_ASESOR' : arrayCSV[5],
				'LA_CALIDEZ_Y_ACTITUD_DEL_ASESOR_PARA_RESOLVER_TUS_DUDAS_O_NECESIDADES' : arrayCSV[6],
				'EL_TIEMPO_DE_ESPERA_PARA_ATENDERTE' : arrayCSV[7],
				'LA_FACILIDAD_PARA_MANEJAR_LAS_OPCIONES_DEL_MENU' : arrayCSV[8],
				'DATE_CREATED' : arrayCSV[9],
				'CREATED_BY' : arrayCSV[10],
				'LAST_UPDATED' : arrayCSV[11],
				'UPDATED_BY' : arrayCSV[12],
				'IP_ADDRESS' : arrayCSV[13],
				'LAST_PAGE_ACCESSED' : arrayCSV[14],
				'COMPLETION_STATUS' : arrayCSV[15]
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
		gcs_project + ":proteccion.satu", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



