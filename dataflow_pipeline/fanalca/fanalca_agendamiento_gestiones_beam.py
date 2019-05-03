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
	'CHASIS:STRING, '
	'CEDULA:STRING, '
	'NOMBRE:STRING, '
	'PLACA:STRING, '
	'DENOMINACION_OBJETO:STRING, '
	'CONCESIONARIO:STRING, '
	'ULTIMO_SERVICIO:STRING, '
	'FECHA_2:STRING, '
	'PROXIMO_SERVICIO:STRING, '
	'E_MAIL:STRING, '
	'AGENCIA:STRING, '
	'GESTION:STRING, '
	'CAUSAL:STRING, '
	'SUBCAUSAL:STRING, '
	'OBSERVACION_QUEJA:STRING, '
	'FECHA_COMPROMISO_AGEND:STRING, '
	'HORA_COMPROMISO_AGEND:STRING, '
	'CONCESIONARIO_2:STRING, '
	'AGENCIA_2:STRING, '
	'CIUDAD:STRING, '
	'DOCUMENTO_VISITA:STRING, '
	'NOMBRE_VISITA:STRING, '
	'TELEFONO_FIJO:STRING, '
	'TELEFONO_CELULAR:STRING, '
	'EMAIL:STRING, '
	'CELULAR_SMS:STRING, '
	'OBSERVACION:STRING, '
	'FECHA_GESTION:STRING, '
	'ASESOR:STRING, '
	'PLACA_DIGITADA:STRING '


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
				'CHASIS': arrayCSV[0],
				'CEDULA': arrayCSV[1],
				'NOMBRE': arrayCSV[2],
				'PLACA': arrayCSV[3],
				'DENOMINACION_OBJETO': arrayCSV[4],
				'CONCESIONARIO': arrayCSV[5],
				'ULTIMO_SERVICIO': arrayCSV[6],
				'FECHA_2': arrayCSV[7],
				'PROXIMO_SERVICIO': arrayCSV[8],
				'E_MAIL': arrayCSV[9],
				'AGENCIA': arrayCSV[10],
				'GESTION': arrayCSV[11],
				'CAUSAL': arrayCSV[12],
				'SUBCAUSAL': arrayCSV[13],
				'OBSERVACION_QUEJA': arrayCSV[14],
				'FECHA_COMPROMISO_AGEND': arrayCSV[15],
				'HORA_COMPROMISO_AGEND': arrayCSV[16],
				'CONCESIONARIO_2': arrayCSV[17],
				'AGENCIA_2': arrayCSV[18],
				'CIUDAD': arrayCSV[19],
				'DOCUMENTO_VISITA': arrayCSV[20],
				'NOMBRE_VISITA': arrayCSV[21],
				'TELEFONO_FIJO': arrayCSV[22],
				'TELEFONO_CELULAR': arrayCSV[23],
				'EMAIL': arrayCSV[24],
				'CELULAR_SMS': arrayCSV[25],
				'OBSERVACION': arrayCSV[26],
				'FECHA_GESTION': arrayCSV[27],
				'ASESOR': arrayCSV[28],
				'PLACA_DIGITADA': arrayCSV[29]


				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-fanalca" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery fanalca' >> beam.io.WriteToBigQuery(
		gcs_project + ":fanalca_agendamiento.gestiones", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



