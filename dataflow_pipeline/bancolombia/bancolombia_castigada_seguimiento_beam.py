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
	'Consecutivo_Gestion:STRING, '
	'Duracion:STRING, '
	'Consecutivo_Obligacion:STRING, '
	'Nit:STRING, '
	'Nombre:STRING, '
	'Nro_Documento:STRING, '
	'Fecha_Gestion:STRING, '
	'Dias_Mora:STRING, '
	'Asesor:STRING, '
	'Regional:STRING, '
	'Tipo:STRING, '
	'Telefono:STRING, '
	'Grabador:STRING, '
	'Nombre_Abogado:STRING, '
	'Codigo_Abogado:STRING, '
	'Codigo_de_Gestion:STRING, '
	'Desc_Ultimo_Codigo_De_Gestion_Prejuridico:STRING, '
	'Fecha_Venci:STRING, '
	'Ocupacion:STRING, '
	't_entrada:STRING, '
	'Hora_Grabacion:STRING, '
	't_igraba:STRING, '
	'Sitio:STRING, '
	'Hora_De_Compromiso:STRING, '
	'Codigo_De_Causal:STRING, '
	'Codigo_De_Cobro_Anterior:STRING, '
	'codcob_mec_no:STRING, '
	'codcob_mec_nor_tra:STRING, '
	'codcob_mec_nor_util:STRING, '
	'codcob_t:STRING, '
	'Codigo_de_Contacto:STRING, '
	'Control:STRING, '
	'consdirec:STRING, '
	'cuadrante:STRING, '
	'Fecha_Promesa:STRING, '
	'Nota:STRING '

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
				'fecha': self.mifecha,
				'Consecutivo_Gestion': arrayCSV[0],
				'Duracion': arrayCSV[1],
				'Consecutivo_Obligacion': arrayCSV[2],
				'Nit': arrayCSV[3],
				'Nombre': arrayCSV[4],
				'Nro_Documento': arrayCSV[5],
				'Fecha_Gestion': arrayCSV[6],
				'Dias_Mora': arrayCSV[7],
				'Asesor': arrayCSV[8],
				'Regional': arrayCSV[9],
				'Tipo': arrayCSV[10],
				'Telefono': arrayCSV[11],
				'Grabador': arrayCSV[12],
				'Nombre_Abogado': arrayCSV[13],
				'Codigo_Abogado': arrayCSV[14],
				'Codigo_de_Gestion': arrayCSV[15],
				'Desc_Ultimo_Codigo_De_Gestion_Prejuridico': arrayCSV[16],
				'Fecha_Venci': arrayCSV[17],
				'Ocupacion': arrayCSV[18],
				't_entrada': arrayCSV[19],
				'Hora_Grabacion': arrayCSV[20],
				't_igraba': arrayCSV[21],
				'Sitio': arrayCSV[22],
				'Hora_De_Compromiso': arrayCSV[23],
				'Codigo_De_Causal': arrayCSV[24],
				'Codigo_De_Cobro_Anterior': arrayCSV[25],
				'codcob_mec_no': arrayCSV[26],
				'codcob_mec_nor_tra': arrayCSV[27],
				'codcob_mec_nor_util': arrayCSV[28],
				'codcob_t': arrayCSV[29],
				'Codigo_de_Contacto': arrayCSV[30],
				'Control': arrayCSV[31],
				'consdirec': arrayCSV[32],
				'cuadrante': arrayCSV[33],
				'Fecha_Promesa': arrayCSV[34],
				'Nota': arrayCSV[35]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia" #Definicion de la raiz del bucket
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
		gcs_project + ":bancolombia_castigada.seguimiento", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



