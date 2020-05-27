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
	'CODINSTITUCION:STRING, '
	'INSTITUCION:STRING, '
	'CODSEDE:STRING, '
	'SEDE:STRING, '
	'CODGRADO:STRING, '
	'GRADO_ACADEMICO:STRING, '
	'CODPROGACAD:STRING, '
	'PROGRAMA_ACADEMICO:STRING, '
	'SEGUNDA_OPCION:STRING, '
	'CODACCION:STRING, '
	'ACCION_MOTIVO:STRING, '
	'CICLO_LECTIVO:STRING, '
	'DESCR_CICLO:STRING, '
	'N_SOLIC:STRING, '
	'TIPO_DOC_ID:STRING, '
	'DOC_ID:STRING, '
	'NOMBRE:STRING, '
	'NOMBRE_2:STRING, '
	'APELLIDO:STRING, '
	'APELLIDO_2:STRING, '
	'CODTIPOADMISION:STRING, '
	'TIPO_ADMISION:STRING, '
	'SEXO:STRING, '
	'NIVEL_SOCIOECONOMICO:STRING, '
	'CORREO_E:STRING, '
	'TELEFONO:STRING, '
	'LOCALIDAD:STRING, '
	'ULT_CTR_DOCEN:STRING, '
	'NOMBRE_ULTIMO_CTR_DOCENTE:STRING, '
	'TIPO_CENTRO_DOC:STRING, '
	'NOMBRE_OTRO_CENTRO_DOC:STRING, '
	'SNP:STRING, '
	'F_EXAMEN:STRING, '
	'F_NACIMIENTO:STRING, '
	'LUGAR_NACIM:STRING, '
	'F_EFVA:STRING, '
	'UC_HORA_PREFER:STRING, '
	'FUENTE_REFERENCIA:STRING, '
	'GRUPO:STRING, '
	'RH:STRING '
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
				'ID' : arrayCSV[0],
				'CODINSTITUCION' : arrayCSV[1],
				'INSTITUCION' : arrayCSV[2],
				'CODSEDE' : arrayCSV[3],
				'SEDE' : arrayCSV[4],
				'CODGRADO' : arrayCSV[5],
				'GRADO_ACADEMICO' : arrayCSV[6],
				'CODPROGACAD' : arrayCSV[7],
				'PROGRAMA_ACADEMICO' : arrayCSV[8],
				'SEGUNDA_OPCION' : arrayCSV[9],
				'CODACCION' : arrayCSV[10],
				'ACCION_MOTIVO' : arrayCSV[11],
				'CICLO_LECTIVO' : arrayCSV[12],
				'DESCR_CICLO' : arrayCSV[13],
				'N_SOLIC' : arrayCSV[14],
				'TIPO_DOC_ID' : arrayCSV[15],
				'DOC_ID' : arrayCSV[16],
				'NOMBRE' : arrayCSV[17],
				'NOMBRE_2' : arrayCSV[18],
				'APELLIDO' : arrayCSV[19],
				'APELLIDO_2' : arrayCSV[20],
				'CODTIPOADMISION' : arrayCSV[21],
				'TIPO_ADMISION' : arrayCSV[22],
				'SEXO' : arrayCSV[23],
				'NIVEL_SOCIOECONOMICO' : arrayCSV[24],
				'CORREO_E' : arrayCSV[25],
				'TELEFONO' : arrayCSV[26],
				'LOCALIDAD' : arrayCSV[27],
				'ULT_CTR_DOCEN' : arrayCSV[28],
				'NOMBRE_ULTIMO_CTR_DOCENTE' : arrayCSV[29],
				'TIPO_CENTRO_DOC' : arrayCSV[30],
				'NOMBRE_OTRO_CENTRO_DOC' : arrayCSV[31],
				'SNP' : arrayCSV[32],
				'F_EXAMEN' : arrayCSV[33],
				'F_NACIMIENTO' : arrayCSV[34],
				'LUGAR_NACIM' : arrayCSV[35],
				'F_EFVA' : arrayCSV[36],
				'UC_HORA_PREFER' : arrayCSV[37],
				'FUENTE_REFERENCIA' : arrayCSV[38],
				'GRUPO' : arrayCSV[39],
				'RH' : arrayCSV[40]

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
		gcs_project + ":ucc.base_inscritos", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")