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
	'DIA_MES:STRING, '
	'MES:STRING, '
	'CEDULA:STRING, '
	'NOMBRE:STRING, '
	'TEAM_LEADER:STRING, '
	'JORNADA:STRING, '
	'DIA:STRING, '
	'PRETURNO:STRING, '
	'HORA_ENTRADA:STRING, '
	'HORA_SALIDA:STRING, '
	'TOTAL_HORAS:STRING, '
	'INICIO_REU:STRING, '
	'FIN_REU:STRING, '
	'INICIO_CAPACITACION:STRING, '
	'FIN_CAPACITACION:STRING, '
	'DESCANSO1:STRING, '
	'DESCANSO2:STRING, '
	'DESCANSO3:STRING, '
	'HORAS_GESTION:STRING, '
	'SEGMENTO:STRING, '
	'TIEMPO_TOTAL_CAPACITACION:STRING, '
	'ENTRENAMIENTO:STRING, '
	'REUNION:STRING, '
	'OBSERVACIONES:STRING, '
	'HORAS_REU:STRING, '
	'DESCANSOS:STRING, '
	'DIFERENCIA_DESCANSO:STRING, '
	'DIFERENCIA_DE_DESCANSO_INTERMEDIO:STRING, '
	'DIFERENCIA_DESCANSO_FINAL:STRING '


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
				'DIA_MES' : arrayCSV[0],
				'MES' : arrayCSV[1],
				'CEDULA' : arrayCSV[2],
				'NOMBRE' : arrayCSV[3],
				'TEAM_LEADER' : arrayCSV[4],
				'JORNADA' : arrayCSV[5],
				'DIA' : arrayCSV[6],
				'PRETURNO' : arrayCSV[7],
				'HORA_ENTRADA' : arrayCSV[8],
				'HORA_SALIDA' : arrayCSV[9],
				'TOTAL_HORAS' : arrayCSV[10],
				'INICIO_REU' : arrayCSV[11],
				'FIN_REU' : arrayCSV[12],
				'INICIO_CAPACITACION' : arrayCSV[13],
				'FIN_CAPACITACION' : arrayCSV[14],
				'DESCANSO1' : arrayCSV[15],
				'DESCANSO2' : arrayCSV[16],
				'DESCANSO3' : arrayCSV[17],
				'HORAS_GESTION' : arrayCSV[18],
				'SEGMENTO' : arrayCSV[19],
				'TIEMPO_TOTAL_CAPACITACION' : arrayCSV[20],
				'ENTRENAMIENTO' : arrayCSV[21],
				'REUNION' : arrayCSV[22],
				'OBSERVACIONES' : arrayCSV[23],
				'HORAS_REU' : arrayCSV[24],
				'DESCANSOS' : arrayCSV[25],
				'DIFERENCIA_DESCANSO' : arrayCSV[26],
				'DIFERENCIA_DE_DESCANSO_INTERMEDIO' : arrayCSV[27],
				'DIFERENCIA_DESCANSO_FINAL' : arrayCSV[28],







				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-turnos" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery turnos' >> beam.io.WriteToBigQuery(
		gcs_project + ":turnos.visor", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



