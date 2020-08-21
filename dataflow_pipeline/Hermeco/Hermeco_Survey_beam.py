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
	'ID:STRING, '
	'COMPLETADO:STRING, '
	'ULTIMA_PAGINA:STRING, '
	'LENGUAJE_INICIAL:STRING, '
	'FECHA_DE_INICIO:STRING, '
	'FECHA_DE_LA_ULTIMA_ACCION:STRING, '
	'PEDIDO:STRING, '
	'VALOR:STRING, '
	'MEDIO_DE_PAGO:STRING, '
	'MEDIO_DE_CONTACTO:STRING, '
	'TIPO_DE_LLAMADA:STRING, '
	'TIPOLOGIA_IN:STRING, '
	'TIPOLOGIA_OUT:STRING, '
	'VENTA_CRUZADA_EFECTIVA:STRING, '
	'NOMBRE_ASESOR:STRING, '
	'NOMBRE_ASESOR_OTRO:STRING, '
	'HORA_INICIO_CHAT:STRING, '
	'FINALIZACION_CHAT:STRING '



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
				'COMPLETADO' : arrayCSV[1],
				'ULTIMA_PAGINA' : arrayCSV[2],
				'LENGUAJE_INICIAL' : arrayCSV[3],
				'FECHA_DE_INICIO' : arrayCSV[4],
				'FECHA_DE_LA_ULTIMA_ACCION' : arrayCSV[5],
				'PEDIDO' : arrayCSV[6],
				'VALOR' : arrayCSV[7],
				'MEDIO_DE_PAGO' : arrayCSV[8],
				'MEDIO_DE_CONTACTO' : arrayCSV[9],
				'TIPO_DE_LLAMADA' : arrayCSV[10],
				'TIPOLOGIA_IN' : arrayCSV[11],
				'TIPOLOGIA_OUT' : arrayCSV[12],
				'VENTA_CRUZADA_EFECTIVA' : arrayCSV[13],
				'NOMBRE_ASESOR' : arrayCSV[14],
				'NOMBRE_ASESOR_OTRO' : arrayCSV[15],
				'HORA_INICIO_CHAT' : arrayCSV[16],
				'FINALIZACION_CHAT' : arrayCSV[17]



				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-sensus" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Survey Hermeco' >> beam.io.WriteToBigQuery(
		gcs_project + ":Hermeco.Survey", 
		schema=TABLE_SCHEMA, 	
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")