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
	'Fecha_Recepcion:STRING, '
	'Base:STRING, '
	'Orden:STRING, '
	'Ultimo_Servicio:STRING, '
	'Fecha_ultimo_servicio:STRING, '
	'Proximo_Servicio:STRING, '
	'Fecha_Estimada:STRING, '
	'Placa:STRING, '
	'No_Chasis:STRING, '
	'Cedula:STRING, '
	'Cliente:STRING, '
	'Telefono_1:STRING, '
	'Telefono_2:STRING, '
	'Denominacion_de_objeto_tecnico:STRING, '
	'Kilometraje:STRING, '
	'Concesionario:STRING, '
	'Agencia:STRING, '
	'EMAIl:STRING, '
	'Fecha_de_Venta:STRING'
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
				'fecha' : self.mifecha,
				'Fecha_Recepcion': arrayCSV[0],
				'Base': arrayCSV[1],
				'Orden': arrayCSV[2],
				'Ultimo_Servicio': arrayCSV[3],
				'Fecha_ultimo_servicio': arrayCSV[4],
				'Proximo_Servicio': arrayCSV[5],
				'Fecha_Estimada': arrayCSV[6],
				'Placa': arrayCSV[7],
				'No_Chasis': arrayCSV[8],
				'Cedula': arrayCSV[9],
				'Cliente': arrayCSV[10],
				'Telefono_1': arrayCSV[11],
				'Telefono_2': arrayCSV[12],
				'Denominacion_de_objeto_tecnico': arrayCSV[13],
				'Kilometraje': arrayCSV[14],
				'Concesionario': arrayCSV[15],
				'Agencia': arrayCSV[16],
				'EMAIl': arrayCSV[17],
				'Fecha_de_Venta': arrayCSV[18]
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

	transformed | 'Escritura a BigQuery fanalca' >> beam.io.WriteToBigQuery(
		gcs_project + ":fanalca_agendamiento.asignacion", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



