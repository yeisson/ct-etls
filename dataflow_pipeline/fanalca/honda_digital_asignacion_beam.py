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
	'idkey:STRING,'
	'fecha_cargue:STRING,'
	'Fecha_Recepcion_Base:STRING,'
	'Nombre_Base:STRING,'
	'Tipobase:STRING,'
	'Fecha_Desc:STRING,'
	'Argue:STRING,'
	'Mes:STRING,'
	'Ano:STRING,'
	'Id:STRING,'
	'Nombre:STRING,'
	'Apellido:STRING,'
	'Nombre_Completo:STRING,'
	'Cedula:STRING,'
	'Email:STRING,'
	'Departamento:STRING,'
	'Ciudad:STRING,'
	'Telefono:STRING,'
	'Responsable:STRING,'
	'Moto:STRING'
)


class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),				
				'fecha_cargue' : self.mifecha,
				'Fecha_Recepcion_Base': arrayCSV[0],
				'Nombre_Base': arrayCSV[1],
				'Tipobase': arrayCSV[2],
				'Fecha_Desc': arrayCSV[3],
				'Argue': arrayCSV[4],
				'Mes': arrayCSV[5],
				'Ano': arrayCSV[6],
				'Id': arrayCSV[7],
				'Nombre': arrayCSV[8],
				'Apellido': arrayCSV[9],
				'Nombre_Completo': arrayCSV[10],
				'Cedula': arrayCSV[11],
				'Email': arrayCSV[12],
				'Departamento': arrayCSV[13],
				'Ciudad': arrayCSV[14],
				'Telefono': arrayCSV[15],
				'Responsable': arrayCSV[16],
				'Moto': arrayCSV[17]
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

	])
	lines = pipeline | 'Lectura de Archivo HONDA-DIGITAL' >> ReadFromText(archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data HONDA-DIGITAL' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery fanalca HONDA-DIGITAL' >> beam.io.WriteToBigQuery(
		gcs_project + ":fanalca.asignacion_digital", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



