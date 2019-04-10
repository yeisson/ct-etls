from __future__ import print_function, absolute_import
import logging
import re
import json
import requests
import uuid
import time
import os
import socket
import argparse
import uuid
import datetime
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
			'Consecutivo:STRING,'
			'Nit:STRING,'
			'Nombres:STRING,'
			'Fecha_de_Pago:STRING,'
			'Obligacion:STRING,'
			'Valor_Pagado:STRING,'
			'Codigo_abogado:STRING,'
			'Nombre_Asesor:STRING,'
			'Fecha_de_Grabacion:STRING'
			)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {
				'Consecutivo': arrayCSV[0],
				'Nit': arrayCSV[1],
				'Nombres': arrayCSV[2],
				'Fecha_de_Pago': arrayCSV[3],
				'Obligacion': arrayCSV[4],
				'Valor_Pagado': arrayCSV[5],
				'Codigo_abogado': arrayCSV[6],
				'Nombre_Asesor': arrayCSV[7],
				'Fecha_de_Grabacion': arrayCSV[8]
				}
		
		return [tupla]

############################ CODIGO DE EJECUCION ###################################
def run(table, TABLE_DB):

	gcs_path = 'gs://ct-bridge' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"
	FECHA_CARGUE = str(datetime.date.today())

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "10",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/" + FECHA_CARGUE + "_" + TABLE_DB +".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/" + "REWORK",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Bridge' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contactabilidad."+ table, 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")


################################################################################