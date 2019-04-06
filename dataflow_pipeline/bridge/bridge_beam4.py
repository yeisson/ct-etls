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

####################### PARAMETROS DE LA TABLA EN BQ ##########################

TABLE_SCHEMA = (
			'Consecutivo_Documento_Deudor:STRING,'
			'Nit:STRING,'
			'Fecha_ultima_Gestion_Prejuridica:STRING,'
			'Desc_Ultimo_Codigo_de_Gestion_Prejuridico:STRING'
			)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {
				'Consecutivo_Documento_Deudor': arrayCSV[0],
				'Nit': arrayCSV[1],
				'Fecha_ultima_Gestion_Prejuridica': arrayCSV[2],
				'Desc_Ultimo_Codigo_de_Gestion_Prejuridico': arrayCSV[3]
				}
		
		return [tupla]

############################ CODIGO DE EJECUCION ###################################
def run(table):

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
        "--max_num_workers", "5",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/" + FECHA_CARGUE + "_4" +".csv")
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