# -*- coding: utf-8 -*-
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
			'idkey:STRING,'
			'Fecha_Cargue:STRING,'
			'Id_Agenda:STRING,'
			'Chasis:STRING,'
			'Agencia:STRING,'
			'Nombre_Agencia:STRING,'
			'Ciudad:STRING,'
			'Concesionario:STRING,'
			'Fecha_Inicio:STRING,'
			'Fecha_Final:STRING,'
			'Estado:STRING,'
			'Gestion:STRING,'
			'Causal:STRING,'
			'Subcausal:STRING,'
			'Queja:STRING,'
			'Nota_Queja:STRING,'
			'Asesor:STRING,'
			'Fecha_Gestion:STRING,'
			'Cumplimiento:STRING,'
			'Tecnico:STRING'

)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {
				'idkey': str(uuid.uuid4()),
				'Fecha_Cargue': datetime.datetime.today().strftime('%Y-%m-%d'),
				'Id_Agenda': arrayCSV[0],
				'Chasis': arrayCSV[1],
				'Agencia': arrayCSV[2],
				'Nombre_Agencia': arrayCSV[3],
				'Ciudad': arrayCSV[4],
				'Concesionario': arrayCSV[5],
				'Fecha_Inicio': arrayCSV[6],
				'Fecha_Final': arrayCSV[7],
				'Estado': arrayCSV[8],
				'Gestion': arrayCSV[9],
				'Causal': arrayCSV[10],
				'Subcausal': arrayCSV[11],
				'Queja': arrayCSV[12],
				'Nota_Queja': arrayCSV[13],
				'Asesor': arrayCSV[14],
				'Fecha_Gestion': arrayCSV[15],
				'Cumplimiento': arrayCSV[16],
				'Tecnico': arrayCSV[17]

				}
		
		return [tupla]

############################ CODIGO DE EJECUCION ###################################
def run(filename):

	gcs_path = 'gs://ct-tech-tof' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"
	FECHA_CARGUE = str(datetime.date.today())

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "15",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'FANALCA_TECH Lectura de Archivo' >> ReadFromText(gcs_path + '/' + filename)
	transformed = (lines | 'FANALCA_TECH Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/" + "REWORK",file_name_suffix='.csv',shard_name_template='')

	transformed | 'FANALCA_TECH Escritura a BigQuery Bridge' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contento_Tech.Consolidado_TOF", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("R!")


################################################################################