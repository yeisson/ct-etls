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
	'TRANSACCION:STRING, '
	'DOCUMENTO:STRING, '
	'NOMBRE_CAMPANA:STRING, '
	'GESTION:STRING, '
	'CAUSAL:STRING, '
	'SUBCAUSAL:STRING, '
	'SUBCAUSAL1:STRING, '
	'SUBCAUSAL2:STRING, '
	'OBSERVACION:STRING, '
	'COMPLETADO:STRING, '
	'ULT_PAGINA:STRING, '
	'LENGUAJE:STRING, '
	'AGENTE:STRING, '
	'FECHA_INGRESO:STRING, '
	'HORA_INGRESO:STRING, '
	'PREGUNTA:STRING, '
	'FECHA_RESPUESTA:STRING, '
	'HORA_RESPUESTA:STRING, '
	'FECHA_GESTION:STRING, '
	'NEGOCIADOR:STRING, '
	'ID_GESTION:STRING '



)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'TRANSACCION' : arrayCSV[0],
				'DOCUMENTO' : arrayCSV[1],
				'NOMBRE_CAMPANA' : arrayCSV[2],
				'GESTION' : arrayCSV[3],
				'CAUSAL' : arrayCSV[4],
				'SUBCAUSAL' : arrayCSV[5],
				'SUBCAUSAL1' : arrayCSV[6],
				'SUBCAUSAL2' : arrayCSV[7],
				'OBSERVACION' : arrayCSV[8],
				'COMPLETADO' : arrayCSV[9],
				'ULT_PAGINA' : arrayCSV[10],
				'LENGUAJE' : arrayCSV[11],
				'AGENTE' : arrayCSV[12],
				'FECHA_INGRESO' : arrayCSV[13],
				'HORA_INGRESO' : arrayCSV[14],
				'PREGUNTA' : arrayCSV[15],
				'FECHA_RESPUESTA' : arrayCSV[16],
				'HORA_RESPUESTA' : arrayCSV[17],
				'FECHA_GESTION' : arrayCSV[18],
				'NEGOCIADOR' : arrayCSV[19],
				'ID_GESTION' : arrayCSV[20]






				}
		return [tupla]

############################ CODIGO DE EJECUCION ###################################

def run(output,KEY_REPORT):

	gcs_path = 'gs://ct-auteco' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

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

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(output)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	
	transformed | 'Escritura a BigQuery Tipificacion' >> beam.io.WriteToBigQuery(
		gcs_project + ":Auteco_Mobility." + "tipificador", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")