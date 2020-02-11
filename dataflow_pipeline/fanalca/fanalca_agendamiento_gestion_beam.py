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
	'Id_Campaign:STRING, '
	'Name:STRING, '
	'Last_Name:STRING, '
	'Id:STRING, '
	'Date:STRING, '
	'Telephone:STRING, '
	'Result:STRING, '
	'Campana:STRING, '
	'Dia:STRING, '
	'Mes:STRING '
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
				'Id_Campaign' : arrayCSV[0],
				'Name' : arrayCSV[1],
				'Last_Name' : arrayCSV[2],
				'Id' : arrayCSV[3],
				'Date' : arrayCSV[4],
				'Telephone' : arrayCSV[5],
				'Result' : arrayCSV[6],
				'Campana' : arrayCSV[7],
				'Dia' : arrayCSV[8],
				'Mes' : arrayCSV[9]
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

	])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery fanalca' >> beam.io.WriteToBigQuery(
		gcs_project + ":fanalca_agendamiento.gestion", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



