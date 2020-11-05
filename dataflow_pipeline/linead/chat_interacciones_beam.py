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

TABLE_SCHEMA = ('idkey:STRING, '
		        'fecha:STRING, '
                'CHANNEL:STRING, '
                'AGENT:STRING, '
                'SKILL:STRING, '
                'ID_CASE:STRING, '
                'SUBJECT:STRING, '
                'DE_QUIEN:STRING, '
                'DE_DONDE:STRING, '
                'BODY:STRING, '
                'ANSWER:STRING, '
                'DATE_OF_CREATION:STRING, '
                'CLOSING_DATE:STRING, '
                'CLOSING_TIME:STRING, '
                'EVALUATION_SURVEY:STRING, '
                'CODE:STRING, '
                'COMMENTS:STRING '

                )

class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha': self.mifecha,
                'CHANNEL' : arrayCSV[0],
                'AGENT' : arrayCSV[1],
                'SKILL' : arrayCSV[2],
                'ID_CASE' : arrayCSV[3],
                'SUBJECT' : arrayCSV[4],
                'DE_QUIEN' : arrayCSV[5],
                'DE_DONDE' : arrayCSV[6],
                'BODY' : arrayCSV[7],
                'ANSWER' : arrayCSV[8],
                'DATE_OF_CREATION' : arrayCSV[9],
                'CLOSING_DATE' : arrayCSV[10],
                'CLOSING_TIME' : arrayCSV[11],
                'EVALUATION_SURVEY' : arrayCSV[12],
                'CODE' : arrayCSV[13],
                'COMMENTS' : arrayCSV[14]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-telefonia" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery telefonia' >> beam.io.WriteToBigQuery(
		gcs_project + ":telefonia.Chat_interacciones", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)


	jobObject = pipeline.run()
	return ("Corrio Full HD")



