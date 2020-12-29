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
                'IDKEY:STRING, '
                'FECHA:STRING, '
                'CHAT_ID:STRING, '
                'MERCHANT_ID:STRING, '
                'PROVIDER:STRING, '
                'CHAT_CREATED_AT:STRING, '
                'MESSAGE_CREATED_AT:STRING, '
                'CONTACT_ID:STRING, '
                'CONTACT_IDENTIFICATION:STRING, '
                'CONTACT_NAME:STRING, '
                'CONTACT_EMAIL:STRING, '
                'CONTACT_PHONE_NUMBER:STRING, '
                'CONTACT_MOBILE_NUMBER:STRING, '
                'AGENT_ID:STRING, '
                'AGENT_NAME:STRING, '
                'AGENT_USERNAME:STRING, '
                'AGENT_EMAIL:STRING, '
                'MESSAGE_INCOMING:STRING, '
                'MESSAGE_CONTACT:STRING, '
                'MESSAGE_AGENT:STRING, '
                'MESSAGE_TYPE:STRING, '
                'MESSAGE_CAPTION:STRING, '
                'ALIAS:STRING, '
                'BOT:STRING, '
                'STATUS:STRING, '
                'OPENED_AT:STRING, '
                'PICKED_UP_AT:STRING, '
                'CLOSED_AT:STRING, '
                'DURATION:STRING '
	            )

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
                'CHAT_ID' : arrayCSV[0],
                'MERCHANT_ID' : arrayCSV[1],
                'PROVIDER' : arrayCSV[2],
                'CHAT_CREATED_AT' : arrayCSV[3],
                'MESSAGE_CREATED_AT' : arrayCSV[4],
                'CONTACT_ID' : arrayCSV[5],
                'CONTACT_IDENTIFICATION' : arrayCSV[6],
                'CONTACT_NAME' : arrayCSV[7],
                'CONTACT_EMAIL' : arrayCSV[8],
                'CONTACT_PHONE_NUMBER' : arrayCSV[9],
                'CONTACT_MOBILE_NUMBER' : arrayCSV[10],
                'AGENT_ID' : arrayCSV[11],
                'AGENT_NAME' : arrayCSV[12],
                'AGENT_USERNAME' : arrayCSV[13],
                'AGENT_EMAIL' : arrayCSV[14],
                'MESSAGE_INCOMING' : arrayCSV[15],
                'MESSAGE_CONTACT' : arrayCSV[16],
                'MESSAGE_AGENT' : arrayCSV[17],
                'MESSAGE_TYPE' : arrayCSV[18],
                'MESSAGE_CAPTION' : arrayCSV[19],
                'ALIAS' : arrayCSV[20],
                'BOT' : arrayCSV[21],
                'STATUS' : arrayCSV[22],
                'OPENED_AT' : arrayCSV[23],
                'PICKED_UP_AT' : arrayCSV[24],
                'CLOSED_AT' : arrayCSV[25],
                'DURATION' : arrayCSV[26]
                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-neurologico" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo,skip_header_lines=1)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery B2chat' >> beam.io.WriteToBigQuery(
		gcs_project + ":Neurologico.B2chat", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")