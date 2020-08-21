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
        'EVENT_DATE:STRING, '
        'CAMPAIGN_NAME:STRING, '
        'PROGRAM:STRING, '
        'EMAIL:STRING, '
        'CODE:STRING, '
        'NUMBER:STRING, '
        'TYPE:STRING, '
        'UNIQUE_OPEN:STRING, '
        'UNIQUE_ACTION:STRING, '
        'CONVERSION:STRING, '
        'KEYWORD_TYPE:STRING, '
        'COUNTRY:STRING, '
        'MESSAGE:STRING, '
        'STATUS:STRING, '
        'PARTS:STRING, '
        'COST:STRING, '
        'CAMPAIGN_ID:STRING, '
        'EVENT_ID:STRING, '
        'NETWORK:STRING, '
        'LANDING_ID:STRING, '
        'FULL_SHORTLINK:STRING, '
        'SHORTLINK_CODE:STRING, '
        'COMPONENT_NAME:STRING, '
        'SHORTLINK_COST:STRING, '
        'TOTAL_COST:STRING, '
        'IN_OUT:STRING, '
        'SHORTVIEW:STRING, '
        'OPEN_NO_ACTION:STRING, '
        'MENSAJE:STRING, '
        'MOBILE_NUMBER:STRING, '
        'DRIPSUNITNAME:STRING '

	)

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
			'campana' : self.mifecha,
                        'EVENT_DATE' : arrayCSV[0],
                        'CAMPAIGN_NAME' : arrayCSV[1],
                        'PROGRAM' : arrayCSV[2],
                        'EMAIL' : arrayCSV[3],
                        'CODE' : arrayCSV[4],
                        'NUMBER' : arrayCSV[5],
                        'TYPE' : arrayCSV[6],
                        'UNIQUE_OPEN' : arrayCSV[7],
                        'UNIQUE_ACTION' : arrayCSV[8],
                        'CONVERSION' : arrayCSV[9],
                        'KEYWORD_TYPE' : arrayCSV[10],
                        'COUNTRY' : arrayCSV[11],
                        'MESSAGE' : arrayCSV[12],
                        'STATUS' : arrayCSV[13],
                        'PARTS' : arrayCSV[14],
                        'COST' : arrayCSV[15],
                        'CAMPAIGN_ID' : arrayCSV[16],
                        'EVENT_ID' : arrayCSV[17],
                        'NETWORK' : arrayCSV[18],
                        'LANDING_ID' : arrayCSV[19],
                        'FULL_SHORTLINK' : arrayCSV[20],
                        'SHORTLINK_CODE' : arrayCSV[21],
                        'COMPONENT_NAME' : arrayCSV[22],
                        'SHORTLINK_COST' : arrayCSV[23],
                        'TOTAL_COST' : arrayCSV[24],
                        'IN_OUT' : arrayCSV[25],
                        'SHORTVIEW' : arrayCSV[26],
                        'OPEN_NO_ACTION' : arrayCSV[27],
                        'MENSAJE' : arrayCSV[28],
                        'MOBILE_NUMBER' : arrayCSV[29],
                        'DRIPSUNITNAME' : arrayCSV[30],


                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-ucc" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery ucc' >> beam.io.WriteToBigQuery(
		gcs_project + ":ucc.SMS_V3", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")