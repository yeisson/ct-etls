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
        	'idkey:STRING,'
	        'archivo:STRING,'
                'CC:STRING, '
                'NOMBRE_NEGOCIADOR:STRING, '
                'LIDER:STRING, '
                'EJECUTIVO:STRING, '
                'PRODUCTO:STRING, '
                'NOTA:STRING, '
                'TIEMPO:STRING, '
                'FECHA:STRING, '
                'NOVEDAD:STRING, '
                'MES_CERTIFICACION:STRING, '
                'FORMADOR:STRING '
            	)

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),				
			'archivo' : self.mifecha,
                        'CC' : arrayCSV[0],
                        'NOMBRE_NEGOCIADOR' : arrayCSV[1],
                        'LIDER' : arrayCSV[2],
                        'EJECUTIVO' : arrayCSV[3],
                        'PRODUCTO' : arrayCSV[4],
                        'NOTA' : arrayCSV[5],
                        'TIEMPO' : arrayCSV[6],
                        'FECHA' : arrayCSV[7],
                        'NOVEDAD' : arrayCSV[8],
                        'MES_CERTIFICACION' : arrayCSV[9],
                        'FORMADOR' : arrayCSV[10]
                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-formacion" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery Formacion' >> beam.io.WriteToBigQuery(
		gcs_project + ":Formacion.Certificaciones", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")