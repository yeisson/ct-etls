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

TABLE_SCHEMA = ('IDKEY:STRING, '
				'FECHA:STRING, '
				'CONSECUTIVO:STRING, '
				'NIT:STRING, '
				'NOMBRES:STRING, '
				'FECHA_DE_PAGO:STRING, '
				'OBLIGACION:STRING, '
				'VALOR_PAGADO:STRING, '
				'CODIGO_ABOGADO:STRING, '
				'NOMBRE_ASESOR:STRING, '
				'FECHA_DE_GRABACION:STRING '
				)

class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla = {'IDKEY' : str(uuid.uuid4()),
				'FECHA': self.mifecha,
				'CONSECUTIVO' : arrayCSV[0],
				'NIT' : arrayCSV[1],
				'NOMBRES' : arrayCSV[2],
				'FECHA_DE_PAGO' : arrayCSV[3],
				'OBLIGACION' : arrayCSV[4],
				'VALOR_PAGADO' : arrayCSV[5],
				'CODIGO_ABOGADO' : arrayCSV[6],
				'NOMBRE_ASESOR' : arrayCSV[7],
				'FECHA_DE_GRABACION' : arrayCSV[8]
				}
		
		return [tupla]

def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia_castigada" #Definicion de la raiz del bucket
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
        # "--num_workers", "30",
        # "--autoscaling_algorithm", "NONE"		
	])
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_castigada.rob_aux_pagos", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()

	return ("Corrio Full HD")
