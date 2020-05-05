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
	'CLIENTE:STRING, '
	'NOMBRE_CLIENTE:STRING, '
	'DOCUMENTO:STRING, '
	'T_DCTO:STRING, '
	'F_EXPEDIC:STRING, '
	'F_VENCIM:STRING, '
	'DIASVC:STRING, '
	'DEUDA:STRING, '
	'PAGADO:STRING, '
	'POR_VENC:STRING, '
	'VENC_0_30:STRING, '
	'VENC_31_60:STRING, '
	'VENC_61_90:STRING, '
	'VENC_91:STRING, '
	'SALDO:STRING, '
	'TIPO_CARTERA:STRING, '
	'NOMBRE_TIPO_CARTERA:STRING, '
	'NOMBRE_VENDEDOR:STRING, '
	'CENTRO_COSTOS:STRING, '
	'NOMBRE_CENTRO_DE_COSTOS:STRING '
)

class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha' : self.mifecha,
				'CLIENTE' : arrayCSV[0],
				'NOMBRE_CLIENTE' : arrayCSV[1],
				'DOCUMENTO' : arrayCSV[2],
				'T_DCTO' : arrayCSV[3],
				'F_EXPEDIC' : arrayCSV[4],
				'F_VENCIM' : arrayCSV[5],
				'DIASVC' : arrayCSV[6],
				'DEUDA' : arrayCSV[7],
				'PAGADO' : arrayCSV[8],
				'POR_VENC' : arrayCSV[9],
				'VENC_0_30' : arrayCSV[10],
				'VENC_31_60' : arrayCSV[11],
				'VENC_61_90' : arrayCSV[12],
				'VENC_91' : arrayCSV[13],
				'SALDO' : arrayCSV[14],
				'TIPO_CARTERA' : arrayCSV[15],
				'NOMBRE_TIPO_CARTERA' : arrayCSV[16],
				'NOMBRE_VENDEDOR' : arrayCSV[17],
				'CENTRO_COSTOS' : arrayCSV[18],
				'NOMBRE_CENTRO_DE_COSTOS' : arrayCSV[19]
				}
		
		return [tupla]

def run(archivo, mifecha):

	gcs_path = "gs://ct-tech-tof"
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
	])
	
	lines = pipeline | 'Lectura de Archivo PFC' >> ReadFromText(archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data PFC' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery PFC' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contento_Tech.profitto_bd_carteras", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	return ("Corrio Full HD")



