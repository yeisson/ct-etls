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
	'ANO:STRING, '
	'PERIODO:STRING, '
	'NOMBRE_PRODUCTO_MVTO:STRING, '
	'PRODUCTO:STRING, '
	'DOCUMENTO:STRING, '
	'T_DCTO:STRING, '
	'CLIENTE:STRING, '
	'NOMBRE_CLIENTE:STRING, '
	'FECHA_DCTO:STRING, '
	'CENTRO_COSTOS:STRING, '
	'NOMBRE_CENTRO_COSTOS:STRING, '
	'TOTAL_PROD:STRING, '
	'TIPO_CLIENTE:STRING, '
	'CLIENTE_AGRUPADO:STRING, '
	'CUENTA:STRING, '
	'TIPO:STRING '
)

class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha' : self.mifecha,
				'ANO' : arrayCSV[0],
				'PERIODO' : arrayCSV[1],
				'NOMBRE_PRODUCTO_MVTO' : arrayCSV[2],
				'PRODUCTO' : arrayCSV[3],
				'DOCUMENTO' : arrayCSV[4],
				'T_DCTO' : arrayCSV[5],
				'CLIENTE' : arrayCSV[6],
				'NOMBRE_CLIENTE' : arrayCSV[7],
				'FECHA_DCTO' : arrayCSV[8],
				'CENTRO_COSTOS' : arrayCSV[9],
				'NOMBRE_CENTRO_COSTOS' : arrayCSV[10],
				'TOTAL_PROD' : arrayCSV[11],
				'TIPO_CLIENTE' : arrayCSV[12],
				'CLIENTE_AGRUPADO' : arrayCSV[13],
				'CUENTA' : arrayCSV[14],
				'TIPO' : arrayCSV[15]
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
	
	lines = pipeline | 'Lectura de Archivo PFF' >> ReadFromText(archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data PFF' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery PFF' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contento_Tech.profitto_bd_factura", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	return ("Corrio Full HD")



