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
	'PROVEEDOR:STRING, '
	'NOMBRE_PROVEEDOR:STRING, '
	'T_DCTO:STRING, '
	'DOCUMENTO:STRING, '
	'DCTO_PRV:STRING, '
	'FECHA_DCTO:STRING, '
	'FECHA_VENCIM:STRING, '
	'DIASVC:STRING, '
	'DEUDA:STRING, '
	'PAGADO:STRING, '
	'POR_VENC:STRING, '
	'VENC_1_30:STRING, '
	'VENC_31_60:STRING, '
	'VENC_61_90:STRING, '
	'VENC_91:STRING, '
	'SALDO:STRING, '
	'TIPO_CXP:STRING, '
	'NOMBRE_CXP:STRING, '
	'TIPO_PRV:STRING, '
	'NOMBRE_TIPO_PROVEEDOR:STRING, '
	'CUENTAXPAGAR:STRING, '
	'TIPO_INFORME:STRING '
)

class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha' : self.mifecha,
				'PROVEEDOR' : arrayCSV[0],
				'NOMBRE_PROVEEDOR' : arrayCSV[1],
				'T_DCTO' : arrayCSV[2],
				'DOCUMENTO' : arrayCSV[3],
				'DCTO_PRV' : arrayCSV[4],
				'FECHA_DCTO' : arrayCSV[5],
				'FECHA_VENCIM' : arrayCSV[6],
				'DIASVC' : arrayCSV[7],
				'DEUDA' : arrayCSV[8],
				'PAGADO' : arrayCSV[9],
				'POR_VENC' : arrayCSV[10],
				'VENC_1_30' : arrayCSV[11],
				'VENC_31_60' : arrayCSV[12],
				'VENC_61_90' : arrayCSV[13],
				'VENC_91' : arrayCSV[14],
				'SALDO' : arrayCSV[15],
				'TIPO_CXP' : arrayCSV[16],
				'NOMBRE_CXP' : arrayCSV[17],
				'TIPO_PRV' : arrayCSV[18],
				'NOMBRE_TIPO_PROVEEDOR' : arrayCSV[19],
				'CUENTAXPAGAR' : arrayCSV[20],
				'TIPO_INFORME' : arrayCSV[21]
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
	
	lines = pipeline | 'Lectura de Archivo PFcxp' >> ReadFromText(archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data PFcxp' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery PFcxp' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contento_Tech.profitto_CuentasxPagar", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	return ("Corrio Full HD")



