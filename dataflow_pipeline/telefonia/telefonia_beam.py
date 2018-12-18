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
	'date: STRING,'
	'agent: STRING,'
	'identification: STRING,'
	'login_date: STRING,'
	'logout_date: STRING,'
	'login_time: STRING,'
	'ipdial_code: STRING,'
	'id_cliente: STRING,'
	'cartera: STRING'
)
# ?
class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split(',')
		arrayCSV = element.split('|')
		tupla= {
				'date': arrayCSV[0],
				'agent': arrayCSV[1],
				'identification': arrayCSV[2],
				'login_date': arrayCSV[3],
				'logout_date': arrayCSV[4],
				'login_time': arrayCSV[5],
				'ipdial_code': arrayCSV[6],
				'id_cliente': arrayCSV[7],
				'cartera': arrayCSV[8]
				}
		return [tupla]


def run(data):
	
	gcs_project = "contento-bi"

	pipeline =  beam.Pipeline(runner="DirectRunner")
	lines = pipeline | 'Lectura de Archivo en memoria' >> beam.Create([data])
	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Telefonia/data2",file_name_suffix='.txt',shard_name_template='')
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	transformed | 'Escribir en Archivo3' >> WriteToText("archivos/Telefonia/data3",file_name_suffix='.txt',shard_name_template='')
	# transformed | 'Escritura a BigQuery telefonia' >> beam.io.WriteToBigQuery(
	# gcs_project + ":telefonia.ipdial_login_out", 
	# schema=TABLE_SCHEMA, 
	# create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
	# write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
	# )

	jobObject = pipeline.run()
	return ("Llegaste, te mire de frente... despues puse un nombre; te llame ternura")