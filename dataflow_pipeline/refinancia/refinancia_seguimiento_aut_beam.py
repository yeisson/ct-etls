# #coding: utf-8 
# from __future__ import print_function, absolute_import

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

#coding: utf-8 

TABLE_SCHEMA = (
	'Id_seguimiento:STRING, '
	'Id_docdeu:STRING, '
	'Id_gestion:STRING, '
    'Id_causal:STRING, '
	'fecha_seguimiento:STRING, '
	'Id_usuario:STRING, '
	'Valor_Saldo_Total:STRING, '
	'numero_contac:STRING, '
	'Nota:STRING'
)

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'Id_seguimiento':arrayCSV[0],
				'Id_docdeu':arrayCSV[1],
				'Id_gestion':arrayCSV[2],
				'Id_causal':arrayCSV[3],
				'fecha_seguimiento':arrayCSV[4],
				'Id_usuario':arrayCSV[5],
				'Valor_Saldo_Total':arrayCSV[6],
				'numero_contac':arrayCSV[7],
				'Nota':arrayCSV[8]
				}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-refinancia" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/seguimiento_aut/Refinancia_Seguimiento_aut" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery refinancia' >> beam.io.WriteToBigQuery(
        gcs_project + ":refinancia.seguimiento_aut",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")



