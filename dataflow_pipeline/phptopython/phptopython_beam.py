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

#coding: utf-8 

TABLE_SCHEMA = (
	'id_cliente:STRING,'
	'producto:STRING,'
	'sub_producto:STRING,'
	'id_colaborador:STRING,'
	'nombre_colaborador:STRING,'
	'id_lider:STRING,'
	'nombre_lider:STRING,'
	'id_ejecutivo:STRING,'
	'nombre_ejecutivo:STRING,'
	'id_gerente:STRING,'
	'nombre_gerente:STRING,'
	'METAxHora:STRING,'
	'estado:STRING,'
	'Ciudad:STRING'
)
# ?
class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'id_cliente': arrayCSV[0],
				'producto': arrayCSV[1],
				'sub_producto': arrayCSV[2],
				'id_colaborador': arrayCSV[3],
				'nombre_colaborador': arrayCSV[4],
				'id_lider': arrayCSV[5],
				'nombre_lider': arrayCSV[6],
				'id_ejecutivo': arrayCSV[7],
				'nombre_ejecutivo': arrayCSV[8],
				'id_gerente': arrayCSV[9],
				'nombre_gerente': arrayCSV[10],
				'METAxHORA': arrayCSV[11],
				'estado': arrayCSV[12],
				'Ciudad': arrayCSV[13]
				}
		
		return [tupla]



def run(archivo):

	gcs_path = "gs://ct-bridge" #Definicion de la raiz del bucket
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
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	transformed | 'Escritura a BigQuery Jerarquia_Metas' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contento.Jerarquias_Metas", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("El proceso de cargue a bigquery fue ejecutado con exito")



