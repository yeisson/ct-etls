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
                'ID_CLIENTE:STRING, '
                'PRODUCTO:STRING, '
                'SUB_PRODUCTO:STRING, '
                'ID_COLABORADOR:STRING, '
                'NOMBRE_COLABORADOR:STRING, '
                'ID_GRABADOR:STRING, '
                'ID_LIDER:STRING, '
                'NOMBRE_LIDER:STRING, '
                'ID_EJECUTIVO:STRING, '
                'NOMBRE_EJECUTIVO:STRING, '
                'ID_GERENTE:STRING, '
                'NOMBRE_GERENTE:STRING, '
                'META:STRING, '
                'CIUDAD:STRING, '
                'FECHA_ACTUALIZACION:DATE '

            	)

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'ID_CLIENTE' : arrayCSV[0],
                'PRODUCTO' : arrayCSV[1],
                'SUB_PRODUCTO' : arrayCSV[2],
                'ID_COLABORADOR' : arrayCSV[3],
                'NOMBRE_COLABORADOR' : arrayCSV[4],
                'ID_GRABADOR' : arrayCSV[5],
                'ID_LIDER' : arrayCSV[6],
                'NOMBRE_LIDER' : arrayCSV[7],
                'ID_EJECUTIVO' : arrayCSV[8],
                'NOMBRE_EJECUTIVO' : arrayCSV[9],
                'ID_GERENTE' : arrayCSV[10],
                'NOMBRE_GERENTE' : arrayCSV[11],
                'META' : arrayCSV[12],
                'CIUDAD' : arrayCSV[13],
                'FECHA_ACTUALIZACION' : arrayCSV[14]


                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-prueba" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery jerararquias y metas' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contento.Jerarquias_Metas", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")