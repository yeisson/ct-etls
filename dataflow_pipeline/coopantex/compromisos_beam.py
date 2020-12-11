from __future__ import print_function, absolute_import
import logging
import re
import json
import requests
import uuid
import time
from datetime import date
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



TABLE_SCHEMA = ('FECHA_CARGUE:STRING, '
                'ESTADO_ADMINFO:STRING, '
                'DESCRIPCION_RESULTADO:STRING, '
                'FECHA_COMPROMISO:DATE, '
                'FECHA_GRABACION:DATE, '
                'VALOR_PACTADO:INTEGER, '
                'GESTOR:STRING, '
                'CASA_DE_COBRO:STRING, '
                'DESCRIPCION_CODIGO_DE_GESTION:STRING, '
                'NIT:STRING, '
                'NRO_OBLIGACION:STRING, '
                'CODIGO_CASA_DE_COBRO:STRING, '
                'VALOR_RECUPERADO:INTEGER '


	        )

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'FECHA_CARGUE' : self.mifecha,
                'ESTADO_ADMINFO' : arrayCSV[0],
                'DESCRIPCION_RESULTADO' : arrayCSV[1],
                'FECHA_COMPROMISO' : arrayCSV[2],
                'FECHA_GRABACION' : arrayCSV[3],
                'VALOR_PACTADO' : arrayCSV[4],
                'GESTOR' : arrayCSV[5],
                'CASA_DE_COBRO' : arrayCSV[6],
                'DESCRIPCION_CODIGO_DE_GESTION' : arrayCSV[7],
                'NIT' : arrayCSV[8],
                'NRO_OBLIGACION' : arrayCSV[9],
                'CODIGO_CASA_DE_COBRO' : arrayCSV[10],
                'VALOR_RECUPERADO' : arrayCSV[11]

                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-unificadas" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery unificadas.Compromisos_coopantex' >> beam.io.WriteToBigQuery(
		gcs_project + ":unificadas.Compromisos_coopantex", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")