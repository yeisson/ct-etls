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
    'IDKEY:STRING, '
    'FECHA:STRING, '
    'GESTOR:STRING, '
    'IDENTIFICACION:STRING, '
    'NOMBRES:STRING, '
    'CLASE_DE_GESTION:STRING, '
    'TELEFONO:STRING, '
    'CONSECUTIVO_OBLIGACION:STRING, '
    'RESPONSABLE_DE_COBRO:STRING, '
    'CODIGO_DE_COBRO_ANTERIOR:STRING, '
    'FECHA_VENCIMIENTO:STRING, '
    'FECHA_GESTION:STRING, '
    'DURACION:STRING, '
    'HORA_INICIO_DE_GRABACION:STRING, '
    'HORA_FIN_DE_GRABACION:STRING, '
    'NOTA:STRING, '
    'CUOTAS_VENCIDAS:STRING, '
    'CODIGO_GESTION:STRING '




	)

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
                'GESTOR' : arrayCSV[0],
                'IDENTIFICACION' : arrayCSV[1],
                'NOMBRES' : arrayCSV[2],
                'CLASE_DE_GESTION' : arrayCSV[3],
                'TELEFONO' : arrayCSV[4],
                'CONSECUTIVO_OBLIGACION' : arrayCSV[5],
                'RESPONSABLE_DE_COBRO' : arrayCSV[6],
                'CODIGO_DE_COBRO_ANTERIOR' : arrayCSV[7],
                'FECHA_VENCIMIENTO' : arrayCSV[8],
                'FECHA_GESTION' : arrayCSV[9],
                'DURACION' : arrayCSV[10],
                'HORA_INICIO_DE_GRABACION' : arrayCSV[11],
                'HORA_FIN_DE_GRABACION' : arrayCSV[12],
                'NOTA' : arrayCSV[13],
                'CUOTAS_VENCIDAS' : arrayCSV[14],
                'CODIGO_GESTION' : arrayCSV[15]

                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-proteccion" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery Gestiones_adm' >> beam.io.WriteToBigQuery(
		gcs_project + ":proteccion.Gestiones_adm", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")