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
                'NOMBRE_DE_LA_CUENTA:STRING, '
                'PROPIETARIO_DEL_CASO:STRING, '
                'NUMERO_DEL_CASO:STRING, '
                'ESTADO:STRING, '
                'TIPIFICACION_NIVEL_1:STRING, '
                'TIPIFICACION_NIVEL_2:STRING, '
                'ORIGEN_DEL_CASO:STRING, '
                'HORARIO:STRING, '
                'FECHA_HORA_DE_APERTURA:STRING, '
                'ABIERTO:STRING, '
                'CERRADO:STRING, '
                'HORA:STRING '


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
                'NOMBRE_DE_LA_CUENTA' : arrayCSV[0],
                'PROPIETARIO_DEL_CASO' : arrayCSV[1],
                'NUMERO_DEL_CASO' : arrayCSV[2],
                'ESTADO' : arrayCSV[3],
                'TIPIFICACION_NIVEL_1' : arrayCSV[4],
                'TIPIFICACION_NIVEL_2' : arrayCSV[5],
                'ORIGEN_DEL_CASO' : arrayCSV[6],
                'HORARIO' : arrayCSV[7],
                'FECHA_HORA_DE_APERTURA' : arrayCSV[8],
                'ABIERTO' : arrayCSV[9],
                'CERRADO' : arrayCSV[10],
                'HORA' : arrayCSV[11]
                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-linead" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery SF_Chats_por_hora' >> beam.io.WriteToBigQuery(
		gcs_project + ":linea_directa.SF_Chats_por_hora", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")