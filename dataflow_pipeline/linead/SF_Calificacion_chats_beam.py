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
                'TRANSCRIPCION_DE_LIVE_CHAT_FECHA_DE_CREACION:STRING, '
                'TRANSCRIPCION_DE_LIVE_CHAT_NOMBRE_DE_TRANSCRIPCION_DE_CHAT:STRING, '
                'CALIFICACION:STRING, '
                'TRANSCRIPCION_DE_LIVE_CHAT_CREADO_POR:STRING, '
                'TRANSCRIPCION_DE_LIVE_CHAT_NUMERO_DEL_CASO:STRING, '
                'OBSERVACIONES_CLIENTE:STRING, '
                'TRANSCRIPCION_DE_LIVE_CHAT_AGENTE_SAC:STRING '



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
                'TRANSCRIPCION_DE_LIVE_CHAT_FECHA_DE_CREACION' : arrayCSV[0],
                'TRANSCRIPCION_DE_LIVE_CHAT_NOMBRE_DE_TRANSCRIPCION_DE_CHAT' : arrayCSV[1],
                'CALIFICACION' : arrayCSV[2],
                'TRANSCRIPCION_DE_LIVE_CHAT_CREADO_POR' : arrayCSV[3],
                'TRANSCRIPCION_DE_LIVE_CHAT_NUMERO_DEL_CASO' : arrayCSV[4],
                'OBSERVACIONES_CLIENTE' : arrayCSV[5],
                'TRANSCRIPCION_DE_LIVE_CHAT_AGENTE_SAC' : arrayCSV[6]

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
	transformed | 'Escritura a BigQuery SF_Calificacion_chats' >> beam.io.WriteToBigQuery(
		gcs_project + ":linea_directa.SF_Calificacion_chats", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")




