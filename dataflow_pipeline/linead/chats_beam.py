#coding: utf-8 
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

TABLE_SCHEMA = ('fecha:STRING, '
                'NOMBRE_DE_LA_CUENTA:STRING, '
                'CUENTA_PERSONAL_MOVIL:STRING, '
                'CONTACTO_CORREO_ELECTRONICO:STRING, '
                'PROPIETARIO_DEL_CASO:STRING, '
                'NUMERO_DEL_CASO:STRING, '
                'FECHA_HORA_DE_APERTURA:STRING, '
                'ORIGEN_DEL_CASO:STRING, '
                'TIPO:STRING, '
                'PROCESO:STRING, '
                'TIPIFICACION_NIVEL_1:STRING, '
                'TIPIFICACION_NIVEL_2:STRING, '
                'ESTADO:STRING, '
                'ABIERTO:STRING, '
                'CERRADO:STRING, '
                'ULTIMA_FECHA_MODIFICACION:STRING, '
                'AGENTE_SAC:STRING '

                )

class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'fecha': self.mifecha,
                'NOMBRE_DE_LA_CUENTA' : arrayCSV[0],
                'CUENTA_PERSONAL_MOVIL' : arrayCSV[1],
                'CONTACTO_CORREO_ELECTRONICO' : arrayCSV[2],
                'PROPIETARIO_DEL_CASO' : arrayCSV[3],
                'NUMERO_DEL_CASO' : arrayCSV[4],
                'FECHA_HORA_DE_APERTURA' : arrayCSV[5],
                'ORIGEN_DEL_CASO' : arrayCSV[6],
                'TIPO' : arrayCSV[7],
                'PROCESO' : arrayCSV[8],
                'TIPIFICACION_NIVEL_1' : arrayCSV[9],
                'TIPIFICACION_NIVEL_2' : arrayCSV[10],
                'ESTADO' : arrayCSV[11],
                'ABIERTO' : arrayCSV[12],
                'CERRADO' : arrayCSV[13],
                'ULTIMA_FECHA_MODIFICACION' : arrayCSV[14],
                'AGENTE_SAC' : arrayCSV[15]
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery linea_directa.Chats' >> beam.io.WriteToBigQuery(
		gcs_project + ":linea_directa.Chats", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)


	jobObject = pipeline.run()
	return ("Corrio Full HD")



