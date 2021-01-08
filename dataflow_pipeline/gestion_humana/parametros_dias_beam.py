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
                'EPS:STRING, '
                'URL:STRING, '
                'USER_EPS:STRING, '
                'PASS:STRING, '
                'CORREO_NOTIFICACIONES:STRING, '
                'ESTADO:STRING, '
                'TIEMPO_TRANSCRIPCION_EMPRESA:STRING, '
                'TIEMPO_COBRO_EMPRESA:STRING, '
                'TIEMPO_TRANSCRIPCION_EPS:STRING, '
                'TIEMPO_PAGO_EPS:STRING, '
                'INSERT_DATE:STRING '




                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'EPS' : arrayCSV[0],
                'URL' : arrayCSV[1],
                'USER_EPS' : arrayCSV[2],
                'PASS' : arrayCSV[3],
                'CORREO_NOTIFICACIONES' : arrayCSV[4],
                'ESTADO' : arrayCSV[5],
                'TIEMPO_TRANSCRIPCION_EMPRESA' : arrayCSV[6],
                'TIEMPO_COBRO_EMPRESA' : arrayCSV[7],
                'TIEMPO_TRANSCRIPCION_EPS' : arrayCSV[8],
                'TIEMPO_PAGO_EPS' : arrayCSV[9],
                'INSERT_DATE' : arrayCSV[10]


				}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-gto" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/dias" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery Parametros_dias' >> beam.io.WriteToBigQuery(
        gcs_project + ":gestion_humana.Parametros_dias",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run();jobObject.wait_until_finish()

    
    # jobID = jobObject.job_id()

	return ("Corrio sin problema")
