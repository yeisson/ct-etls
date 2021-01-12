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
                'MAD_MAIL_DOCUMENTOS:STRING, '
                'MAD_ID_GESTION:STRING, '
                'MAD_CORREO_RESPONSBLE:STRING, '
                'MAD_NOMBRE_COMPLETO:STRING, '
                'MAD_DOCUMENTO:STRING, '
                'MAD_FECHA_REAL_INICIAL:STRING, '
                'MAD_DOCUMENTO_PENDIENTE:STRING, '
                'MAD_INSERT_DATE:STRING '



                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'MAD_MAIL_DOCUMENTOS' : arrayCSV[0],
                'MAD_ID_GESTION' : arrayCSV[1],
                'MAD_CORREO_RESPONSBLE' : arrayCSV[2],
                'MAD_NOMBRE_COMPLETO' : arrayCSV[3],
                'MAD_DOCUMENTO' : arrayCSV[4],
                'MAD_FECHA_REAL_INICIAL' : arrayCSV[5],
                'MAD_DOCUMENTO_PENDIENTE' : arrayCSV[6],
                'MAD_INSERT_DATE' : arrayCSV[7]

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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/d_pendientes" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery D_pendientes' >> beam.io.WriteToBigQuery(
        gcs_project + ":gestion_humana.D_pendientes",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run();jobObject.wait_until_finish()

    
    # jobID = jobObject.job_id()

	return ("Corrio sin problema")
