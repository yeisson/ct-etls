#encoding: utf-8 
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

TABLE_SCHEMA = ('FECHA_CARGUE:STRING, '
                'CAMPANA:STRING, '
                'TARIFA_POLIZA:STRING, '
                'META_CONTACTABILIDAD:STRING, '
                'META_EFECTIVIDAD:STRING, '
                'META_AHT_VENTAS__SEGUNDOS_:INTEGER, '
                'META_AHT_NO_EFECTIVOS__SEGUNDOS_:INTEGER, '
                'META_INTENTOS_REGISTRO:INTEGER, '
                'META_VENTAS_MES:INTEGER, '
                'FECHA_METAS:DATE '



	        )

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'FECHA_CARGUE' : self.mifecha,
                'CAMPANA' : arrayCSV[0],
                'TARIFA_POLIZA' : arrayCSV[1],
                'META_CONTACTABILIDAD' : arrayCSV[2],
                'META_EFECTIVIDAD' : arrayCSV[3],
                'META_AHT_VENTAS__SEGUNDOS_' : arrayCSV[4],
                'META_AHT_NO_EFECTIVOS__SEGUNDOS_' : arrayCSV[5],
                'META_INTENTOS_REGISTRO' : arrayCSV[6],
                'META_VENTAS_MES' : arrayCSV[7],
                'FECHA_METAS' : arrayCSV[8]


                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-liberty" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery contento.metas_liberty' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contento.metas_liberty", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")