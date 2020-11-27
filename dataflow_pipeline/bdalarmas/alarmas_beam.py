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
	'Nombre_Guia:STRING, '
    'Id_resultado:STRING, '
    'fecha_registro:STRING, '
    'Doc_Asesor:STRING, '
    'nombres:STRING, '
    'doc_team:STRING, '
    'Nombre_team_leader:STRING, '
    'estado_aseg:STRING, '
    'estado_alarma:STRING, '
    'reagendamiento:STRING, '
    'Descripcion:STRING, '
    'Asignacion:STRING '
  
    

)

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'Nombre_Guia' : arrayCSV[0],
                'Id_resultado' : arrayCSV[1],
                'fecha_registro' : arrayCSV[2],
                'Doc_Asesor' : arrayCSV[3],
                'nombres' : arrayCSV[4],
                'doc_team' : arrayCSV[5],
                'Nombre_team_leader' : arrayCSV[6],
                'estado_aseg' : arrayCSV[7],
                'estado_alarma' : arrayCSV[8],
                'reagendamiento' : arrayCSV[9],
                'Descripcion' : arrayCSV[10],
                'Asignacion' : arrayCSV[11]
               
               

				}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-sensus" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/Segmento/alarmas" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery unificadas' >> beam.io.WriteToBigQuery(
        gcs_project + ":sensus.alarmas",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")