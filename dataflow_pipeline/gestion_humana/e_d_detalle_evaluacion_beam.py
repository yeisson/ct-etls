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
                'ID_EVALUACION_DETALLE:STRING, '
                'NOTAXPREGUNTA:STRING, '
                'ESTADO:STRING, '
                'FECHA_MODIF:STRING, '
                'USUARIO_MODIF:STRING, '
                'ID_RELCOMPETENCIA:STRING, '
                'PESOXCOMPETENCIA:STRING, '
                'RESPUESTAABIERTA:STRING, '
                'ID_EVALUACION_HISTORICO:STRING '



                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'ID_EVALUACION_DETALLE' : arrayCSV[0],
                'NOTAXPREGUNTA' : arrayCSV[1],
                'ESTADO' : arrayCSV[2],
                'FECHA_MODIF' : arrayCSV[3],
                'USUARIO_MODIF' : arrayCSV[4],
                'ID_RELCOMPETENCIA' : arrayCSV[5],
                'PESOXCOMPETENCIA' : arrayCSV[6],
                'RESPUESTAABIERTA' : arrayCSV[7],
                'ID_EVALUACION_HISTORICO' : arrayCSV[8]
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/evaluacion_desempeno/detalle_eva" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery E_D_detalle_eva' >> beam.io.WriteToBigQuery(
        gcs_project + ":gestion_humana.E_D_detalle_eva",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run();jobObject.wait_until_finish()

    
    # jobID = jobObject.job_id()

	return ("Corrio sin problema")
