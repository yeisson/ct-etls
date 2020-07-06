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
    'ID_TIPIF:STRING, '
    'ID_CAMPANA:STRING, '
    'ID_COD_GESTION:STRING, '
    'ID_COD_CAUSAL:STRING, '
    'ID_COD_SUBCAUSAL:STRING, '
    'COD_HOMOLOGADO:STRING, '
    'COD_HOMOLOGADO_CAUSAL:STRING, '
    'ADICIONALONE:STRING, '
    'ADICIONALTWO:STRING, '
    'ADICIONALTREE:STRING, '
    'FECHA_CREACION:STRING, '
    'PLANTILLA:STRING, '
    'PLANTILLA1:STRING, '
    'ESTADO:STRING, '
    'USUARIO_GESTOR:STRING, '
    'HIT:STRING '

)

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'ID_TIPIF' : arrayCSV[0],
                'ID_CAMPANA' : arrayCSV[1],
                'ID_COD_GESTION' : arrayCSV[2],
                'ID_COD_CAUSAL' : arrayCSV[3],
                'ID_COD_SUBCAUSAL' : arrayCSV[4],
                'COD_HOMOLOGADO' : arrayCSV[5],
                'COD_HOMOLOGADO_CAUSAL' : arrayCSV[6],
                'ADICIONALONE' : arrayCSV[7],
                'ADICIONALTWO' : arrayCSV[8],
                'ADICIONALTREE' : arrayCSV[9],
                'FECHA_CREACION' : arrayCSV[10],
                'PLANTILLA' : arrayCSV[11],
                'PLANTILLA1' : arrayCSV[12],
                'ESTADO' : arrayCSV[13],
                'USUARIO_GESTOR' : arrayCSV[14],
                'HIT' : arrayCSV[15]

				}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-unificadas" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/tipificaciones/Unificadas_tipificaciones" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery unificadas' >> beam.io.WriteToBigQuery(
        gcs_project + ":unificadas.Tipificaciones",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run();jobObject.wait_until_finish()

    
    # jobID = jobObject.job_id()

	return ("Corrio sin problema")