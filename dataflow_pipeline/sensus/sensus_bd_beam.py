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
	'ID_RESULTADO:STRING, '
    'ID_CC:STRING, '
    'DOC_ASESOR:STRING, '
    'NOMBRES:STRING, '
    'DOC_LIDER:STRING, '
    'NOMBRE_TEAM_LEADER:STRING, '
    'DOC_EJECUTIVO:STRING, '
    'NOMBRE_EJECUTIVO:STRING, '
    'DOC_GERENTE:STRING, '
    'NOMBRE_GERENTE:STRING, '
    'DESCRIPCION:STRING, '
    'SEDE:STRING, '
    'PRODUCTO:STRING, '
    'DOC_ASEGURADOR:STRING, '
    'EVALUADOR:STRING, '
    'FECHA_ASEGURAMIENTO:STRING, '
    'ESTADO_ASEG:STRING, '
    'HORA_ASEGURAMIENTO:STRING, '
    'PEC:STRING, '
    'PENC:STRING, '
    'GOS:STRING, '
    'ID_CALL:STRING, '
    'DOC_CLIENTE:STRING, '
    'TELEFONO_CLIENTE:STRING, '
    'TIPIFICACION:STRING, '
    'FECHA_REGISTRO:STRING, '
    'HORA_REGISTRO:STRING '

)

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'ID_RESULTADO' : arrayCSV[0],
                'ID_CC' : arrayCSV[1],
                'DOC_ASESOR' : arrayCSV[2],
                'NOMBRES' : arrayCSV[3],
                'DOC_LIDER' : arrayCSV[4],
                'NOMBRE_TEAM_LEADER' : arrayCSV[5],
                'DOC_EJECUTIVO' : arrayCSV[6],
                'NOMBRE_EJECUTIVO' : arrayCSV[7],
                'DOC_GERENTE' : arrayCSV[8],
                'NOMBRE_GERENTE' : arrayCSV[9],
                'DESCRIPCION' : arrayCSV[10],
                'SEDE' : arrayCSV[11],
                'PRODUCTO' : arrayCSV[12],
                'DOC_ASEGURADOR' : arrayCSV[13],
                'EVALUADOR' : arrayCSV[14],
                'FECHA_ASEGURAMIENTO' : arrayCSV[15],
                'ESTADO_ASEG' : arrayCSV[16],
                'HORA_ASEGURAMIENTO' : arrayCSV[17],
                'PEC' : arrayCSV[18],
                'PENC' : arrayCSV[19],
                'GOS' : arrayCSV[20],
                'ID_CALL' : arrayCSV[21],
                'DOC_CLIENTE' : arrayCSV[22],
                'TELEFONO_CLIENTE' : arrayCSV[23],
                'TIPIFICACION' : arrayCSV[24],
                'FECHA_REGISTRO' : arrayCSV[25],
                'HORA_REGISTRO' : arrayCSV[26]

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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/Segmento/bd_sensus" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery unificadas' >> beam.io.WriteToBigQuery(
        gcs_project + ":sensus.bd_sensus",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")