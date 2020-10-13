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
                'ID_REGISTRO:STRING, '
                'DOCUMENTO:STRING, '
                'NOMBRES:STRING, '
                'SEXO:STRING, '
                'PROCESO:STRING, '
                'CODIGO:STRING, '
                'CENTRO_COSTOS:STRING, '
                'GERENTE:STRING, '
                'CARGO:STRING, '
                'CIUDAD:STRING, '
                'FECHA_INGRESO:STRING, '
                'EMPLEADOR:STRING, '
                'TIPO_CONTRATO:STRING, '
                'USUARIO_DA:STRING, '
                'LIDER:STRING, '
                'ESTADO:STRING '
                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'ID_REGISTRO' : arrayCSV[0],
                'DOCUMENTO' : arrayCSV[1],
                'NOMBRES' : arrayCSV[2],
                'SEXO' : arrayCSV[3],
                'PROCESO' : arrayCSV[4],
                'CODIGO' : arrayCSV[5],
                'CENTRO_COSTOS' : arrayCSV[6],
                'GERENTE' : arrayCSV[7],
                'CARGO' : arrayCSV[8],
                'CIUDAD' : arrayCSV[9],
                'FECHA_INGRESO' : arrayCSV[10],
                'EMPLEADOR' : arrayCSV[11],
                'TIPO_CONTRATO' : arrayCSV[12],
                'USUARIO_DA' : arrayCSV[13],
                'LIDER' : arrayCSV[14],
                'ESTADO' : arrayCSV[15]
				}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-felicidad_y_cultura" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/Clima/personal" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery Felicidad_y_Cultura' >> beam.io.WriteToBigQuery(
        gcs_project + ":Felicidad_y_Cultura.Personal",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run();jobObject.wait_until_finish()

    
    # jobID = jobObject.job_id()

	return ("Corrio sin problema")
