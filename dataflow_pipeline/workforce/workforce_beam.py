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

TABLE_SCHEMA = ('DOCUMENTO_NEG:STRING, '
                'SEGMENTO:STRING, '
                'ITER:STRING, '
                'FECHA_MALLA:STRING, '
                'HORA_INICIO:STRING, '
                'FECHA_FINAL:STRING, '
                'HORA_FINAL:STRING, '
                'LOGUEO:STRING, '
                'DESLOGUEO:STRING, '
                'DIF_INICIO:STRING, '
                'DIF_FINAL:STRING, '
                'AUSENTISMO:STRING, '
                'TIEMPO_MALLA:STRING, '
                'TIEMPO_CONEXION:STRING, '
                'TIEMPO_CONEXION_TIEMPO:STRING, '
                'TIEMPO_ESTAUX:STRING, '
                'TIEMPO_ESTAUX_TIEMPO:STRING, '
                'TIEMPO_ESTAUX_OUT:STRING, '
                'TIEMPO_ESTAUX_OUT_TIEMPO:STRING, '
                'ADHERENCIA_MALLA:STRING, '
                'ADHERENCIA_TIEMPO:STRING, '
                'CENTRO_COSTO:STRING, '
                'REL_UNICO:STRING, '
                'REL_ORDEN:STRING '

                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'DOCUMENTO_NEG' : arrayCSV[0],
                        'SEGMENTO' : arrayCSV[1],
                        'ITER' : arrayCSV[2],
                        'FECHA_MALLA' : arrayCSV[3],
                        'HORA_INICIO' : arrayCSV[4],
                        'FECHA_FINAL' : arrayCSV[5],
                        'HORA_FINAL' : arrayCSV[6],
                        'LOGUEO' : arrayCSV[7],
                        'DESLOGUEO' : arrayCSV[8],
                        'DIF_INICIO' : arrayCSV[9],
                        'DIF_FINAL' : arrayCSV[10],
                        'AUSENTISMO' : arrayCSV[11],
                        'TIEMPO_MALLA' : arrayCSV[12],
                        'TIEMPO_CONEXION' : arrayCSV[13],
                        'TIEMPO_CONEXION_TIEMPO' : arrayCSV[14],
                        'TIEMPO_ESTAUX' : arrayCSV[15],
                        'TIEMPO_ESTAUX_TIEMPO' : arrayCSV[16],
                        'TIEMPO_ESTAUX_OUT' : arrayCSV[17],
                        'TIEMPO_ESTAUX_OUT_TIEMPO' : arrayCSV[18],
                        'ADHERENCIA_MALLA' : arrayCSV[19],
                        'ADHERENCIA_TIEMPO' : arrayCSV[20],
                        'CENTRO_COSTO' : arrayCSV[21],
                        'REL_UNICO' : arrayCSV[22],
                        'REL_ORDEN' : arrayCSV[23]
			}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-workforce" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/adherencia/workforce" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery Workforce' >> beam.io.WriteToBigQuery(
        gcs_project + ":Workforce.Adherencia",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")



