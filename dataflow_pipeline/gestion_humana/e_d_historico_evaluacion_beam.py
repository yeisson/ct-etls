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

TABLE_SCHEMA = ('ID_EVALUACION_HISTORICO:STRING, '
                'OBSERVACIONES:STRING, '
                'DOCUMENTO_EVALUADOR:STRING, '
                'ID_CARGO_EVALUADOR:STRING, '
                'DOCUMENTO_EVALUADO:STRING, '
                'ID_CARGO_EVALUADO:STRING, '
                'ID_CENTROCOSTO:STRING, '
                'FECHA_CIERRE:STRING, '
                'FECHA_EVALUACION:STRING '

                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'ID_EVALUACION_HISTORICO' : arrayCSV[0],
                        'OBSERVACIONES' : arrayCSV[1],
                        'DOCUMENTO_EVALUADOR' : arrayCSV[2],
                        'ID_CARGO_EVALUADOR' : arrayCSV[3],
                        'DOCUMENTO_EVALUADO' : arrayCSV[4],
                        'ID_CARGO_EVALUADO' : arrayCSV[5],
                        'ID_CENTROCOSTO' : arrayCSV[6],
                        'FECHA_CIERRE' : arrayCSV[7],
                        'FECHA_EVALUACION' : arrayCSV[8]

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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/evaluacion_desempeno/historico_eva" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery gestion_humana.E_D_historico_eva' >> beam.io.WriteToBigQuery(
        gcs_project + ":gestion_humana.E_D_historico_eva",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")



