from __future__ import print_function, absolute_import

import logging
import re
import json
import requests
import uuid
import os
import argparse
import uuid
import socket

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText, textio
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
	'idkey:STRING, '
	'fecha:STRING, '
	'Id_Seguimiento:STRING, '
	'Id_Docdeu:STRING, '
	'Id_Gestion:STRING, '
	'Id_Causal:STRING, '
	'Fecha_Seguimiento:STRING, '
	'Id_Usuario:STRING, '
	'Id_Abogado:STRING, '
	'Id_pago:STRING '
)

# Clase encargada de ejecutar un modelo ParDo de separacion de fichero GCS
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha

	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha' : self.mifecha,
				'Id_Seguimiento' : arrayCSV[0],
				'Id_Docdeu' : arrayCSV[1],
				'Id_Gestion' : arrayCSV[2],
				'Id_Causal' : arrayCSV[3],
				'Fecha_Seguimiento' : arrayCSV[4],
				'Id_Usuario' : arrayCSV[5],
				'Id_Abogado' : arrayCSV[6],
				'Id_pago' : arrayCSV[7]
				}
		
		return [tupla]


def run(archivo, mifecha):

	gcs_path = "gs://ct-leonisa" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	#mi_runner = "DataflowRunner"

	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
        # "--num_workers", "30",
        # "--autoscaling_algorithm", "NONE"
    ])
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/" + archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	transformed | 'Escritura a BigQuery Leonisa' >> beam.io.WriteToBigQuery(
		gcs_project + ":leonisa.seguimiento", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	pipeline.run()
	
	response = {}
	response["code"] = 200
	response["description"] = "Dataflow iniciado correctamente"
	response["input"] = gcs_path + "/" + archivo

	return response



