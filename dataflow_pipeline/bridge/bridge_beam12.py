from __future__ import print_function, absolute_import
import logging
import re
import json
import requests
import uuid
import time
import os
import socket
import argparse
import uuid
import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
			'Usuario_Adminfo:STRING,'
			'Producto:STRING,'
			'Fecha:STRING,'
			'Hora:STRING,'
			'Dias_Lab:STRING,'
			'Gest:STRING,'
			'Gest_Contacto:STRING,'
			'Gest_Contacto_Dir:STRING,'
			'Gest_Product:STRING,'
			'Gest_Efectiva:STRING,'
			'Gest_Buzones:STRING,'
			'Valor_Obligacion:STRING,'
			'Valor_Vencido:STRING,'
			'Valor_Oblig_Contactado:STRING,'
			'Valor_Venc_Contactado:STRING,'
			'Valor_Oblig_RPC:STRING,'
			'Valor_Venc_RPC:STRING,'
			'Valor_Oblig_Productivo:STRING,'
			'Valor_Venc_Productivo:STRING,'
			'Valor_Oblig_Buzones:STRING,'
			'Valor_Venc_Buzones:STRING'
			)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {
				'Usuario_Adminfo': arrayCSV[0],
				'Producto': arrayCSV[1],
				'Fecha': arrayCSV[2],
				'Hora': arrayCSV[3],
				'Dias_Lab': arrayCSV[4],
				'Gest': arrayCSV[5],
				'Gest_Contacto': arrayCSV[6],
				'Gest_Contacto_Dir': arrayCSV[7],
				'Gest_Product': arrayCSV[8],
				'Gest_Efectiva': arrayCSV[9],
				'Gest_Buzones': arrayCSV[10],
				'Valor_Obligacion': arrayCSV[11],
				'Valor_Vencido': arrayCSV[12],
				'Valor_Oblig_Contactado': arrayCSV[13],
				'Valor_Venc_Contactado': arrayCSV[14],
				'Valor_Oblig_RPC': arrayCSV[15],
				'Valor_Venc_RPC': arrayCSV[16],
				'Valor_Oblig_Productivo': arrayCSV[17],
				'Valor_Venc_Productivo': arrayCSV[18],
				'Valor_Oblig_Buzones': arrayCSV[19],
				'Valor_Venc_Buzones': arrayCSV[20]
				}
		
		return [tupla]

############################ CODIGO DE EJECUCION ###################################
def run(table, TABLE_DB):

	gcs_path = 'gs://ct-bridge' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"
	FECHA_CARGUE = str(datetime.date.today())

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "10",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/" + FECHA_CARGUE + "_" + TABLE_DB +".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/" + "REWORK",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Bridge' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contactabilidad."+ table, 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")


################################################################################