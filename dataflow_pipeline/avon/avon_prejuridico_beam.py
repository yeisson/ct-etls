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

TABLE_SCHEMA = (
	'idkey:STRING, '
	'Fecha_Cargue:STRING, '
	'Id_Docdeu:STRING,'
	'Nit:STRING,'
	'Factura:STRING,'
	'Fecha_Factura:STRING,'
	'Campana:STRING,'
	'Ano:STRING,'
	'Zona:STRING,'
	'Unidad:STRING,'
	'Seccion:STRING,'
	'Past_Due:STRING,'
	'Ultim_Num_InVoice:STRING,'
	'Valor_Factura:STRING,'
	'Saldo:STRING,'
	'N_Vencidas:STRING,'
	'Num_Campanas:STRING,'
	'estado:STRING,'
	'Valor_PD1:STRING,'
	'CT:STRING,'
	'Fecha:STRING,'
	'Usuario:STRING,'
	'asignacion:STRING,'
	'Ciclo:STRING,'
	'Vlr_redimir:STRING,'
	'dia:STRING,'
	'Dia_Estrategia:STRING,'
	'Origen:STRING,'
	'marca:STRING'
)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				'Fecha_Cargue' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'Id_Docdeu': arrayCSV[0],
				'Nit': arrayCSV[1],
				'Factura': arrayCSV[2],
				'Fecha_Factura': arrayCSV[3],
				'Campana': arrayCSV[4],
				'Ano': arrayCSV[5],
				'Zona': arrayCSV[6],
				'Unidad': arrayCSV[7],
				'Seccion': arrayCSV[8],
				'Past_Due': arrayCSV[9],
				'Ultim_Num_InVoice': arrayCSV[10],
				'Valor_Factura': arrayCSV[11],
				'Saldo': arrayCSV[12],
				'N_Vencidas': arrayCSV[13],
				'Num_Campanas': arrayCSV[14],
				'estado': arrayCSV[15],
				'Valor_PD1': arrayCSV[16],
				'CT': arrayCSV[17],
				'Fecha': arrayCSV[18],
				'Usuario': arrayCSV[19],
				'asignacion': arrayCSV[20],
				'Ciclo': arrayCSV[21],
				'Vlr_redimir': arrayCSV[22],
				'dia': arrayCSV[23],
				'Dia_Estrategia': arrayCSV[24],
				'Origen': arrayCSV[25],
				'marca': arrayCSV[26]
				}
		
		return [tupla]



def run():

	gcs_path = 'gs://ct-avon' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"
	FECHA_CARGUE = str(datetime.date.today())

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/prejuridico/Avon_inf_prej_" + FECHA_CARGUE + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/prejuridico/Avon_inf_prej2_" + FECHA_CARGUE,file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery Avon' >> beam.io.WriteToBigQuery(
        gcs_project + ":avon.prejuridico",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")



