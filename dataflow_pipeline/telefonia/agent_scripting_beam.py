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

####################### PARAMETROS DE LA TABLA EN BQ ##########################

TABLE_SCHEMA = (
	'nombre_cliente:STRING,'
	'apellido_cliente:STRING,'
	'tipo_doc:STRING,'
	'id_doc_cliente:STRING,'
	'sexo:STRING,'
	'pais:STRING,'
	'departamento:STRING,'
	'ciudad:STRING,'
	'zona:STRING,'
	'direccion:STRING,'
	'opt1:STRING,'
	'opt2:STRING,'
	'opt3:STRING,'
	'opt4:STRING,'
	'opt5:STRING,'
	'opt6:STRING,'
	'opt7:STRING,'
	'opt8:STRING,'
	'opt9:STRING,'
	'opt10:STRING,'
	'opt11:STRING,'
	'opt12:STRING,'
	'tel1:STRING,'
	'tel2:STRING,'
	'tel3:STRING,'
	'tel4:STRING,'
	'tel5:STRING,'
	'tel6:STRING,'
	'tel7:STRING,'
	'tel8:STRING,'
	'tel9:STRING,'
	'tel10:STRING,'
	'tel_extra:STRING,'
	'id_agent:STRING,'
	'fecha:STRING,'
	'llamadas:STRING,'
	'id_call:STRING,'
	'rellamada:STRING,'
	'resultado:STRING,'
	'cod_rslt1:STRING,'
	'cod_rslt2:STRING,'
	'rellamada_count:STRING,'
	'id_cliente:STRING,'
	'ipdial_code:STRING,'
	'rand_token:STRING,'
	'hora:STRING,'
	'id_campana:STRING,'
	'fecha_cargue:STRING'

)

################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split(';')
		tupla= {
				'nombre_cliente': arrayCSV[0],
				'apellido_cliente': arrayCSV[1],
				'tipo_doc': arrayCSV[2],
				'id_doc_cliente': arrayCSV[3],
				'sexo': arrayCSV[4],
				'pais': arrayCSV[5],
				'departamento': arrayCSV[6],
				'ciudad': arrayCSV[7],
				'zona': arrayCSV[8],
				'direccion': arrayCSV[9],
				'opt1': arrayCSV[10],
				'opt2': arrayCSV[11],
				'opt3': arrayCSV[12],
				'opt4': arrayCSV[13],
				'opt5': arrayCSV[14],
				'opt6': arrayCSV[15],
				'opt7': arrayCSV[16],
				'opt8': arrayCSV[17],
				'opt9': arrayCSV[18],
				'opt10': arrayCSV[19],
				'opt11': arrayCSV[20],
				'opt12': arrayCSV[21],
				'tel1': arrayCSV[22],
				'tel2': arrayCSV[23],
				'tel3': arrayCSV[24],
				'tel4': arrayCSV[25],
				'tel5': arrayCSV[26],
				'tel6': arrayCSV[27],
				'tel7': arrayCSV[28],
				'tel8': arrayCSV[29],
				'tel9': arrayCSV[30],
				'tel10': arrayCSV[31],
				'tel_extra': arrayCSV[32],
				'id_agent': arrayCSV[33],
				'fecha': arrayCSV[34],
				'llamadas': arrayCSV[35],
				'id_call': arrayCSV[36],
				'rellamada': arrayCSV[37],
				'resultado': arrayCSV[38],
				'cod_rslt1': arrayCSV[39],
				'cod_rslt2': arrayCSV[40],
				'rellamada_count': arrayCSV[41],
				'id_cliente': arrayCSV[42],
				'ipdial_code': arrayCSV[43],
				'rand_token': arrayCSV[44],
				'hora': arrayCSV[45],
				'id_campana': arrayCSV[46],
				'fecha_cargue': arrayCSV[47]
				}
		return [tupla]

############################ CODIGO DE EJECUCION ###################################

def run(output):

	gcs_path = 'gs://ct-telefonia' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "15",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(output)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	
	transformed | 'Escritura a BigQuery Telefonia' >> beam.io.WriteToBigQuery(
		gcs_project + ":telefonia.Agent_scripting", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")