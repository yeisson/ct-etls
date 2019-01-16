####################################################################################################
####################################################################################################
############################                                          ##############################
############################ REPORTE BEAM DE TELEFONIA = LOGIN-LOGOUT ##############################
############################                                          ##############################
####################################################################################################
####################################################################################################



######################## INDICE ##############################

# FILA.11.................... INDICE
# FILA.22.................... LIBRERIAS
# FILA.49.................... VARIABLES GLOBALES
# FILA.67.................... PARAMETROS DE LA TABLA EN BQ
# FILA.96.................... PAR-DO
# FILA.131................... CODIGO DE EJECUCION

##############################################################


########################### LIBRERIAS #########################################

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

###############################################################################



####################### VARIABLES GLOBALES ####################################

ayer = datetime.datetime.today() - datetime.timedelta(days = 1)
if len(str(ayer.day)) == 1:
    dia = "0" + str(ayer.day)
else:
    dia = ayer.day
if len(str(ayer.month)) == 1:
    mes = "0"+ str(ayer.month)
else:
    mes = ayer.month
ano = ayer.year
fecha = str(ano)+str(mes)+str(dia)
# fecha = "20181204 - 20181231"
###############################################################################



####################### PARAMETROS DE LA TABLA EN BQ ##########################

TABLE_SCHEMA = (
	'operation:STRING,'
	'id_agent_ipdial:STRING,'
	'skill:STRING,'
	'date:DATETIME,'
	'id_call:STRING,'
	'ANI:STRING,'
	'id_customer:STRING,'
	'q01:INTEGER,'
	'q02:INTEGER,'
	'q03:INTEGER,'
	'q04:INTEGER,'
	'q05:INTEGER,'
	'q06:INTEGER,'
	'q07:INTEGER,'
	'q08:INTEGER,'
	'q09:INTEGER,'
	'q10:INTEGER,'
	'duration:INTEGER,'
	'type_call:STRING,'
	'result:STRING,'
	'id_cliente:STRING,'
	'cartera:STRING'
)
################################################################################


################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split(',')
		tupla= {
				'operation': arrayCSV[0],
				'id_agent_ipdial': arrayCSV[1],
				'skill': arrayCSV[2],
				'date': arrayCSV[3],
				'id_call': arrayCSV[4],
				'ANI': arrayCSV[5],
				'id_customer': arrayCSV[6],
				'q01': arrayCSV[7],
				'q02': arrayCSV[8],
				'q03': arrayCSV[9],
				'q04': arrayCSV[10],
				'q05': arrayCSV[11],
				'q06': arrayCSV[12],
				'q07': arrayCSV[13],
				'q08': arrayCSV[14],
				'q09': arrayCSV[15],
				'q10': arrayCSV[16],
				'duration': arrayCSV[17],
				'type_call': arrayCSV[18],
				'result': arrayCSV[19],
				'id_cliente': arrayCSV[20],
				'cartera': arrayCSV[21]
				}
		return [tupla]

################################################################################


############################ CODIGO DE EJECUCION ###################################
def run(data):

	gcs_path = "gs://ct-telefonia" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

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

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText("/media/BI_Archivos/GOOGLE/Telefonia/csat.txt")
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("//192.168.20.87/BI_Archivos/GOOGLE/Telefonia/csat.txt")
	lines | 'Escribir en Archivo' >> WriteToText(gcs_path + "/csat/" + fecha, file_name_suffix='.txt',shard_name_template='')
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	transformed | 'Escritura a BigQuery Telefonia' >> beam.io.WriteToBigQuery(
		gcs_project + ":telefonia.csat", 
		schema=TABLE_SCHEMA,
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)
	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")

#################################################################################