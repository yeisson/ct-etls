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
# FILA.66.................... PARAMETROS DE LA TABLA EN BQ
# FILA.83.................... PAR-DO
# FILA.106................... CODIGO DE EJECUCION

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
	'date:DATE,'
	'agent:STRING,'
	'identification:STRING,'
	'login_date:DATETIME,'
	'logout_date:DATETIME,'
	'login_time:STRING,'
	'ipdial_code:STRING,'
	'id_cliente:STRING,'
	'cartera:STRING'
)
################################################################################


################################# PAR'DO #######################################

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')
		tupla= {
				'date': arrayCSV[0],
				'agent': arrayCSV[1],
				'identification': arrayCSV[2],
				'login_date': arrayCSV[3],
				'logout_date': arrayCSV[4],
				'login_time': arrayCSV[5],
				'ipdial_code': arrayCSV[6],
				'id_cliente': arrayCSV[7],
				'cartera': arrayCSV[8]
				}
		return [tupla]

################################################################################


############################ CODIGO DE EJECUCION ###################################
def run(data):

	gcs_path = 'gs://ct-telefonia' #Definicion de la raiz del bucket
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

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText("/media/BI_Archivos/GOOGLE/Telefonia/Login_out.csv")
	
	# lines | 'Escribir en Archivo' >> beam.io.WriteToText(gcs_path + "/Login_out/" + fecha, file_name_suffix='.txt',shard_name_template='')
	lines | 'Escribir en Archivo' >> WriteToText("/media/BI_Archivos/GOOGLE/Telefonia/x/Login_out2", file_name_suffix='.csv',shard_name_template='')
  # lines | 'Escribir en Archivo' >> WriteToText("archivos/Base_Marcada_small", file_name_suffix='.csv',shard_name_template='')
	# transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escritura a BigQuery Telefonia' >> beam.io.WriteToBigQuery(
	# 	gcs_project + ":telefonia.login_logout", 
	# 	schema=TABLE_SCHEMA, 
	# 	create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
	# 	write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
	# 	)
	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")


################################################################################