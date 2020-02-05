#coding: utf-8 
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
		'fecha:STRING, '
		'ANO:STRING, '
		'MES:STRING, '
		'CENTRO_DE_COSTOS:STRING, '
		'OPERACION:STRING, '
		'PDC_SEMANA_1:STRING, '
		'EJEC_SEMANA_1:STRING, '
		'VAR_PDC_VS_EJEC_1:STRING, '
		'PDC_SEMANA_2:STRING, '
		'EJEC_SEMANA_2:STRING, '
		'VAR_PDC_VS_EJEC_2:STRING, '
		'PDC_SEMANA_3:STRING, '
		'EJEC_SEMANA_3:STRING, '
		'VAR_PDC_VS_EJEC_3:STRING, '
		'PDC_SEMANA_4:STRING, '
		'EJEC_SEMANA_4:STRING, '
		'VAR_PDC_VS_EJEC_4:STRING, '
		'PDC_SEMANA_5:STRING, '
		'EJEC_SEMANA_5:STRING, '
		'VAR_PDC_VS_EJEC_5:STRING, '
		'PRESUPUESTO_MES:STRING, '
		'EJECUC_MES_A_LA_FECHA:STRING, '
		'EJECUCION_PPTO_MES:STRING, '
		'PDC_CIERRE_1:STRING, '
		'PDC_CIERRE_2:STRING, '
		'PDC_CIERRE_3:STRING, '
		'PDC_CIERRE_4:STRING, '
		'PDC_CIERRE_5:STRING, '
		'REAL_FACTURA:STRING, '
		'REAL_GASTO:STRING, '
		'MINUTOS_HABLADOS_CIERRE_MES:STRING, '
		'META_DEL_CLIENTE:STRING '





)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha': self.mifecha,
				'ANO' : arrayCSV[0],
				'MES' : arrayCSV[1],
				'CENTRO_DE_COSTOS' : arrayCSV[2],
				'OPERACION' : arrayCSV[3],
				'PDC_SEMANA_1' : arrayCSV[4],
				'EJEC_SEMANA_1' : arrayCSV[5],
				'VAR_PDC_VS_EJEC_1' : arrayCSV[6],
				'PDC_SEMANA_2' : arrayCSV[7],
				'EJEC_SEMANA_2' : arrayCSV[8],
				'VAR_PDC_VS_EJEC_2' : arrayCSV[9],
				'PDC_SEMANA_3' : arrayCSV[10],
				'EJEC_SEMANA_3' : arrayCSV[11],
				'VAR_PDC_VS_EJEC_3' : arrayCSV[12],
				'PDC_SEMANA_4' : arrayCSV[13],
				'EJEC_SEMANA_4' : arrayCSV[14],
				'VAR_PDC_VS_EJEC_4' : arrayCSV[15],
				'PDC_SEMANA_5' : arrayCSV[16],
				'EJEC_SEMANA_5' : arrayCSV[17],
				'VAR_PDC_VS_EJEC_5' : arrayCSV[18],
				'PRESUPUESTO_MES' : arrayCSV[19],
				'EJECUC_MES_A_LA_FECHA' : arrayCSV[20],
				'EJECUCION_PPTO_MES' : arrayCSV[21],
				'PDC_CIERRE_1' : arrayCSV[22],
				'PDC_CIERRE_2' : arrayCSV[23],
				'PDC_CIERRE_3' : arrayCSV[24],
				'PDC_CIERRE_4' : arrayCSV[25],
				'PDC_CIERRE_5' : arrayCSV[26],
				'REAL_FACTURA' : arrayCSV[27],
				'REAL_GASTO' : arrayCSV[28],
				'MINUTOS_HABLADOS_CIERRE_MES' : arrayCSV[29],
				'META_DEL_CLIENTE' : arrayCSV[30]



				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-pio" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runer = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runer, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "10",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
        # "--num_workers", "30",
        # "--autoscaling_algorithm", "NONE"		
	])
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181206 1100.csv", skip_header_lines=1)
	#lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181129 0800.csv", skip_header_lines=1)
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_seg", file_name_suffix='.csv',shard_name_template='')
	#transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/info-segumiento/info_carga_banco_seg",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery semanal' >> beam.io.WriteToBigQuery(
		gcs_project + ":pio.base", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



