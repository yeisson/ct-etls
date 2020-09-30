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
		'CONSECUTIVO_DOCUMENTO_DEUDOR:STRING, '
		'CEDULA_COMPRADORA:STRING, '
		'NOMBRE_COMPRADORA:STRING, '
		'NUMERO_FACTURA:STRING, '
		'PLAN:STRING, '
		'FECHA_OBLIGACION:STRING, '
		'NUMERO_CUOTAS:STRING, '
		'VALOR_FACTURA:STRING, '
		'SALDO:STRING, '
		'ZONA:STRING, '
		'CIUDAD_DE_COMPRADORA:STRING, '
		'NOMBREDPTO:STRING, '
		'GRABADOR_ANTERIOR:STRING, '
		'ASESOR:STRING, '
		'ULTIMO_GRABADOR:STRING, '
		'CODIGO_ABOGADO:STRING, '
		'FECHA_ULTIMA_GESTION:STRING, '
		'FECHA_PROXIMA_CONFERENCIA:STRING, '
		'ULTIMA_FECHA_GRABACION:STRING, '
		'CODIGO_ULTIMA_GESTION:STRING, '
		'CODIGO_DE_GESTION_TELEFONICO:STRING, '
		'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO_1:STRING, '
		'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO_2:STRING, '
		'FECHA_DE_MEJOR_GESTION:STRING, '
		'DIVISION:STRING, '
		'ULTIMO_TELEFONO:STRING, '
		'DIAS_MORA:STRING, '
		'MEJOR_GESTION:STRING, '
		'MAIL_PLAN:STRING, '
		'TIPO_DE_COMPRADORA:STRING, '
		'ESTADO_CUENTA:STRING, '
		'TIPO_COMPRADORA:STRING, '
		'DESCRIPCION_CODIGO_VISITA:STRING, '
		'PAIS_COMPRADORA:STRING, '
		'CAMPANA:STRING, '
		'FECHA_PROMESA_DE_PAGO:STRING, '
		'NUMERO_CONTACTO:STRING '




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
				'CONSECUTIVO_DOCUMENTO_DEUDOR' : arrayCSV[0],
				'CEDULA_COMPRADORA' : arrayCSV[1],
				'NOMBRE_COMPRADORA' : arrayCSV[2],
				'NUMERO_FACTURA' : arrayCSV[3],
				'PLAN' : arrayCSV[4],
				'FECHA_OBLIGACION' : arrayCSV[5],
				'NUMERO_CUOTAS' : arrayCSV[6],
				'VALOR_FACTURA' : arrayCSV[7],
				'SALDO' : arrayCSV[8],
				'ZONA' : arrayCSV[9],
				'CIUDAD_DE_COMPRADORA' : arrayCSV[10],
				'NOMBREDPTO' : arrayCSV[11],
				'GRABADOR_ANTERIOR' : arrayCSV[12],
				'ASESOR' : arrayCSV[13],
				'ULTIMO_GRABADOR' : arrayCSV[14],
				'CODIGO_ABOGADO' : arrayCSV[15],
				'FECHA_ULTIMA_GESTION' : arrayCSV[16],
				'FECHA_PROXIMA_CONFERENCIA' : arrayCSV[17],
				'ULTIMA_FECHA_GRABACION' : arrayCSV[18],
				'CODIGO_ULTIMA_GESTION' : arrayCSV[19],
				'CODIGO_DE_GESTION_TELEFONICO' : arrayCSV[20],
				'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO_1' : arrayCSV[21],
				'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO_2' : arrayCSV[22],
				'FECHA_DE_MEJOR_GESTION' : arrayCSV[23],
				'DIVISION' : arrayCSV[24],
				'ULTIMO_TELEFONO' : arrayCSV[25],
				'DIAS_MORA' : arrayCSV[26],
				'MEJOR_GESTION' : arrayCSV[27],
				'MAIL_PLAN' : arrayCSV[28],
				'TIPO_DE_COMPRADORA' : arrayCSV[29],
				'ESTADO_CUENTA' : arrayCSV[30],
				'TIPO_COMPRADORA' : arrayCSV[31],
				'DESCRIPCION_CODIGO_VISITA' : arrayCSV[32],
				'PAIS_COMPRADORA' : arrayCSV[33],
				'CAMPANA' : arrayCSV[34],
				'FECHA_PROMESA_DE_PAGO' : arrayCSV[35],
				'NUMERO_CONTACTO' : arrayCSV[36]



				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-unificadas" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Leonisa Estrategia' >> beam.io.WriteToBigQuery(
		gcs_project + ":unificadas.estrategia", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



