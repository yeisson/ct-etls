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
    'REFERENCIA:STRING, '
    'TIPO_IDENTIFICACION:STRING, '
    'CEDULA_NIT:STRING, '
    'NOMBRE_DEL_CLIENTE:STRING, '
    'SEXO:STRING, '
    'FECHA_DE_NACIMIENTO:STRING, '
    'EDAD:STRING, '
    'CODIGO_PRODUCTO:STRING, '
    'CICLO:STRING, '
    'DIRECCION_CORRESPONDENCIA:STRING, '
    'CODIGO_CIUDAD_DE_CORRESPON:STRING, '
    'TEL_NRO_CORRESPONDENCIA:STRING, '
    'CELULAR:STRING, '
    'NOMBRE_EMPRESA:STRING, '
    'DIRNRO_EMPRESA:STRING, '
    'TELNRO_OFICINA:STRING, '
    'CODIGO_CIUDAD_OFICINA:STRING, '
    'CARGO_EMPRESA:STRING, '
    'FECHA_DE_INGRESO:STRING, '
    'CODIGO_ESTADO_CIVIL:STRING, '
    'CEDULA_CONYUGE:STRING, '
    'PERSONAS_A_CARGO:STRING, '
    'CODIGO_PROFESION:STRING, '
    'SALARIO_MENSUAL:STRING, '
    'TOTAL_EGRESOS:STRING, '
    'OCUPACION:STRING, '
    'CUPO:STRING, '
    'SALDO_DISPONIBLE:STRING, '
    'MERCADO:STRING, '
    'FECHA_DE_APROBACION_TCO:STRING, '
    'FECHA_DESBLOQUEO_TCO:STRING, '
    'MAIL:STRING, '
    'TIPO_EXTRACTO:STRING, '
    'SEGMENTO:STRING, '
    'CATEGORIA:STRING, '
    'ANO_MES_ENVIO:STRING, '
    'COD_GESTION:STRING, '
    'TIPIFICACION:STRING, '
    'TEL_MARCADO:STRING, '
    'PROSPECT_NUM:INTEGER, '
    'RANGO_CUPO_VENTA:STRING, '
    'PLAN_MAX_AP:STRING '
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
				'REFERENCIA' : arrayCSV[0],
                'TIPO_IDENTIFICACION' : arrayCSV[1],
                'CEDULA_NIT' : arrayCSV[2],
                'NOMBRE_DEL_CLIENTE' : arrayCSV[3],
                'SEXO' : arrayCSV[4],
                'FECHA_DE_NACIMIENTO' : arrayCSV[5],
                'EDAD' : arrayCSV[6],
                'CODIGO_PRODUCTO' : arrayCSV[7],
                'CICLO' : arrayCSV[8],
                'DIRECCION_CORRESPONDENCIA' : arrayCSV[9],
                'CODIGO_CIUDAD_DE_CORRESPON' : arrayCSV[10],
                'TEL_NRO_CORRESPONDENCIA' : arrayCSV[11],
                'CELULAR' : arrayCSV[12],
                'NOMBRE_EMPRESA' : arrayCSV[13],
                'DIRNRO_EMPRESA' : arrayCSV[14],
                'TELNRO_OFICINA' : arrayCSV[15],
                'CODIGO_CIUDAD_OFICINA' : arrayCSV[16],
                'CARGO_EMPRESA' : arrayCSV[17],
                'FECHA_DE_INGRESO' : arrayCSV[18],
                'CODIGO_ESTADO_CIVIL' : arrayCSV[19],
                'CEDULA_CONYUGE' : arrayCSV[20],
                'PERSONAS_A_CARGO' : arrayCSV[21],
                'CODIGO_PROFESION' : arrayCSV[22],
                'SALARIO_MENSUAL' : arrayCSV[23],
                'TOTAL_EGRESOS' : arrayCSV[24],
                'OCUPACION' : arrayCSV[25],
                'CUPO' : arrayCSV[26],
                'SALDO_DISPONIBLE' : arrayCSV[27],
                'MERCADO' : arrayCSV[28],
                'FECHA_DE_APROBACION_TCO' : arrayCSV[29],
                'FECHA_DESBLOQUEO_TCO' : arrayCSV[30],
                'MAIL' : arrayCSV[31],
                'TIPO_EXTRACTO' : arrayCSV[32],
                'SEGMENTO' : arrayCSV[33],
                'CATEGORIA' : arrayCSV[34],
                'ANO_MES_ENVIO' : arrayCSV[35],
                'COD_GESTION' : arrayCSV[36],
                'TIPIFICACION' : arrayCSV[37],
                'TEL_MARCADO' : arrayCSV[38],
                'PROSPECT_NUM' : arrayCSV[39],
                'RANGO_CUPO_VENTA' : arrayCSV[40],
                'PLAN_MAX_AP' : arrayCSV[41]
				}
		
		return [tupla]


def run(archivo, mifecha):

	gcs_path = "gs://ct-metlife" #Definicion de la raiz del bucket
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
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181129 0800.csv", skip_header_lines=1)
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_seg", file_name_suffix='.csv',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/info-segumiento/info_carga_banco_seg",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery metlife' >> beam.io.WriteToBigQuery(
		gcs_project + ":MetLife.Base", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")