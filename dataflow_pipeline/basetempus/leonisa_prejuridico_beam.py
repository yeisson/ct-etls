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
		'ZONA:STRING, '
		'CODIGO_DE_CIUDAD:STRING, '
		'CEDULA_CIUDADANIA:STRING, '
		'CODIGO_INTERNO:STRING, '
		'TIPO_COMPRADORA:STRING, '
		'CUSTOMER_CLASS:STRING, '
		'CUPO:STRING, '
		'NUMERO_DE_OBLIGACION:STRING, '
		'VALOR_FACTURA:STRING, '
		'FECHA_FACTURA:STRING, '
		'FECHA_VENCIMIENTO:STRING, '
		'VALOR_SALDO_EN_CARTERA:STRING, '
		'DIAS_DE_VENCIMIENTO:STRING, '
		'CAMPANA_ORIGINAL:STRING, '
		'ULTIMA_CAMPANA:STRING, '
		'CODIGO:STRING, '
		'NOMBRE:STRING, '
		'APELLIDOS:STRING, '
		'TELEFONO_1:STRING, '
		'CELULAR:STRING, '
		'TEL_CEL_2:STRING, '
		'E_MAIL:STRING, '
		'AUTORIZO_ENVIO_DE_MENSAJES_DE_TEXTO_A_MI_CELULAR_SI_NO:STRING, '
		'AUTORIZO_CORREOS_DE_VOZ_A_MI_CELULAR_SI_NO:STRING, '
		'AUTORIZO_ENVIO_DE_E_MAIL_SI_NO:STRING, '
		'DIRECCION:STRING, '
		'BARRIO:STRING, '
		'CIUDAD:STRING, '
		'DEPARTAMENTO:STRING, '
		'DIRECCION_1:STRING, '
		'BARRIO_1:STRING, '
		'CIUDAD_1:STRING, '
		'DEPARTAMENTO_1:STRING, '
		'NOMBRE_REF1:STRING, '
		'APELLIDO_1:STRING, '
		'PARENTESCO_1:STRING, '
		'CELULAR_1:STRING, '
		'NOMBRE_REF2:STRING, '
		'APELLIDO_2:STRING, '
		'PARENTESCO_2:STRING, '
		'TELEFONO_2:STRING, '
		'CELULAR_2:STRING, '
		'DIRECCION_2:STRING, '
		'CIUDAD_2:STRING, '
		'DEPARTAMENTO_2:STRING, '
		'NOMBRE_REF3:STRING, '
		'APELLIDO_3:STRING, '
		'TELEFONO_3:STRING, '
		'CELULAR_3:STRING, '
		'DIRECCION_3:STRING, '
		'CIUDAD_3:STRING, '
		'DEPARTAMENTO_3:STRING, '
		'NOMBRE_REF4:STRING, '
		'APELLIDO_4:STRING, '
		'DIRECCION_4:STRING, '
		'TELEFONO_4:STRING, '
		'CELULAR_4:STRING, '
		'CIUDAD_4:STRING, '
		'DEPARTAMENTO_4:STRING, '
		'ABOGAD:STRING, '
		'DIVSION:STRING, '
		'PAIS:STRING, '
		'FECHA_DE_PROXIMA_CONFERENCIA:STRING '






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
				'ZONA' : arrayCSV[0],
				'CODIGO_DE_CIUDAD' : arrayCSV[1],
				'CEDULA_CIUDADANIA' : arrayCSV[2],
				'CODIGO_INTERNO' : arrayCSV[3],
				'TIPO_COMPRADORA' : arrayCSV[4],
				'CUSTOMER_CLASS' : arrayCSV[5],
				'CUPO' : arrayCSV[6],
				'NUMERO_DE_OBLIGACION' : arrayCSV[7],
				'VALOR_FACTURA' : arrayCSV[8],
				'FECHA_FACTURA' : arrayCSV[9],
				'FECHA_VENCIMIENTO' : arrayCSV[10],
				'VALOR_SALDO_EN_CARTERA' : arrayCSV[11],
				'DIAS_DE_VENCIMIENTO' : arrayCSV[12],
				'CAMPANA_ORIGINAL' : arrayCSV[13],
				'ULTIMA_CAMPANA' : arrayCSV[14],
				'CODIGO' : arrayCSV[15],
				'NOMBRE' : arrayCSV[16],
				'APELLIDOS' : arrayCSV[17],
				'TELEFONO_1' : arrayCSV[18],
				'CELULAR' : arrayCSV[19],
				'TEL_CEL_2' : arrayCSV[20],
				'E_MAIL' : arrayCSV[21],
				'AUTORIZO_ENVIO_DE_MENSAJES_DE_TEXTO_A_MI_CELULAR_SI_NO' : arrayCSV[22],
				'AUTORIZO_CORREOS_DE_VOZ_A_MI_CELULAR_SI_NO' : arrayCSV[23],
				'AUTORIZO_ENVIO_DE_E_MAIL_SI_NO' : arrayCSV[24],
				'DIRECCION' : arrayCSV[25],
				'BARRIO' : arrayCSV[26],
				'CIUDAD' : arrayCSV[27],
				'DEPARTAMENTO' : arrayCSV[28],
				'DIRECCION_1' : arrayCSV[29],
				'BARRIO_1' : arrayCSV[30],
				'CIUDAD_1' : arrayCSV[31],
				'DEPARTAMENTO_1' : arrayCSV[32],
				'NOMBRE_REF1' : arrayCSV[33],
				'APELLIDO_1' : arrayCSV[34],
				'PARENTESCO_1' : arrayCSV[35],
				'CELULAR_1' : arrayCSV[36],
				'NOMBRE_REF2' : arrayCSV[37],
				'APELLIDO_2' : arrayCSV[38],
				'PARENTESCO_2' : arrayCSV[39],
				'TELEFONO_2' : arrayCSV[40],
				'CELULAR_2' : arrayCSV[41],
				'DIRECCION_2' : arrayCSV[42],
				'CIUDAD_2' : arrayCSV[43],
				'DEPARTAMENTO_2' : arrayCSV[44],
				'NOMBRE_REF3' : arrayCSV[45],
				'APELLIDO_3' : arrayCSV[46],
				'TELEFONO_3' : arrayCSV[47],
				'CELULAR_3' : arrayCSV[48],
				'DIRECCION_3' : arrayCSV[49],
				'CIUDAD_3' : arrayCSV[50],
				'DEPARTAMENTO_3' : arrayCSV[51],
				'NOMBRE_REF4' : arrayCSV[52],
				'APELLIDO_4' : arrayCSV[53],
				'DIRECCION_4' : arrayCSV[54],
				'TELEFONO_4' : arrayCSV[55],
				'CELULAR_4' : arrayCSV[56],
				'CIUDAD_4' : arrayCSV[57],
				'DEPARTAMENTO_4' : arrayCSV[58],
				'ABOGAD' : arrayCSV[59],
				'DIVSION' : arrayCSV[60],
				'PAIS' : arrayCSV[61],
				'FECHA_DE_PROXIMA_CONFERENCIA' : arrayCSV[62]





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
		gcs_project + ":unificadas.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



