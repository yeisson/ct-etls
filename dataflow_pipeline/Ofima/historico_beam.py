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
		'Idkey:STRING, '
		'Fecha:STRING, '
		'FECHA_GESTION:STRING, '
		'NUMERODEPEDIDO:STRING, '
		'MEDIO_DE_CONTACTO:STRING, '
		'CIUDAD__UF_:STRING, '
		'MEDIO_DE_PAGO__AE_:STRING, '
		'ESTADO_PEDIDO_HANNA:STRING, '
		'ESTADO_PEDIDO_VITEX__Y_:STRING, '
		'VENTA_NETA:STRING, '
		'TIPOLOGIA:STRING, '
		'VENTA_CRUZADA:STRING, '
		'ID:STRING, '
		'ASESOR:STRING, '
		'VENTA_IVA:STRING, '
		'F1:STRING, '
		'F2:STRING, '
		'F3_CANTADA:STRING, '
		'FECHA_2:STRING, '
		'DIA_SEMANA:STRING, '
		'HORA:STRING, '
		'HORA_2:STRING, '
		'FECHA_FACTURA:STRING, '
		'MES:STRING, '
		'ANO:STRING '


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
				'FECHA_GESTION' : arrayCSV[0],
				'NUMERODEPEDIDO' : arrayCSV[1],
				'MEDIO_DE_CONTACTO' : arrayCSV[2],
				'CIUDAD__UF_' : arrayCSV[3],
				'MEDIO_DE_PAGO__AE_' : arrayCSV[4],
				'ESTADO_PEDIDO_HANNA' : arrayCSV[5],
				'ESTADO_PEDIDO_VITEX__Y_' : arrayCSV[6],
				'VENTA_NETA' : arrayCSV[7],
				'TIPOLOGIA' : arrayCSV[8],
				'VENTA_CRUZADA' : arrayCSV[9],
				'ID' : arrayCSV[10],
				'ASESOR' : arrayCSV[11],
				'VENTA_IVA' : arrayCSV[12],
				'F1' : arrayCSV[13],
				'F2' : arrayCSV[14],
				'F3_CANTADA' : arrayCSV[15],
				'FECHA_2' : arrayCSV[16],
				'DIA_SEMANA' : arrayCSV[17],
				'HORA' : arrayCSV[18],
				'HORA_2' : arrayCSV[19],
				'FECHA_FACTURA' : arrayCSV[20],
				'MES' : arrayCSV[21],
				'ANO' : arrayCSV[22]






				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-dispersion" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery base' >> beam.io.WriteToBigQuery(
		gcs_project + ":Hermeco.historico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



