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
	'Zona:STRING, '
	'Codigo_de_ciudad:STRING, '
	'Cedula:STRING, '
	'Codigo_interno:STRING, '
	'Tipo_compradora:STRING, '
	'Customer_class:STRING, '
	'Cupo:INTEGER, '
	'Numero_de_obligacion:STRING, '
	'Valor_factura:STRING, '
	'Fecha_factura:STRING, '
	'Fecha_vencimiento:STRING, '
	'Valor_saldo_en_cartera:STRING, '
	'Dias_de_Vencimiento:STRING, '
	'Campana_original:STRING, '
	'Ultima_Campana:STRING, '
	'Codigo:STRING, '
	'Nombre:STRING, '
	'Apellidos:STRING, '
	'Telefono:STRING, '
	'Celular:STRING, '
	'Otro_contacto:STRING, '
	'Correo_electronico:STRING, '
	'Envio_de_sms:STRING, '
	'Envio_de_sms_de_voz:STRING, '
	'Envio_de_correo:STRING, '
	'Direccion:STRING, '
	'Barrio:STRING, '
	'Ciudad:STRING, '
	'Departamento:STRING '
)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'Zona' : arrayCSV[0].replace('"',''),
				'Codigo_de_ciudad' : arrayCSV[1].replace('"',''),
				'Cedula' : arrayCSV[2].replace('"',''),
				'Codigo_interno' : arrayCSV[3].replace('"',''),
				'Tipo_compradora' : arrayCSV[4].replace('"',''),
				'Customer_class' : arrayCSV[5].replace('"',''),
				'Cupo' : arrayCSV[6].replace('"',''),
				'Numero_de_obligacion' : arrayCSV[7].replace('"',''),
				'Valor_factura' : arrayCSV[8].replace('"',''),
				'Fecha_factura' : arrayCSV[9].replace('"',''),
				'Fecha_vencimiento' : arrayCSV[10].replace('"',''),
				'Valor_saldo_en_cartera' : arrayCSV[11].replace('"',''),
				'Dias_de_Vencimiento' : arrayCSV[12].replace('"',''),
				'Campana_original' : arrayCSV[13].replace('"',''),
				'Ultima_Campana' : arrayCSV[14].replace('"',''),
				'Codigo' : arrayCSV[15].replace('"',''),
				'Nombre' : arrayCSV[16].replace('"',''),
				'Apellidos' : arrayCSV[17].replace('"',''),
				'Telefono' : arrayCSV[18].replace('"',''),
				'Celular' : arrayCSV[19].replace('"',''),
				'Otro_contacto' : arrayCSV[20].replace('"',''),
				'Correo_electronico' : arrayCSV[21].replace('"',''),
				'Envio_de_sms' : arrayCSV[22].replace('"',''),
				'Envio_de_sms_de_voz' : arrayCSV[23].replace('"',''),
				'Envio_de_correo' : arrayCSV[24].replace('"',''),
				'Direccion' : arrayCSV[25].replace('"',''),
				'Barrio' : arrayCSV[26].replace('"',''),
				'Ciudad' : arrayCSV[27].replace('"',''),
				'Departamento' : arrayCSV[28].replace('"','')
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-leonisa" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runer = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runer, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5",
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

	transformed | 'Escritura a BigQuery leonisa' >> beam.io.WriteToBigQuery(
		gcs_project + ":leonisa.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



