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
'CONSECUTIVO_SERVICIO:STRING, '
'CONSECUTIVO_PAGO:STRING, '
'CICLO:STRING, '
'CODIGO_PRODUCTO:STRING, '
'CODIGO_ENTIDAD_RECAUDADORA:STRING, '
'DESCRIPCION_ENTIDAD_RECAUDADORA:STRING, '
'NRO_CUENTA_DE_COBRO_CANCELADAS:STRING, '
'NRO_CUPON_DE_PAGO:STRING, '
'NRO_FACTURA:STRING, '
'FECHA_GRABACION_EN_ADMINFO:STRING, '
'FECHA_GRABACION_ORIGEN:STRING, '
'FECHA_PAGO:STRING, '
'NRO_SERVICIO_SUSCRITO:STRING, '
'NRO_SUSCRIPCION:STRING, '
'VALOR_PAGADO_CUPON:STRING, '
'VALOR_PAGADO_SERVICIO_SUSCRITO:STRING, '
'CODIGO_TRASLADO:STRING, '
'TIPO_DOCUMENTO_PAGO:STRING, '
'DESCRIPCION_PRODUCTO:STRING, '
'LINEA_AGENCIA_ABOGADO:STRING '
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
				'fecha' : self.mifecha,
				'CONSECUTIVO_SERVICIO' : arrayCSV[0],
				'CONSECUTIVO_PAGO' : arrayCSV[1],
				'CICLO' : arrayCSV[2],
				'CODIGO_PRODUCTO' : arrayCSV[3],
				'CODIGO_ENTIDAD_RECAUDADORA' : arrayCSV[4],
				'DESCRIPCION_ENTIDAD_RECAUDADORA' : arrayCSV[5],
				'NRO_CUENTA_DE_COBRO_CANCELADAS' : arrayCSV[6],
				'NRO_CUPON_DE_PAGO' : arrayCSV[7],
				'NRO_FACTURA' : arrayCSV[8],
				'FECHA_GRABACION_EN_ADMINFO' : arrayCSV[9],
				'FECHA_GRABACION_ORIGEN' : arrayCSV[10],
				'FECHA_PAGO' : arrayCSV[11],
				'NRO_SERVICIO_SUSCRITO' : arrayCSV[12],
				'NRO_SUSCRIPCION' : arrayCSV[13],
				'VALOR_PAGADO_CUPON' : arrayCSV[14],
				'VALOR_PAGADO_SERVICIO_SUSCRITO' : arrayCSV[15],
				'CODIGO_TRASLADO' : arrayCSV[16],
				'TIPO_DOCUMENTO_PAGO' : arrayCSV[17],
				'DESCRIPCION_PRODUCTO' : arrayCSV[18],
				'LINEA_AGENCIA_ABOGADO' : arrayCSV[19]
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-epm" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery epm' >> beam.io.WriteToBigQuery(
		gcs_project + ":epm.pagos", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



