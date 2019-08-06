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
'NRO_IDENTIFICACION_CLIENTE:STRING, '
'NOMBRE_CLIENTE:STRING, '
'CONSECUTIVO_SERVICIO:STRING, '
'NRO_SERVICIO:STRING, '
'SALDO_CAPITAL:STRING, '
'SALDO_INTERESES_CORRIENTES_CAU:STRING, '
'SALDO_INTERESES_MORA_CAUSADOS:STRING, '
'GASTOS_JURIDICOS:STRING, '
'HONORARIOS:STRING, '
'SALDO_TOTAL:STRING, '
'VALOR_MORA:STRING, '
'DIAS_DE_MORA:STRING, '
'VALOR_DESEMBOLSO:STRING, '
'FECHA_DESEMBOLSO:STRING, '
'TIPO_CREDITO:STRING, '
'CUOTA:STRING, '
'CLASIFICACION:STRING, '
'FECHA_VENCIMIENTO:STRING, '
'ABOGADO:STRING, '
'ENTREGADA_ABOGADO:STRING, '
'ETAPA:STRING, '
'JUZGADO:STRING, '
'RADICADO:STRING, '
'FECHA1:STRING, '
'FECHA_ULTIMO_MOVIMIENTO:STRING, '
'FECHA_ULTIMA_CUOTA_PAGADA:STRING, '
'CONTACTO01:STRING, '
'CONTACTO02:STRING, '
'CONTACTO03:STRING, '
'CONTACTO04:STRING, '
'ACTIVIDAD:STRING, '
'CELULAR1:STRING, '
'CORREO:STRING, '
'DIRECCION:STRING, '
'PAIS:STRING, '
'DEPARTAMENTO:STRING, '
'CIUDAD:STRING, '
'BARRIO_VEREDA:STRING, '
'CODEUDOR01:STRING, '
'IDENTIFICACION01:STRING, '
'DIRECCION01:STRING, '
'PAIS01:STRING, '
'DEPTO01:STRING, '
'CIUDAD01:STRING, '
'BARRIO01:STRING, '
'CODEUDOR02:STRING, '
'IDENTIFICACION02:STRING, '
'DIRECCION02:STRING, '
'PAIS02:STRING, '
'DEPTO02:STRING, '
'CIUDAD02:STRING, '
'BARRIO02:STRING, '
'CODEUDOR03:STRING, '
'IDENTIFICACION03:STRING, '
'DIRECCION03:STRING, '
'PAIS03:STRING, '
'DEPTO03:STRING, '
'CIUDAD03:STRING, '
'BARRIO03:STRING, '
'CODEUDOR04:STRING, '
'IDENTIFICACION04:STRING, '
'DIRECCION04:STRING, '
'PAIS04:STRING, '
'DEPTO04:STRING, '
'CIUDAD04:STRING, '
'BARRIO04:STRING, '
'EMPRESA:STRING, '
'LINEA:STRING, '
'OFICINA:STRING, '
'CELULAR2:STRING, '
'TELEFONOS1:STRING, '
'TELEFONOS2:STRING, '
'TELEFONOS3:STRING '
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
				'NRO_IDENTIFICACION_CLIENTE' : arrayCSV[0].replace('"',''),
				'NOMBRE_CLIENTE' : arrayCSV[1].replace('"',''),
				'CONSECUTIVO_SERVICIO' : arrayCSV[2].replace('"',''),
				'NRO_SERVICIO' : arrayCSV[3].replace('"',''),
				'SALDO_CAPITAL' : arrayCSV[4].replace('"',''),
				'SALDO_INTERESES_CORRIENTES_CAU' : arrayCSV[5].replace('"',''),
				'SALDO_INTERESES_MORA_CAUSADOS' : arrayCSV[6].replace('"',''),
				'GASTOS_JURIDICOS' : arrayCSV[7].replace('"',''),
				'HONORARIOS' : arrayCSV[8].replace('"',''),
				'SALDO_TOTAL' : arrayCSV[9].replace('"',''),
				'VALOR_MORA' : arrayCSV[10].replace('"',''),
				'DIAS_DE_MORA' : arrayCSV[11].replace('"',''),
				'VALOR_DESEMBOLSO' : arrayCSV[12].replace('"',''),
				'FECHA_DESEMBOLSO' : arrayCSV[13].replace('"',''),
				'TIPO_CREDITO' : arrayCSV[14].replace('"',''),
				'CUOTA' : arrayCSV[15].replace('"',''),
				'CLASIFICACION' : arrayCSV[16].replace('"',''),
				'FECHA_VENCIMIENTO' : arrayCSV[17].replace('"',''),
				'ABOGADO' : arrayCSV[18].replace('"',''),
				'ENTREGADA_ABOGADO' : arrayCSV[19].replace('"',''),
				'ETAPA' : arrayCSV[20].replace('"',''),
				'JUZGADO' : arrayCSV[21].replace('"',''),
				'RADICADO' : arrayCSV[22].replace('"',''),
				'FECHA1' : arrayCSV[23].replace('"',''),
				'FECHA_ULTIMO_MOVIMIENTO' : arrayCSV[24].replace('"',''),
				'FECHA_ULTIMA_CUOTA_PAGADA' : arrayCSV[25].replace('"',''),
				'CONTACTO01' : arrayCSV[26].replace('"',''),
				'CONTACTO02' : arrayCSV[27].replace('"',''),
				'CONTACTO03' : arrayCSV[28].replace('"',''),
				'CONTACTO04' : arrayCSV[29].replace('"',''),
				'ACTIVIDAD' : arrayCSV[30].replace('"',''),
				'CELULAR1' : arrayCSV[31].replace('"',''),
				'CORREO' : arrayCSV[32].replace('"',''),
				'DIRECCION' : arrayCSV[33].replace('"',''),
				'PAIS' : arrayCSV[34].replace('"',''),
				'DEPARTAMENTO' : arrayCSV[35].replace('"',''),
				'CIUDAD' : arrayCSV[36].replace('"',''),
				'BARRIO_VEREDA' : arrayCSV[37].replace('"',''),
				'CODEUDOR01' : arrayCSV[38].replace('"',''),
				'IDENTIFICACION01' : arrayCSV[39].replace('"',''),
				'DIRECCION01' : arrayCSV[40].replace('"',''),
				'PAIS01' : arrayCSV[41].replace('"',''),
				'DEPTO01' : arrayCSV[42].replace('"',''),
				'CIUDAD01' : arrayCSV[43].replace('"',''),
				'BARRIO01' : arrayCSV[44].replace('"',''),
				'CODEUDOR02' : arrayCSV[45].replace('"',''),
				'IDENTIFICACION02' : arrayCSV[46].replace('"',''),
				'DIRECCION02' : arrayCSV[47].replace('"',''),
				'PAIS02' : arrayCSV[48].replace('"',''),
				'DEPTO02' : arrayCSV[49].replace('"',''),
				'CIUDAD02' : arrayCSV[50].replace('"',''),
				'BARRIO02' : arrayCSV[51].replace('"',''),
				'CODEUDOR03' : arrayCSV[52].replace('"',''),
				'IDENTIFICACION03' : arrayCSV[53].replace('"',''),
				'DIRECCION03' : arrayCSV[54].replace('"',''),
				'PAIS03' : arrayCSV[55].replace('"',''),
				'DEPTO03' : arrayCSV[56].replace('"',''),
				'CIUDAD03' : arrayCSV[57].replace('"',''),
				'BARRIO03' : arrayCSV[58].replace('"',''),
				'CODEUDOR04' : arrayCSV[59].replace('"',''),
				'IDENTIFICACION04' : arrayCSV[60].replace('"',''),
				'DIRECCION04' : arrayCSV[61].replace('"',''),
				'PAIS04' : arrayCSV[62].replace('"',''),
				'DEPTO04' : arrayCSV[63].replace('"',''),
				'CIUDAD04' : arrayCSV[64].replace('"',''),
				'BARRIO04' : arrayCSV[65].replace('"',''),
				'EMPRESA' : arrayCSV[66].replace('"',''),
				'LINEA' : arrayCSV[67].replace('"',''),
				'OFICINA' : arrayCSV[68].replace('"',''),
				'CELULAR2' : arrayCSV[69].replace('"',''),
				'TELEFONOS1' : arrayCSV[70].replace('"',''),
				'TELEFONOS2' : arrayCSV[71].replace('"',''),
				'TELEFONOS3' : arrayCSV[72].replace('"','')
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-cotrafa" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery cotrafa' >> beam.io.WriteToBigQuery(
		gcs_project + ":cotrafa.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



