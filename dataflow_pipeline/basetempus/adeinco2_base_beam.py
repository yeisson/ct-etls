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
		'FECHA:STRING, '
		'NUMERO_DE_CREDITO:STRING, '
		'CONCESIONARIO:STRING, '
		'AGENCIA:STRING, '
		'TIPO_DEUDOR:STRING, '
		'NOMBRE_CLIENTE:STRING, '
		'TIPO_DOCUMENTO:STRING, '
		'NUMERO_DOCUMENTO:STRING, '
		'TIPO_PERSONA:STRING, '
		'ABOGADO:STRING, '
		'FECHA_DESEMBOLSO:STRING, '
		'TASA_MENSUAL_ORIGEN:STRING, '
		'TASA_EFECTIVA_ORIGEN:STRING, '
		'VALOR_PAGARE:STRING, '
		'VALOR_CUOTA_PAGARE:STRING, '
		'PLAZO_EN_MESES:STRING, '
		'NRO_PAGARE:STRING, '
		'SALDO_CAPITAL_VIGENTE:STRING, '
		'SALDO_INTERES_CTEVIGENTE:STRING, '
		'SALDO_SEGUROS_VIGENTE:STRING, '
		'INTERES_MORA:STRING, '
		'GASTOS_PROCESALES_VIGENTES:STRING, '
		'HONORARIOS_VIGENTE:STRING, '
		'TOTAL_DEUDA_CLIENTE:STRING, '
		'HONORARIOS_CARTERA_CASTIGADA:STRING, '
		'CASTIGADO:STRING, '
		'FECHA_CASTIGO:STRING, '
		'DIAS_MORA:STRING, '
		'FECHA_ULTIMO_PAGO_CART_ADMIN:STRING, '
		'FECHAS_TRASLADO_JURIDICO:STRING, '
		'REESTRUCTURACION:STRING, '
		'FECHA_ULT_REEST:STRING, '
		'DACION:STRING, '
		'FECHA_DACION:STRING, '
		'VALOR_DACION:STRING, '
		'REPORTADO_DATACREDITO:STRING, '
		'FECHA_ULT_REPORTE_DATACREDITO:STRING, '
		'PLACA:STRING, '
		'COLOR:STRING, '
		'TITULAR_PRENDARIO:STRING, '
		'DOC_TITULAR_PRENDARIO:STRING, '
		'MARCA:STRING, '
		'CANAL:STRING, '
		'ACTIVIDAD:STRING, '
		'TITULO:STRING, '
		'EMPRESA_LABORA:STRING, '
		'ANTIG_LABORAL:STRING, '
		'SALARIO:STRING, '
		'OTROS_INGRESOS:STRING, '
		'CONSECUTIVO:STRING, '
		'ESTADO_CIVIL:STRING, '
		'NACIMIENTO:STRING, '
		'GENERO:STRING, '
		'CARGO:STRING, '
		'VIVIENDA:STRING, '
		'RESIDENCIA_1:STRING, '
		'RESIDENCIA__2:STRING, '
		'EMAIL:STRING, '
		'EMPRESA:STRING, '
		'EMPRESA_2:STRING, '
		'NRO_REFERENCIA:STRING, '
		'REFERENCIA:STRING, '
		'CIUDAD_TITULAR:STRING, '
		'CIUDAD_CODEUDOR:STRING, '
		'INDI:STRING, '
		'DEPARTAMENTO_TITULAR:STRING, '
		'DEPARTAMENTO_CODEUDOR:STRING, '
		'NOMBRE_REFERENCIA:STRING, '
		'REFERENCIA_2:STRING, '
		'CELULAR:STRING, '
		'RESIDENCIA:STRING '





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
				'NUMERO_DE_CREDITO' : arrayCSV[0],
				'CONCESIONARIO' : arrayCSV[1],
				'AGENCIA' : arrayCSV[2],
				'TIPO_DEUDOR' : arrayCSV[3],
				'NOMBRE_CLIENTE' : arrayCSV[4],
				'TIPO_DOCUMENTO' : arrayCSV[5],
				'NUMERO_DOCUMENTO' : arrayCSV[6],
				'TIPO_PERSONA' : arrayCSV[7],
				'ABOGADO' : arrayCSV[8],
				'FECHA_DESEMBOLSO' : arrayCSV[9],
				'TASA_MENSUAL_ORIGEN' : arrayCSV[10],
				'TASA_EFECTIVA_ORIGEN' : arrayCSV[11],
				'VALOR_PAGARE' : arrayCSV[12],
				'VALOR_CUOTA_PAGARE' : arrayCSV[13],
				'PLAZO_EN_MESES' : arrayCSV[14],
				'NRO_PAGARE' : arrayCSV[15],
				'SALDO_CAPITAL_VIGENTE' : arrayCSV[16],
				'SALDO_INTERES_CTEVIGENTE' : arrayCSV[17],
				'SALDO_SEGUROS_VIGENTE' : arrayCSV[18],
				'INTERES_MORA' : arrayCSV[19],
				'GASTOS_PROCESALES_VIGENTES' : arrayCSV[20],
				'HONORARIOS_VIGENTE' : arrayCSV[21],
				'TOTAL_DEUDA_CLIENTE' : arrayCSV[22],
				'HONORARIOS_CARTERA_CASTIGADA' : arrayCSV[23],
				'CASTIGADO' : arrayCSV[24],
				'FECHA_CASTIGO' : arrayCSV[25],
				'DIAS_MORA' : arrayCSV[26],
				'FECHA_ULTIMO_PAGO_CART_ADMIN' : arrayCSV[27],
				'FECHAS_TRASLADO_JURIDICO' : arrayCSV[28],
				'REESTRUCTURACION' : arrayCSV[29],
				'FECHA_ULT_REEST' : arrayCSV[30],
				'DACION' : arrayCSV[31],
				'FECHA_DACION' : arrayCSV[32],
				'VALOR_DACION' : arrayCSV[33],
				'REPORTADO_DATACREDITO' : arrayCSV[34],
				'FECHA_ULT_REPORTE_DATACREDITO' : arrayCSV[35],
				'PLACA' : arrayCSV[36],
				'COLOR' : arrayCSV[37],
				'TITULAR_PRENDARIO' : arrayCSV[38],
				'DOC_TITULAR_PRENDARIO' : arrayCSV[39],
				'MARCA' : arrayCSV[40],
				'CANAL' : arrayCSV[41],
				'ACTIVIDAD' : arrayCSV[42],
				'TITULO' : arrayCSV[43],
				'EMPRESA_LABORA' : arrayCSV[44],
				'ANTIG_LABORAL' : arrayCSV[45],
				'SALARIO' : arrayCSV[46],
				'OTROS_INGRESOS' : arrayCSV[47],
				'CONSECUTIVO' : arrayCSV[48],
				'ESTADO_CIVIL' : arrayCSV[49],
				'NACIMIENTO' : arrayCSV[50],
				'GENERO' : arrayCSV[51],
				'CARGO' : arrayCSV[52],
				'VIVIENDA' : arrayCSV[53],
				'RESIDENCIA_1' : arrayCSV[54],
				'RESIDENCIA__2' : arrayCSV[55],
				'EMAIL' : arrayCSV[56],
				'EMPRESA' : arrayCSV[57],
				'EMPRESA_2' : arrayCSV[58],
				'NRO_REFERENCIA' : arrayCSV[59],
				'REFERENCIA' : arrayCSV[60],
				'CIUDAD_TITULAR' : arrayCSV[61],
				'CIUDAD_CODEUDOR' : arrayCSV[62],
				'INDI' : arrayCSV[63],
				'DEPARTAMENTO_TITULAR' : arrayCSV[64],
				'DEPARTAMENTO_CODEUDOR' : arrayCSV[65],
				'NOMBRE_REFERENCIA' : arrayCSV[66],
				'REFERENCIA_2' : arrayCSV[67],
				'CELULAR' : arrayCSV[68],
				'RESIDENCIA' : arrayCSV[69]













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

	transformed | 'Escritura a BigQuery Adeinco Asignaciones' >> beam.io.WriteToBigQuery(
		gcs_project + ":unificadas.adeinco2", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



