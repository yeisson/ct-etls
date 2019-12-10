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
'NRO_DE_SUSCRIPCION:STRING, '
'NRO_SERVICIO_SUSCRITO:STRING, '
'VALOR_PENDIENTE_ACTUAL:STRING, '
'VALOR_TOTAL_PENDIENTE:STRING, '
'CICLO:STRING, '
'MAXIMOS_DIAS_MORA:STRING, '
'DIAS_DE_MORA:STRING, '
'TELEFONO:STRING, '
'CELULAR:STRING, '
'TELEFONO2:STRING, '
'CELULAR2:STRING, '
'EMAIL1:STRING, '
'EMAIL:STRING, '
'DIAS_SIN_GESTION:STRING, '
'ULTIMO_CODIGO_GESTION:STRING, '
'FECHA_ULTIMA_GESTION:STRING, '
'DESCRIPCION_RESULTADO_ULTIMA_GESTION:STRING, '
'DESCRIPCION_CAUSAL:STRING, '
'GRABADOR:STRING, '
'CODIGO_PRODUCTO:STRING, '
'DESCRIPCION_PRODUCTO:STRING, '
'NOMBRE_DE_LA_EMPRESA:STRING, '
'CODIGO_ESTADO_DE_CORTE:STRING, '
'DESCRIPCION_ESTADO_DE_CORTE:STRING, '
'FINANCIADO:STRING, '
'SUSPENDIBLE:STRING, '
'ESTADO_INSCRIPCION_PATM:STRING, '
'NIVEL_RIESGO_BURO:STRING, '
'NIVEL_DE_RIESGO_CARTERA:STRING, '
'CODIGO_PLAN_DE_FACTURACION:STRING, '
'IDENTIFICACION_DEL_CLIENTE:STRING, '
'NOMBRE_CLIENTE:STRING, '
'IDENTIFICACION_SOMOS:STRING, '
'NOMBRE_SOMOS:STRING, '
'IDENTIFICACION_USUARIO_PAGA_TU_MEDIDA:STRING, '
'NOMBRES_PAGA_TU_MEDIDA:STRING, '
'DIRECCION:STRING, '
'NRO_INSTALACION:STRING, '
'DESCRIPCION_BARRIO:STRING, '
'DESCRIPCION_COMUNA:STRING, '
'DESCRIPCION_VEREDA:STRING, '
'DESCRIPCION_CORREGIMIENTO:STRING, '
'DESCRIPCION_CIUDAD:STRING, '
'DESCRIPCION_DEPARTAMENTO:STRING, '
'DESCRIPCION_REGION:STRING, '
'CONSECUTIVO_TRASLADO:STRING, '
'DESCRIPCION_TRASLADO:STRING, '
'LINEA_AGENCIA_ABOGADO:STRING, '
'CONSECUTIVO_SERVICIO:STRING, '
'DESCRIPCION_CATEGORIA:STRING, '
'DESCRIPCION_SUBCATEGORIA:STRING, '
'FECHA_DE_ASIGNACION:STRING, '
'FECHA_DE_VENCIMIENTO_SIN_RECARGO_MAS_ANTIGUA:STRING, '
'PUNTAJE_SCORING:STRING, '
'SEGMENTO:STRING, '
'CARTERA_CASTIGADA:STRING, '
'DESCRIPCION_DE_GESTION_RESULTADO_VISITA:STRING, '
'FECHA_VISITA:STRING, '
'TIENE_COMPROMISO_DE_PAGO:STRING, '
'DESCRIPCION_PLAN_DE_FACTURACION:STRING, '
'SUBSEGMENTO:STRING '
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
				'NRO_DE_SUSCRIPCION' : arrayCSV[0],
				'NRO_SERVICIO_SUSCRITO' : arrayCSV[1],
				'VALOR_PENDIENTE_ACTUAL' : arrayCSV[2],
				'VALOR_TOTAL_PENDIENTE' : arrayCSV[3],
				'CICLO' : arrayCSV[4],
				'MAXIMOS_DIAS_MORA' : arrayCSV[5],
				'DIAS_DE_MORA' : arrayCSV[6],
				'TELEFONO' : arrayCSV[7],
				'CELULAR' : arrayCSV[8],
				'TELEFONO2' : arrayCSV[9],
				'CELULAR2' : arrayCSV[10],
				'EMAIL1' : arrayCSV[11],
				'EMAIL' : arrayCSV[12],
				'DIAS_SIN_GESTION' : arrayCSV[13],
				'ULTIMO_CODIGO_GESTION' : arrayCSV[14],
				'FECHA_ULTIMA_GESTION' : arrayCSV[15],
				'DESCRIPCION_RESULTADO_ULTIMA_GESTION' : arrayCSV[16],
				'DESCRIPCION_CAUSAL' : arrayCSV[17],
				'GRABADOR' : arrayCSV[18],
				'CODIGO_PRODUCTO' : arrayCSV[19],
				'DESCRIPCION_PRODUCTO' : arrayCSV[20],
				'NOMBRE_DE_LA_EMPRESA' : arrayCSV[21],
				'CODIGO_ESTADO_DE_CORTE' : arrayCSV[22],
				'DESCRIPCION_ESTADO_DE_CORTE' : arrayCSV[23],
				'FINANCIADO' : arrayCSV[24],
				'SUSPENDIBLE' : arrayCSV[25],
				'ESTADO_INSCRIPCION_PATM' : arrayCSV[26],
				'NIVEL_RIESGO_BURO' : arrayCSV[27],
				'NIVEL_DE_RIESGO_CARTERA' : arrayCSV[28],
				'CODIGO_PLAN_DE_FACTURACION' : arrayCSV[29],
				'IDENTIFICACION_DEL_CLIENTE' : arrayCSV[30],
				'NOMBRE_CLIENTE' : arrayCSV[31],
				'IDENTIFICACION_SOMOS' : arrayCSV[32],
				'NOMBRE_SOMOS' : arrayCSV[33],
				'IDENTIFICACION_USUARIO_PAGA_TU_MEDIDA' : arrayCSV[34],
				'NOMBRES_PAGA_TU_MEDIDA' : arrayCSV[35],
				'DIRECCION' : arrayCSV[36],
				'NRO_INSTALACION' : arrayCSV[37],
				'DESCRIPCION_BARRIO' : arrayCSV[38],
				'DESCRIPCION_COMUNA' : arrayCSV[39],
				'DESCRIPCION_VEREDA' : arrayCSV[40],
				'DESCRIPCION_CORREGIMIENTO' : arrayCSV[41],
				'DESCRIPCION_CIUDAD' : arrayCSV[42],
				'DESCRIPCION_DEPARTAMENTO' : arrayCSV[43],
				'DESCRIPCION_REGION' : arrayCSV[44],
				'CONSECUTIVO_TRASLADO' : arrayCSV[45],
				'DESCRIPCION_TRASLADO' : arrayCSV[46],
				'LINEA_AGENCIA_ABOGADO' : arrayCSV[47],
				'CONSECUTIVO_SERVICIO' : arrayCSV[48],
				'DESCRIPCION_CATEGORIA' : arrayCSV[49],
				'DESCRIPCION_SUBCATEGORIA' : arrayCSV[50],
				'FECHA_DE_ASIGNACION' : arrayCSV[51],
				'FECHA_DE_VENCIMIENTO_SIN_RECARGO_MAS_ANTIGUA' : arrayCSV[52],
				'PUNTAJE_SCORING' : arrayCSV[53],
				'SEGMENTO' : arrayCSV[54],
				'CARTERA_CASTIGADA' : arrayCSV[55],
				'DESCRIPCION_DE_GESTION_RESULTADO_VISITA' : arrayCSV[56],
				'FECHA_VISITA' : arrayCSV[57],
				'TIENE_COMPROMISO_DE_PAGO' : arrayCSV[58],
				'DESCRIPCION_PLAN_DE_FACTURACION' : arrayCSV[59],
				'SUBSEGMENTO' : arrayCSV[60]
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
		gcs_project + ":epm.asignacion", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



