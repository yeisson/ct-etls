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
		'NUMERO_OBLIGACION:STRING, '
		'CEDULA_DE_LA_ASESORA:STRING, '
		'NOMBRE_DE_LA_ASESORA:STRING, '
		'CLASIFICACION_DE_LA_ASESORA:STRING, '
		'ESTADO:STRING, '
		'NUMERO_DE_CREDITOS:STRING, '
		'DIAS_VENCIDOS:STRING, '
		'TIPO_PRODUCTO:STRING, '
		'FECHA_TRASLADO:STRING, '
		'CODIGO_GESTION_ANTERIOR:STRING, '
		'ULTIMO_CODIGO_GESTION:STRING, '
		'CAUSAL_NO_PAGO:STRING, '
		'DESCRIPCULTIMO_CODIGO_GEST:STRING, '
		'DESCRIPCION_CAUSAL:STRING, '
		'RESPONSABLE_DE_COBRO:STRING, '
		'GRABADOR:STRING, '
		'SALDO_TOTAL:STRING, '
		'SALDO_ACTUAL:STRING, '
		'DESCRIPCION_CODIGO_GESTION_ANTERIOR:STRING, '
		'CUPO_ULTIMA_FACTURA:STRING, '
		'TIENE_COMPROMISO:STRING, '
		'CUPO_PROXIMA_FACTURA:STRING, '
		'TIPO_CARTERA_LINEA_DIRECTA:STRING, '
		'RESULTADOS:STRING, '
		'FECHA_DE_VENCIMIENTO:STRING, '
		'FECHA_INGRESO:STRING, '
		'FECHA_PROMESA:STRING, '
		'FECHA_ULTIMA_GESTION:STRING, '
		'FECHA_CREACION:STRING, '
		'FECHA_ACTUALIZACION:STRING, '
		'FECHA_DE_IMPORTACION:STRING, '
		'NUMERO_TRASLADO:STRING, '
		'USUARIO_RESPONSABLE:STRING, '
		'TAREA_DE_USUARIO:STRING, '
		'DESCRIPCION_TAREA:STRING, '
		'CODIGO_DE_LA_EMPRESA:STRING, '
		'CODIGO_DE_BLOQUEO:STRING, '
		'CONSECUTIVO_DOCUMENTO_DEUDOR:STRING, '
		'DETALLE_CARTERA:STRING, '
		'ESTADO_STENCIL:STRING, '
		'TIPO_PERSONA:STRING, '
		'CAMPANA_INGRESO:STRING, '
		'SECCION:STRING, '
		'ULTIMA_CAMPANA_PEDIDO:STRING, '
		'ZONA:STRING, '
		'DIVISION:STRING, '
		'CAMPANA_FACTURA:STRING, '
		'CANTIDAD_GESTION_TOTAL:STRING, '
		'GEOREFERENCIADO:STRING, '
		'DEPARTAMENTO:STRING, '
		'CIUDAD:STRING, '
		'CIUDAD2:STRING, '
		'PROYECCION_CASTIGO:STRING, '
		'ANO_CASTIGO:STRING, '
		'DESCRIPCION_TRASLADO:STRING, '
		'DIAS_TRASLADO:STRING, '
		'CANTIDAD_GESTIONES_MES:STRING '


		




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
				'NUMERO_OBLIGACION' : arrayCSV[0],
				'CEDULA_DE_LA_ASESORA' : arrayCSV[1],
				'NOMBRE_DE_LA_ASESORA' : arrayCSV[2],
				'CLASIFICACION_DE_LA_ASESORA' : arrayCSV[3],
				'ESTADO' : arrayCSV[4],
				'NUMERO_DE_CREDITOS' : arrayCSV[5],
				'DIAS_VENCIDOS' : arrayCSV[6],
				'TIPO_PRODUCTO' : arrayCSV[7],
				'FECHA_TRASLADO' : arrayCSV[8],
				'CODIGO_GESTION_ANTERIOR' : arrayCSV[9],
				'ULTIMO_CODIGO_GESTION' : arrayCSV[10],
				'CAUSAL_NO_PAGO' : arrayCSV[11],
				'DESCRIPCULTIMO_CODIGO_GEST' : arrayCSV[12],
				'DESCRIPCION_CAUSAL' : arrayCSV[13],
				'RESPONSABLE_DE_COBRO' : arrayCSV[14],
				'GRABADOR' : arrayCSV[15],
				'SALDO_TOTAL' : arrayCSV[16],
				'SALDO_ACTUAL' : arrayCSV[17],
				'DESCRIPCION_CODIGO_GESTION_ANTERIOR' : arrayCSV[18],
				'CUPO_ULTIMA_FACTURA' : arrayCSV[19],
				'TIENE_COMPROMISO' : arrayCSV[20],
				'CUPO_PROXIMA_FACTURA' : arrayCSV[21],
				'TIPO_CARTERA_LINEA_DIRECTA' : arrayCSV[22],
				'RESULTADOS' : arrayCSV[23],
				'FECHA_DE_VENCIMIENTO' : arrayCSV[24],
				'FECHA_INGRESO' : arrayCSV[25],
				'FECHA_PROMESA' : arrayCSV[26],
				'FECHA_ULTIMA_GESTION' : arrayCSV[27],
				'FECHA_CREACION' : arrayCSV[28],
				'FECHA_ACTUALIZACION' : arrayCSV[29],
				'FECHA_DE_IMPORTACION' : arrayCSV[30],
				'NUMERO_TRASLADO' : arrayCSV[31],
				'USUARIO_RESPONSABLE' : arrayCSV[32],
				'TAREA_DE_USUARIO' : arrayCSV[33],
				'DESCRIPCION_TAREA' : arrayCSV[34],
				'CODIGO_DE_LA_EMPRESA' : arrayCSV[35],
				'CODIGO_DE_BLOQUEO' : arrayCSV[36],
				'CONSECUTIVO_DOCUMENTO_DEUDOR' : arrayCSV[37],
				'DETALLE_CARTERA' : arrayCSV[38],
				'ESTADO_STENCIL' : arrayCSV[39],
				'TIPO_PERSONA' : arrayCSV[40],
				'CAMPANA_INGRESO' : arrayCSV[41],
				'SECCION' : arrayCSV[42],
				'ULTIMA_CAMPANA_PEDIDO' : arrayCSV[43],
				'ZONA' : arrayCSV[44],
				'DIVISION' : arrayCSV[45],
				'CAMPANA_FACTURA' : arrayCSV[46],
				'CANTIDAD_GESTION_TOTAL' : arrayCSV[47],
				'GEOREFERENCIADO' : arrayCSV[48],
				'DEPARTAMENTO' : arrayCSV[49],
				'CIUDAD' : arrayCSV[50],
				'CIUDAD2' : arrayCSV[51],
				'PROYECCION_CASTIGO' : arrayCSV[52],
				'ANO_CASTIGO' : arrayCSV[53],
				'DESCRIPCION_TRASLADO' : arrayCSV[54],
				'DIAS_TRASLADO' : arrayCSV[55],
				'CANTIDAD_GESTIONES_MES' : arrayCSV[56]


			
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

	transformed | 'Escritura a BigQuery Telfonos Cod' >> beam.io.WriteToBigQuery(
		gcs_project + ":unificadas.prejul", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



