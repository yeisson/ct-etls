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
'NUMERO_OBLIGACION:STRING, '
'CEDULA_DE_LA_ASESORA:STRING, '
'NOMBRE_DE_LA_ASESORA:STRING, '
'CLASIFICACION_DE_LA_ASESORA:STRING, '
'ESTADO:STRING, '
'NUMERO_DE_CREDITOS:STRING, '
'DIAS_VENCIDOS:STRING, '
'TIPO_PRODUCTO:STRING, '
'FECHA_TRASLADO:STRING, '
'ULTIMO_CODIGO_GESTION:STRING, '
'CODIGO_GESTION_ANTERIOR:STRING, '
'CAUSAL_NO_PAGO:STRING, '
'DESCRIPCULTIMO_CODIGO_GEST:STRING, '
'DESCRIPCION_CAUSAL:STRING, '
'RESPONSABLE_DE_COBRO:STRING, '
'GRABADOR:STRING, '
'SALDO_TOTAL:STRING, '
'SALDO_ACTUAL:STRING, '
'DESCRIPCION_CODIGO_GESTION_ANTERIOR:STRING, '
'TIENE_COMPROMISO:STRING, '
'CUPO_ULTIMA_FACTURA:STRING, '
'CUPO_PROXIMA_FACTURA:STRING, '
'TIPO_CARTERA_LINEA_DIRECTA:STRING, '
'RESULTADOS:STRING, '
'FECHA_DE_VENCIMIENTO:STRING, '
'FECHA_INGRESO:STRING, '
'FECHA_ULTIMA_GESTION:STRING, '
'FECHA_PROMESA:STRING, '
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
'CIUDAD:STRING, '
'PROYECCION_CASTIGO:STRING, '
'ANO_CASTIGO:STRING, '
'DESCRIPCION_TRASLADO:STRING, '
'DIAS_TRASLADO:STRING '
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
				'NUMERO_OBLIGACION' : arrayCSV[0].replace('"',''),
				'CEDULA_DE_LA_ASESORA' : arrayCSV[1].replace('"',''),
				'NOMBRE_DE_LA_ASESORA' : arrayCSV[2].replace('"',''),
				'CLASIFICACION_DE_LA_ASESORA' : arrayCSV[3].replace('"',''),
				'ESTADO' : arrayCSV[4].replace('"',''),
				'NUMERO_DE_CREDITOS' : arrayCSV[5].replace('"',''),
				'DIAS_VENCIDOS' : arrayCSV[6].replace('"',''),
				'TIPO_PRODUCTO' : arrayCSV[7].replace('"',''),
				'FECHA_TRASLADO' : arrayCSV[8].replace('"',''),
				'ULTIMO_CODIGO_GESTION' : arrayCSV[9].replace('"',''),
				'CODIGO_GESTION_ANTERIOR' : arrayCSV[10].replace('"',''),
				'CAUSAL_NO_PAGO' : arrayCSV[11].replace('"',''),
				'DESCRIPCULTIMO_CODIGO_GEST' : arrayCSV[12].replace('"',''),
				'DESCRIPCION_CAUSAL' : arrayCSV[13].replace('"',''),
				'RESPONSABLE_DE_COBRO' : arrayCSV[14].replace('"',''),
				'GRABADOR' : arrayCSV[15].replace('"',''),
				'SALDO_TOTAL' : arrayCSV[16].replace('"',''),
				'SALDO_ACTUAL' : arrayCSV[17].replace('"',''),
				'DESCRIPCION_CODIGO_GESTION_ANTERIOR' : arrayCSV[18].replace('"',''),
				'TIENE_COMPROMISO' : arrayCSV[19].replace('"',''),
				'CUPO_ULTIMA_FACTURA' : arrayCSV[20].replace('"',''),
				'CUPO_PROXIMA_FACTURA' : arrayCSV[21].replace('"',''),
				'TIPO_CARTERA_LINEA_DIRECTA' : arrayCSV[22].replace('"',''),
				'RESULTADOS' : arrayCSV[23].replace('"',''),
				'FECHA_DE_VENCIMIENTO' : arrayCSV[24].replace('"',''),
				'FECHA_INGRESO' : arrayCSV[25].replace('"',''),
				'FECHA_ULTIMA_GESTION' : arrayCSV[26].replace('"',''),
				'FECHA_PROMESA' : arrayCSV[27].replace('"',''),
				'FECHA_CREACION' : arrayCSV[28].replace('"',''),
				'FECHA_ACTUALIZACION' : arrayCSV[29].replace('"',''),
				'FECHA_DE_IMPORTACION' : arrayCSV[30].replace('"',''),
				'NUMERO_TRASLADO' : arrayCSV[31].replace('"',''),
				'USUARIO_RESPONSABLE' : arrayCSV[32].replace('"',''),
				'TAREA_DE_USUARIO' : arrayCSV[33].replace('"',''),
				'DESCRIPCION_TAREA' : arrayCSV[34].replace('"',''),
				'CODIGO_DE_LA_EMPRESA' : arrayCSV[35].replace('"',''),
				'CODIGO_DE_BLOQUEO' : arrayCSV[36].replace('"',''),
				'CONSECUTIVO_DOCUMENTO_DEUDOR' : arrayCSV[37].replace('"',''),
				'DETALLE_CARTERA' : arrayCSV[38].replace('"',''),
				'ESTADO_STENCIL' : arrayCSV[39].replace('"',''),
				'TIPO_PERSONA' : arrayCSV[40].replace('"',''),
				'CAMPANA_INGRESO' : arrayCSV[41].replace('"',''),
				'SECCION' : arrayCSV[42].replace('"',''),
				'ULTIMA_CAMPANA_PEDIDO' : arrayCSV[43].replace('"',''),
				'ZONA' : arrayCSV[44].replace('"',''),
				'DIVISION' : arrayCSV[45].replace('"',''),
				'CAMPANA_FACTURA' : arrayCSV[46].replace('"',''),
				'CANTIDAD_GESTION_TOTAL' : arrayCSV[47].replace('"',''),
				'GEOREFERENCIADO' : arrayCSV[48].replace('"',''),
				'CIUDAD' : arrayCSV[49].replace('"',''),
				'PROYECCION_CASTIGO' : arrayCSV[50].replace('"',''),
				'ANO_CASTIGO' : arrayCSV[51].replace('"',''),
				'DESCRIPCION_TRASLADO' : arrayCSV[52].replace('"',''),
				'DIAS_TRASLADO' : arrayCSV[53].replace('"','')
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-linead" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery linead' >> beam.io.WriteToBigQuery(
		gcs_project + ":linea_directa.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



