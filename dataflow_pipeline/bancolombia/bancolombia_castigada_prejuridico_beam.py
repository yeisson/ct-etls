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
	'CONSECUTIVO_DOCUMENTO_DEUDOR:STRING, '
	'VALOR_CUOTA:STRING, '
	'NIT:STRING, '
	'NOMBRES:STRING, '
	'NUMERO_DOCUMENTO:STRING, '
	'TIPO_PRODUCTO:STRING, '
	'FECHA_ACTUALIZACION_PRIORIZACION:STRING, '
	'FECHA_PAGO_CUOTA:STRING, '
	'NOMBRE_DE_PRODUCTO:STRING, '
	'FECHA_DE_PERFECCIONAMIENTO:STRING, '
	'FECHA_VENCIMIENTO_DEF:STRING, '
	'NUMERO_CUOTAS:STRING, '
	'CUOTAS_EN_MORA:STRING, '
	'DIA_DE_VENCIMIENTO_DE_CUOTA:STRING, '
	'VALOR_OBLIGACION:STRING, '
	'VALOR_VENCIDO:STRING, '
	'SALDO_ACTIVO:STRING, '
	'SALDO_ORDEN:STRING, '
	'REGIONAL:STRING, '
	'CIUDAD:STRING, '
	'GRABADOR:STRING, '
	'CODIGO_AGENTE:STRING, '
	'NOMBRE_ASESOR:STRING, '
	'CODIGO_ABOGADO:STRING, '
	'NOMBRE_ABOGADO:STRING, '
	'FECHA_ULTIMA_GESTION_PREJURIDICA:STRING, '
	'ULTIMO_CODIGO_DE_GESTION_PARALELO:STRING, '
	'ULTIMO_CODIGO_DE_GESTION_PREJURIDICO:STRING, '
	'DESCRIPCION_SUBSECTOR:STRING, '
	'DESCRIPCION_CODIGO_SEGMENTO:STRING, '
	'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO:STRING, '
	'DESCRIPCION_SUBSEGMENTO:STRING, '
	'DESCRIPCION_SECTOR:STRING, '
	'DESCRIPCION_CODIGO_CIIU:STRING, '
	'CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO:STRING, '
	'DESC_CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO:STRING, '
	'FECHA_ULTIMA_GESTION_JURIDICA:STRING, '
	'ULTIMA_FECHA_DE_ACTUACION_JURIDICA:STRING, '
	'ULTIMA_FECHA_PAGO:STRING, '
	'EJEC_ULTIMO_CODIGO_DE_GESTION_JURIDICO:STRING, '
	'DESC_ULTIMO_CODIGO_DE_GESTION_JURIDICO:STRING, '
	'CANT_OBLIG:STRING, '
	'CLUSTER_PERSONA:STRING, '
	'DIAS_MORA:STRING, '
	'PAIS_RESIDENCIA:STRING, '
	'TIPO_DE_CARTERA:STRING, '
	'CALIFICACION:STRING, '
	'RADICACION:STRING, '
	'ESTADO_DE_LA_OBLIGACION:STRING, '
	'FONDO_NACIONAL_GARANTIAS:STRING, '
	'REGION:STRING, '
	'SEGMENTO:STRING, '
	'CODIGO_SEGMENTO:STRING, '
	'FECHA_IMPORTACION:STRING, '
	'NIVEL_DE_RIESGO:STRING, '
	'FECHA_ULTIMA_FACTURACION:STRING, '
	'SUBSEGMENTO:STRING, '
	'TITULAR_UNIVERSAL:STRING, '
	'NEGOCIO_TITUTULARIZADO:STRING, '
	'SECTOR_ECONOMICO:STRING, '
	'PROFESION:STRING, '
	'CAUSAL:STRING, '
	'OCUPACION:STRING, '
	'CUADRANTE:STRING, '
	'FECHA_TRASLADO_PARA_COBRO:STRING, '
	'DESC_CODIGO_DE_GESTION_VISITA:STRING, '
	'FECHA_GRABACION_VISITA:STRING, '
	'ENDEUDAMIENTO:STRING, '
	'CALIFICACION_REAL:STRING, '
	'FECHA_PROMESA:STRING, '
	'RED:STRING, '
	'ESTADO_NEGOCIACION:STRING, '
	'TIPO_CLIENTE_SUFI:STRING, '
	'CLASE:STRING, '
	'FRANQUICIA:STRING, '
	'SALDO_CAPITAL_PESOS:STRING, '
	'SALDO_INTERESES_PESOS:STRING, '
	'PROBABILIDAD_DE_PROPENSION_DE_PAGO:STRING, '
	'PRIORIZACION_FINAL:STRING, '
	'PRIORIZACION_POR_CLIENTE:STRING, '
	'GRUPO_DE_PRIORIZACION:STRING '
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
				'CONSECUTIVO_DOCUMENTO_DEUDOR' : arrayCSV[0],
				'VALOR_CUOTA' : arrayCSV[1],
				'NIT' : arrayCSV[2],
				'NOMBRES' : arrayCSV[3],
				'NUMERO_DOCUMENTO' : arrayCSV[4],
				'TIPO_PRODUCTO' : arrayCSV[5],
				'FECHA_ACTUALIZACION_PRIORIZACION' : arrayCSV[6],
				'FECHA_PAGO_CUOTA' : arrayCSV[7],
				'NOMBRE_DE_PRODUCTO' : arrayCSV[8],
				'FECHA_DE_PERFECCIONAMIENTO' : arrayCSV[9],
				'FECHA_VENCIMIENTO_DEF' : arrayCSV[10],
				'NUMERO_CUOTAS' : arrayCSV[11],
				'CUOTAS_EN_MORA' : arrayCSV[12],
				'DIA_DE_VENCIMIENTO_DE_CUOTA' : arrayCSV[13],
				'VALOR_OBLIGACION' : arrayCSV[14],
				'VALOR_VENCIDO' : arrayCSV[15],
				'SALDO_ACTIVO' : arrayCSV[16],
				'SALDO_ORDEN' : arrayCSV[17],
				'REGIONAL' : arrayCSV[18],
				'CIUDAD' : arrayCSV[19],
				'GRABADOR' : arrayCSV[20],
				'CODIGO_AGENTE' : arrayCSV[21],
				'NOMBRE_ASESOR' : arrayCSV[22],
				'CODIGO_ABOGADO' : arrayCSV[23],
				'NOMBRE_ABOGADO' : arrayCSV[24],
				'FECHA_ULTIMA_GESTION_PREJURIDICA' : arrayCSV[25],
				'ULTIMO_CODIGO_DE_GESTION_PARALELO' : arrayCSV[26],
				'ULTIMO_CODIGO_DE_GESTION_PREJURIDICO' : arrayCSV[27],
				'DESCRIPCION_SUBSECTOR' : arrayCSV[28],
				'DESCRIPCION_CODIGO_SEGMENTO' : arrayCSV[29],
				'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO' : arrayCSV[30],
				'DESCRIPCION_SUBSEGMENTO' : arrayCSV[31],
				'DESCRIPCION_SECTOR' : arrayCSV[32],
				'DESCRIPCION_CODIGO_CIIU' : arrayCSV[33],
				'CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO' : arrayCSV[34],
				'DESC_CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO' : arrayCSV[35],
				'FECHA_ULTIMA_GESTION_JURIDICA' : arrayCSV[36],
				'ULTIMA_FECHA_DE_ACTUACION_JURIDICA' : arrayCSV[37],
				'ULTIMA_FECHA_PAGO' : arrayCSV[38],
				'EJEC_ULTIMO_CODIGO_DE_GESTION_JURIDICO' : arrayCSV[39],
				'DESC_ULTIMO_CODIGO_DE_GESTION_JURIDICO' : arrayCSV[40],
				'CANT_OBLIG' : arrayCSV[41],
				'CLUSTER_PERSONA' : arrayCSV[42],
				'DIAS_MORA' : arrayCSV[43],
				'PAIS_RESIDENCIA' : arrayCSV[44],
				'TIPO_DE_CARTERA' : arrayCSV[45],
				'CALIFICACION' : arrayCSV[46],
				'RADICACION' : arrayCSV[47],
				'ESTADO_DE_LA_OBLIGACION' : arrayCSV[48],
				'FONDO_NACIONAL_GARANTIAS' : arrayCSV[49],
				'REGION' : arrayCSV[50],
				'SEGMENTO' : arrayCSV[51],
				'CODIGO_SEGMENTO' : arrayCSV[52],
				'FECHA_IMPORTACION' : arrayCSV[53],
				'NIVEL_DE_RIESGO' : arrayCSV[54],
				'FECHA_ULTIMA_FACTURACION' : arrayCSV[55],
				'SUBSEGMENTO' : arrayCSV[56],
				'TITULAR_UNIVERSAL' : arrayCSV[57],
				'NEGOCIO_TITUTULARIZADO' : arrayCSV[58],
				'SECTOR_ECONOMICO' : arrayCSV[59],
				'PROFESION' : arrayCSV[60],
				'CAUSAL' : arrayCSV[61],
				'OCUPACION' : arrayCSV[62],
				'CUADRANTE' : arrayCSV[63],
				'FECHA_TRASLADO_PARA_COBRO' : arrayCSV[64],
				'DESC_CODIGO_DE_GESTION_VISITA' : arrayCSV[65],
				'FECHA_GRABACION_VISITA' : arrayCSV[66],
				'ENDEUDAMIENTO' : arrayCSV[67],
				'CALIFICACION_REAL' : arrayCSV[68],
				'FECHA_PROMESA' : arrayCSV[69],
				'RED' : arrayCSV[70],
				'ESTADO_NEGOCIACION' : arrayCSV[71],
				'TIPO_CLIENTE_SUFI' : arrayCSV[72],
				'CLASE' : arrayCSV[73],
				'FRANQUICIA' : arrayCSV[74],
				'SALDO_CAPITAL_PESOS' : arrayCSV[75],
				'SALDO_INTERESES_PESOS' : arrayCSV[76],
				'PROBABILIDAD_DE_PROPENSION_DE_PAGO' : arrayCSV[77],
				'PRIORIZACION_FINAL' : arrayCSV[78],
				'PRIORIZACION_POR_CLIENTE' : arrayCSV[79],
				'GRUPO_DE_PRIORIZACION' : arrayCSV[80]
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_castigada.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



