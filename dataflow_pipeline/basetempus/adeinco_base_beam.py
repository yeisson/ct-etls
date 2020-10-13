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
		'CONSECUTIVO_OBLIGACION:STRING, '
		'NO_OBLIGACION:STRING, '
		'FECHA_INICIAL_OBLIGACION:STRING, '
		'NRO_IDENTIFICACION:STRING, '
		'NOMBRE_CLIENTE:STRING, '
		'CARGO:STRING, '
		'INGRESOS:STRING, '
		'CUOTA_TOTAL:STRING, '
		'CODIGO_ACTIVIDAD_ECONOMICA:STRING, '
		'ACTIVIDAD_ECONOMICA:STRING, '
		'VALOR_CREDITO:STRING, '
		'CUOTA_INICIAL:STRING, '
		'VALOR_DE_LA_CUOTA_ACTUAL:STRING, '
		'VALOR_PROXIMA_CUOTA:STRING, '
		'SEGURO_ROBO:STRING, '
		'SALDO_TOTAL_DEUDA:STRING, '
		'TARJETA_PAGO:STRING, '
		'SALDO_CAPITAL:STRING, '
		'NRO_CUOTAS:STRING, '
		'NO_CUOTAS_PAGADAS:STRING, '
		'NRO_DE_CUOTAS_POR_PAGAR:STRING, '
		'NO_CUOTAS_MORA:STRING, '
		'DIAS_DE_MORA:STRING, '
		'FECHA_CUOTA_MAS_VENCIDA:STRING, '
		'CANTIDAD_VECES_MORA:STRING, '
		'FECHA_ULTIMO_PAGO:STRING, '
		'TASA_INTERES_ACTUAL:STRING, '
		'SALDO_TOTAL_VENCIDO:STRING, '
		'NRO_CREDITO_ORIGEN:STRING, '
		'CODIGO_ORIGEN_CREDITO:STRING, '
		'ORIGEN_CREDITO:STRING, '
		'TASA_INTERESES_MORA:STRING, '
		'VALOR_INTERES_MORA:STRING, '
		'CAPITAL_POR_VENCER:STRING, '
		'TOTAL_POR_VENCER:STRING, '
		'INTERES_CORRIENTE_VENCIDO:STRING, '
		'INTERES_CORRIENTE_POR_VENCER:STRING, '
		'GASTOS_PROCESALES:STRING, '
		'PROXIMA_FECHA_DE_VENCIMIENTO:STRING, '
		'FECHA_VENCIMIENTO_FINAL:STRING, '
		'SALDO_SEGUROS_VENCIDO:STRING, '
		'SALDO_CUOTAS_VENCIDAS:STRING, '
		'SEGURO_DEUDA_VENCIDO:STRING, '
		'SEGURO_DESEMPLEO_VENCIDO:STRING, '
		'SEGURO_ACCIDENTES_VENCIDO:STRING, '
		'TIPO_CARTERA:STRING, '
		'CODIGO_TIPO_CARTERA_ADEINCO:STRING, '
		'TIPO_CARTERA_ADEINCO:STRING, '
		'CODIGO_CLASIFICACION_CARTERA:STRING, '
		'CLASIFICACION_CARTERA:STRING, '
		'CODIGO_ZONA:STRING, '
		'ZONA:STRING, '
		'CODIGO_ENTIDAD_FINANCIERA:STRING, '
		'ENTIDAD_FINANCIERA:STRING, '
		'CODIGO_PLAN:STRING, '
		'PLAN:STRING, '
		'CODIGO_CONCESIONARIO:STRING, '
		'CONCESIONARIO:STRING, '
		'CODIGO_AGENCIA:STRING, '
		'RIESGO_HABITO_OBLIGACION:STRING, '
		'RIESGO_HABITO_CLIENTE:STRING, '
		'AGENCIA:STRING, '
		'CODIGO_TIPO_DE_NEGOCIO:STRING, '
		'TIPO_DE_NEGOCIO:STRING, '
		'CODIGO_CANAL:STRING, '
		'CANAL:STRING, '
		'MARCA:STRING, '
		'CODIGO_GESTION:STRING, '
		'CODIGO_GESTION_ANTERIOR:STRING, '
		'DESCRIPCION_CODIGO_GESTION:STRING, '
		'RESPONSABLE_DE_COBRO:STRING, '
		'ASESOR_COMERCIAL:STRING, '
		'ESTADO:STRING, '
		'GRABADOR:STRING, '
		'GESTOR_TAREA:STRING, '
		'NO_TAREA_USUARIO:STRING, '
		'FECHA_TAREA:STRING, '
		'USUARIO_ASIGNO:STRING, '
		'FECHA_TRASLADO:STRING, '
		'TIPO_TAREA:STRING, '
		'TIPO_ASIGNACION:STRING, '
		'DESCRIPCION_TAREA_USUARIO:STRING, '
		'FECHA_CREACION:STRING, '
		'FECHA_ACTUALIZACION:STRING, '
		'FECHA_DE_IMPORTACION:STRING, '
		'FECHA_ULTIMA_GESTION:STRING, '
		'NUMERO_TRASLADO:STRING, '
		'SEGUROS_DE_ACCIDENTE:STRING, '
		'SEGUROS_DE_DESEMPLEO:STRING, '
		'NOMBRE_CIUDAD:STRING, '
		'TOTAL_DE_SEGUROS:STRING, '
		'TOTAL_CUOTA_1:STRING, '
		'TOTAL_CUOTA_MAS_SEGUROS:STRING '




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
				'CONSECUTIVO_OBLIGACION' : arrayCSV[0],
				'NO_OBLIGACION' : arrayCSV[1],
				'FECHA_INICIAL_OBLIGACION' : arrayCSV[2],
				'NRO_IDENTIFICACION' : arrayCSV[3],
				'NOMBRE_CLIENTE' : arrayCSV[4],
				'CARGO' : arrayCSV[5],
				'INGRESOS' : arrayCSV[6],
				'CUOTA_TOTAL' : arrayCSV[7],
				'CODIGO_ACTIVIDAD_ECONOMICA' : arrayCSV[8],
				'ACTIVIDAD_ECONOMICA' : arrayCSV[9],
				'VALOR_CREDITO' : arrayCSV[10],
				'CUOTA_INICIAL' : arrayCSV[11],
				'VALOR_DE_LA_CUOTA_ACTUAL' : arrayCSV[12],
				'VALOR_PROXIMA_CUOTA' : arrayCSV[13],
				'SEGURO_ROBO' : arrayCSV[14],
				'SALDO_TOTAL_DEUDA' : arrayCSV[15],
				'TARJETA_PAGO' : arrayCSV[16],
				'SALDO_CAPITAL' : arrayCSV[17],
				'NRO_CUOTAS' : arrayCSV[18],
				'NO_CUOTAS_PAGADAS' : arrayCSV[19],
				'NRO_DE_CUOTAS_POR_PAGAR' : arrayCSV[20],
				'NO_CUOTAS_MORA' : arrayCSV[21],
				'DIAS_DE_MORA' : arrayCSV[22],
				'FECHA_CUOTA_MAS_VENCIDA' : arrayCSV[23],
				'CANTIDAD_VECES_MORA' : arrayCSV[24],
				'FECHA_ULTIMO_PAGO' : arrayCSV[25],
				'TASA_INTERES_ACTUAL' : arrayCSV[26],
				'SALDO_TOTAL_VENCIDO' : arrayCSV[27],
				'NRO_CREDITO_ORIGEN' : arrayCSV[28],
				'CODIGO_ORIGEN_CREDITO' : arrayCSV[29],
				'ORIGEN_CREDITO' : arrayCSV[30],
				'TASA_INTERESES_MORA' : arrayCSV[31],
				'VALOR_INTERES_MORA' : arrayCSV[32],
				'CAPITAL_POR_VENCER' : arrayCSV[33],
				'TOTAL_POR_VENCER' : arrayCSV[34],
				'INTERES_CORRIENTE_VENCIDO' : arrayCSV[35],
				'INTERES_CORRIENTE_POR_VENCER' : arrayCSV[36],
				'GASTOS_PROCESALES' : arrayCSV[37],
				'PROXIMA_FECHA_DE_VENCIMIENTO' : arrayCSV[38],
				'FECHA_VENCIMIENTO_FINAL' : arrayCSV[39],
				'SALDO_SEGUROS_VENCIDO' : arrayCSV[40],
				'SALDO_CUOTAS_VENCIDAS' : arrayCSV[41],
				'SEGURO_DEUDA_VENCIDO' : arrayCSV[42],
				'SEGURO_DESEMPLEO_VENCIDO' : arrayCSV[43],
				'SEGURO_ACCIDENTES_VENCIDO' : arrayCSV[44],
				'TIPO_CARTERA' : arrayCSV[45],
				'CODIGO_TIPO_CARTERA_ADEINCO' : arrayCSV[46],
				'TIPO_CARTERA_ADEINCO' : arrayCSV[47],
				'CODIGO_CLASIFICACION_CARTERA' : arrayCSV[48],
				'CLASIFICACION_CARTERA' : arrayCSV[49],
				'CODIGO_ZONA' : arrayCSV[50],
				'ZONA' : arrayCSV[51],
				'CODIGO_ENTIDAD_FINANCIERA' : arrayCSV[52],
				'ENTIDAD_FINANCIERA' : arrayCSV[53],
				'CODIGO_PLAN' : arrayCSV[54],
				'PLAN' : arrayCSV[55],
				'CODIGO_CONCESIONARIO' : arrayCSV[56],
				'CONCESIONARIO' : arrayCSV[57],
				'CODIGO_AGENCIA' : arrayCSV[58],
				'RIESGO_HABITO_OBLIGACION' : arrayCSV[59],
				'RIESGO_HABITO_CLIENTE' : arrayCSV[60],
				'AGENCIA' : arrayCSV[61],
				'CODIGO_TIPO_DE_NEGOCIO' : arrayCSV[62],
				'TIPO_DE_NEGOCIO' : arrayCSV[63],
				'CODIGO_CANAL' : arrayCSV[64],
				'CANAL' : arrayCSV[65],
				'MARCA' : arrayCSV[66],
				'CODIGO_GESTION' : arrayCSV[67],
				'CODIGO_GESTION_ANTERIOR' : arrayCSV[68],
				'DESCRIPCION_CODIGO_GESTION' : arrayCSV[69],
				'RESPONSABLE_DE_COBRO' : arrayCSV[70],
				'ASESOR_COMERCIAL' : arrayCSV[71],
				'ESTADO' : arrayCSV[72],
				'GRABADOR' : arrayCSV[73],
				'GESTOR_TAREA' : arrayCSV[74],
				'NO_TAREA_USUARIO' : arrayCSV[75],
				'FECHA_TAREA' : arrayCSV[76],
				'USUARIO_ASIGNO' : arrayCSV[77],
				'FECHA_TRASLADO' : arrayCSV[78],
				'TIPO_TAREA' : arrayCSV[79],
				'TIPO_ASIGNACION' : arrayCSV[80],
				'DESCRIPCION_TAREA_USUARIO' : arrayCSV[81],
				'FECHA_CREACION' : arrayCSV[82],
				'FECHA_ACTUALIZACION' : arrayCSV[83],
				'FECHA_DE_IMPORTACION' : arrayCSV[84],
				'FECHA_ULTIMA_GESTION' : arrayCSV[85],
				'NUMERO_TRASLADO' : arrayCSV[86],
				'SEGUROS_DE_ACCIDENTE' : arrayCSV[87],
				'SEGUROS_DE_DESEMPLEO' : arrayCSV[88],
				'NOMBRE_CIUDAD' : arrayCSV[89],
				'TOTAL_DE_SEGUROS' : arrayCSV[90],
				'TOTAL_CUOTA_1' : arrayCSV[91],
				'TOTAL_CUOTA_MAS_SEGUROS' : arrayCSV[92]













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

	transformed | 'Escritura a BigQuery Adeinco Adminfo' >> beam.io.WriteToBigQuery(
		gcs_project + ":unificadas.adeinco1", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



