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
		'NRO_CREDITO:STRING, '
		'NIT_CEDULA:STRING, '
		'RAZON_SOCIAL:STRING, '
		'NOMBRE_CLIENTE:STRING, '
		'TELEFONO:STRING, '
		'TIENDA:STRING, '
		'CELULAR:STRING, '
		'VALOR_A_COBRAR:STRING, '
		'VALOR_INCIAL_CREDITO:STRING, '
		'VALOR_A_COBRAR_TOTAL:STRING, '
		'RANGO_MORA_OBLIGACION:STRING, '
		'DIAS_SIN_TRAMITE_MAX:STRING, '
		'RANGO_MORA_CLIENTE:STRING, '
		'FECHA_DE_VENCIMIENTO:STRING, '
		'ANO_VENCIMIENTO:STRING, '
		'EDAD_DE_MORA_CLIENTE:STRING, '
		'EDAD_DE_MORA:STRING, '
		'DIAS_SIN_TRAMITE:STRING, '
		'CIUDAD_DEL_CLIENTE:STRING, '
		'RESULTADO_DEL_TRAMITE:STRING, '
		'FECHA_ULTIMO_PAGO:STRING, '
		'ASIGNACION_DE_USUARIOS:STRING, '
		'GESTOR_ASIGNADO:STRING, '
		'CON_EMAIL:STRING, '
		'CON_TELEFONO:STRING, '
		'CON_DIRECCION:STRING, '
		'CON_REFERENCIA:STRING, '
		'TIPIFICACION:STRING, '
		'ULT_RESULTADO_EFECTIVO:STRING, '
		'ULT_RESULTADO_EFECTIVO__FECHA_:STRING, '
		'ANO_VENCI_OBLIG__RANGO_:STRING, '
		'ANO_VENCI_CLIENTE_RANGO_:STRING, '
		'ESTADO_DE_CARTERA:STRING, '
		'ANO_ORIGINACION:STRING, '
		'ANO_ORIG_OBLIG__RANGO_:STRING, '
		'ANO_ORIG_CLIENTE__RANGO_:STRING, '
		'CREDITOS_EN_MORA:STRING, '
		'FECHA_PROX_RECORDATORIO:STRING, '
		'FECHA_DE_ASIGNACION:STRING, '
		'ANO_VENCIMIENTO_2:STRING, '
		'CAPITAL_INICIAL:STRING, '
		'TOTAL_CAPITAL_INICIAL:STRING, '
		'GESTOR_ULTIMA_GESTION:STRING, '
		'FECHA_ULT_SMS:STRING, '
		'EXONERAR_INTERESES:STRING, '
		'FECHA_EXONERAR_INTERESES:STRING, '
		'CON_CELULAR:STRING, '
		'EMPRESA_QUE_GESTIONA:STRING, '
		'CUOTA_VENCIDA:STRING, '
		'TOTAL_CUOTAS_VENCIDAS:STRING, '
		'CUOTAS_EN_MORA:STRING, '
		'USUARIO_RESPONSABLE:STRING, '
		'VALOR_INTERESES:STRING, '
		'EMPRESA_QUE_REPORTA:STRING, '
		'VALOR_AVAL:STRING, '
		'VALOR_CUOTA:STRING, '
		'VALOR_ABONOS:STRING, '
		'CIUDAD_PUNTO_DE_CREDITO:STRING, '
		'FECHA_EMPRESA_REPORTA:STRING, '
		'ESTADO_DE_LA_CUOTA:STRING, '
		'EMPRESA_ORIGEN:STRING, '
		'INTERESES_MORA:STRING, '
		'TOTAL_INTERESES_MORA:STRING, '
		'TOTAL_HONORARIOS:STRING, '
		'SALDO_CAPITAL:STRING, '
		'VALCAPITALMAX:STRING, '
		'TIPO_DE_CREDITO:STRING, '
		'EMAILS:STRING, '
		'NUMERO_DE_CREDITO:STRING, '
		'FECHA_ACUERDO:STRING, '
		'TELEFONO_1:STRING, '
		'TELEFONO_2:STRING, '
		'TELEFONO_3:STRING, '
		'TELEFONO_4:STRING, '
		'TELEFONO_5:STRING, '
		'TELEFONO_6:STRING, '
		'TELEFONO_7:STRING '


		




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
				'NRO_CREDITO' : arrayCSV[0],
				'NIT_CEDULA' : arrayCSV[1],
				'RAZON_SOCIAL' : arrayCSV[2],
				'NOMBRE_CLIENTE' : arrayCSV[3],
				'TELEFONO' : arrayCSV[4],
				'TIENDA' : arrayCSV[5],
				'CELULAR' : arrayCSV[6],
				'VALOR_A_COBRAR' : arrayCSV[7],
				'VALOR_INCIAL_CREDITO' : arrayCSV[8],
				'VALOR_A_COBRAR_TOTAL' : arrayCSV[9],
				'RANGO_MORA_OBLIGACION' : arrayCSV[10],
				'DIAS_SIN_TRAMITE_MAX' : arrayCSV[11],
				'RANGO_MORA_CLIENTE' : arrayCSV[12],
				'FECHA_DE_VENCIMIENTO' : arrayCSV[13],
				'ANO_VENCIMIENTO' : arrayCSV[14],
				'EDAD_DE_MORA_CLIENTE' : arrayCSV[15],
				'EDAD_DE_MORA' : arrayCSV[16],
				'DIAS_SIN_TRAMITE' : arrayCSV[17],
				'CIUDAD_DEL_CLIENTE' : arrayCSV[18],
				'RESULTADO_DEL_TRAMITE' : arrayCSV[19],
				'FECHA_ULTIMO_PAGO' : arrayCSV[20],
				'ASIGNACION_DE_USUARIOS' : arrayCSV[21],
				'GESTOR_ASIGNADO' : arrayCSV[22],
				'CON_EMAIL' : arrayCSV[23],
				'CON_TELEFONO' : arrayCSV[24],
				'CON_DIRECCION' : arrayCSV[25],
				'CON_REFERENCIA' : arrayCSV[26],
				'TIPIFICACION' : arrayCSV[27],
				'ULT_RESULTADO_EFECTIVO' : arrayCSV[28],
				'ULT_RESULTADO_EFECTIVO__FECHA_' : arrayCSV[29],
				'ANO_VENCI_OBLIG__RANGO_' : arrayCSV[30],
				'ANO_VENCI_CLIENTE_RANGO_' : arrayCSV[31],
				'ESTADO_DE_CARTERA' : arrayCSV[32],
				'ANO_ORIGINACION' : arrayCSV[33],
				'ANO_ORIG_OBLIG__RANGO_' : arrayCSV[34],
				'ANO_ORIG_CLIENTE__RANGO_' : arrayCSV[35],
				'CREDITOS_EN_MORA' : arrayCSV[36],
				'FECHA_PROX_RECORDATORIO' : arrayCSV[37],
				'FECHA_DE_ASIGNACION' : arrayCSV[38],
				'ANO_VENCIMIENTO_2' : arrayCSV[39],
				'CAPITAL_INICIAL' : arrayCSV[40],
				'TOTAL_CAPITAL_INICIAL' : arrayCSV[41],
				'GESTOR_ULTIMA_GESTION' : arrayCSV[42],
				'FECHA_ULT_SMS' : arrayCSV[43],
				'EXONERAR_INTERESES' : arrayCSV[44],
				'FECHA_EXONERAR_INTERESES' : arrayCSV[45],
				'CON_CELULAR' : arrayCSV[46],
				'EMPRESA_QUE_GESTIONA' : arrayCSV[47],
				'CUOTA_VENCIDA' : arrayCSV[48],
				'TOTAL_CUOTAS_VENCIDAS' : arrayCSV[49],
				'CUOTAS_EN_MORA' : arrayCSV[50],
				'USUARIO_RESPONSABLE' : arrayCSV[51],
				'VALOR_INTERESES' : arrayCSV[52],
				'EMPRESA_QUE_REPORTA' : arrayCSV[53],
				'VALOR_AVAL' : arrayCSV[54],
				'VALOR_CUOTA' : arrayCSV[55],
				'VALOR_ABONOS' : arrayCSV[56],
				'CIUDAD_PUNTO_DE_CREDITO' : arrayCSV[57],
				'FECHA_EMPRESA_REPORTA' : arrayCSV[58],
				'ESTADO_DE_LA_CUOTA' : arrayCSV[59],
				'EMPRESA_ORIGEN' : arrayCSV[60],
				'INTERESES_MORA' : arrayCSV[61],
				'TOTAL_INTERESES_MORA' : arrayCSV[62],
				'TOTAL_HONORARIOS' : arrayCSV[63],
				'SALDO_CAPITAL' : arrayCSV[64],
				'VALCAPITALMAX' : arrayCSV[65],
				'TIPO_DE_CREDITO' : arrayCSV[66],
				'EMAILS' : arrayCSV[67],
				'NUMERO_DE_CREDITO' : arrayCSV[68],
				'FECHA_ACUERDO' : arrayCSV[69],
				'TELEFONO_1' : arrayCSV[70],
				'TELEFONO_2' : arrayCSV[71],
				'TELEFONO_3' : arrayCSV[72],
				'TELEFONO_4' : arrayCSV[73],
				'TELEFONO_5' : arrayCSV[74],
				'TELEFONO_6' : arrayCSV[75],
				'TELEFONO_7' : arrayCSV[76]



			
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
		gcs_project + ":unificadas.aval1", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



