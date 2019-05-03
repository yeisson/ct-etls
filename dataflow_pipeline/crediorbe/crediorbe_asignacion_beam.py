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
	'OBLIGACION:STRING, '
	'CEDULA:STRING, '
	'CLIENTE:STRING, '
	'PRODUCTO:STRING, '
	'SUBESTADO_MOROSIDAD:STRING, '
	'VALOR_DESEMBOLSO:STRING, '
	'VALOR_CUOTA:STRING, '
	'FECHA_DESEMBOLSO:STRING, '
	'DIAS_MORA:STRING, '
	'FECHA_PROXIMO_PAGO:STRING, '
	'FECHA_ULTIMO_PAGO:STRING, '
	'SALDO_INTERES_CORRIENTE:STRING, '
	'VALOR_MORA:STRING, '
	'VALOR_SALDO_CAPITAL:STRING, '
	'NOM_CIUDAD:STRING, '
	'DIRECCION_CORRESPONDENCIA:STRING, '
	'EMAIL:STRING, '
	'CUOTAS_PAGADAS:STRING, '
	'CUOTAS_SIN_PAGAR:STRING, '
	'CUOTAS_EN_MORA:STRING, '
	'DIA_CORTE:STRING, '
	'USUARIO_GESTIONM:STRING, '
	'TIPO_CONTACTOM:STRING, '
	'GESTION_CONM:STRING, '
	'INFORMACION_CONTACTOM:STRING, '
	'DOCUMENTO_CODEUDORM:STRING, '
	'NOMBRE_CODEUDORM:STRING, '
	'CELULAR_CODEUDORM:STRING, '
	'TELEFONO_CODEUDORM:STRING, '
	'CORREO_CODEUDOR:STRING, '
	'TIPO_CLIENTE:STRING, '
	'TELEFONO:STRING, '
	'CELULAR:STRING, '
	'PROFESION:STRING, '
	'EDAD:STRING, '
	'RESULTADO_GESTIONM:STRING, '
	'FECHA_GESTIONM:STRING, '
	'VALOR_ULTIMO_PAGO:STRING, '
	'FECHA_ULTIMOACUERDOM:STRING, '
	'ACUERDOPAGOM:STRING, '
	'PLACA:STRING, '
	'REFERENCIA:STRING '

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
				'OBLIGACION' : arrayCSV[0],
				'CEDULA' : arrayCSV[1],
				'CLIENTE' : arrayCSV[2],
				'PRODUCTO' : arrayCSV[3],
				'SUBESTADO_MOROSIDAD' : arrayCSV[4],
				'VALOR_DESEMBOLSO' : arrayCSV[5],
				'VALOR_CUOTA' : arrayCSV[6],
				'FECHA_DESEMBOLSO' : arrayCSV[7],
				'DIAS_MORA' : arrayCSV[8],
				'FECHA_PROXIMO_PAGO' : arrayCSV[9],
				'FECHA_ULTIMO_PAGO' : arrayCSV[10],
				'SALDO_INTERES_CORRIENTE' : arrayCSV[11],
				'VALOR_MORA' : arrayCSV[12],
				'VALOR_SALDO_CAPITAL' : arrayCSV[13],
				'NOM_CIUDAD' : arrayCSV[14],
				'DIRECCION_CORRESPONDENCIA' : arrayCSV[15],
				'EMAIL' : arrayCSV[16],
				'CUOTAS_PAGADAS' : arrayCSV[17],
				'CUOTAS_SIN_PAGAR' : arrayCSV[18],
				'CUOTAS_EN_MORA' : arrayCSV[19],
				'DIA_CORTE' : arrayCSV[20],
				'USUARIO_GESTIONM' : arrayCSV[21],
				'TIPO_CONTACTOM' : arrayCSV[22],
				'GESTION_CONM' : arrayCSV[23],
				'INFORMACION_CONTACTOM' : arrayCSV[24],
				'DOCUMENTO_CODEUDORM' : arrayCSV[25],
				'NOMBRE_CODEUDORM' : arrayCSV[26],
				'CELULAR_CODEUDORM' : arrayCSV[27],
				'TELEFONO_CODEUDORM' : arrayCSV[28],
				'CORREO_CODEUDOR' : arrayCSV[29],
				'TIPO_CLIENTE' : arrayCSV[30],
				'TELEFONO' : arrayCSV[31],
				'CELULAR' : arrayCSV[32],
				'PROFESION' : arrayCSV[33],
				'EDAD' : arrayCSV[34],
				'RESULTADO_GESTIONM' : arrayCSV[35],
				'FECHA_GESTIONM' : arrayCSV[36],
				'VALOR_ULTIMO_PAGO' : arrayCSV[37],
				'FECHA_ULTIMOACUERDOM' : arrayCSV[38],
				'ACUERDOPAGOM' : arrayCSV[39],
				'PLACA' : arrayCSV[40],
				'REFERENCIA' : arrayCSV[41]

				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-crediorbe" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery crediorbe' >> beam.io.WriteToBigQuery(
		gcs_project + ":crediorbe.asignacion", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



