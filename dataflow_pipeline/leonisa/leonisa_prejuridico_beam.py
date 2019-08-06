# coding: utf-8 
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
	'ZONA:STRING, '
	'CODIGO_DE_CIUDAD:STRING, '
	'CEDULA_CIUDADANIA:STRING, '
	'CODIGO_INTERNO:STRING, '
	'TIPO_COMPRADORA:STRING, '
	'CUSTOMER_CLASS:STRING, '
	'CUPO:INTEGER, '
	'NUMERO_DE_OBLIGACION:STRING, '
	'VALOR_FACTURA:STRING, '
	'FECHA_FACTURA:STRING, '
	'FECHA_VENCIMIENTO:STRING, '
	'VALOR_SALDO_EN_CARTERA:STRING, '
	'DIAS_DE_VENCIMIENTO:STRING, '
	'CAMPANA_ORIGINAL:STRING, '
	'ULTIMA_CAMPANA:STRING, '
	'CODIGO:STRING, '
	'NOMBRE1:STRING, '
	'APELLIDOS:STRING, '
	'TELEFONO_1:STRING, '
	'CELULAR1:STRING, '
	'TEL_CEL_2:STRING, '
	'E_MAIL:STRING, '
	'AUTORIZO_ENVIO_DE_MENSAJES_DE_TEXTO_A_MI_CELULAR_SI_NO:STRING, '
	'AUTORIZO_CORREOS_DE_VOZ_A_MI_CELULAR_SI_NO:STRING, '
	'AUTORIZO_ENVIO_DE_E_MAIL_SI_NO:STRING, '
	'DIRECCION1:STRING, '
	'BARRIO1:STRING, '
	'CIUDAD1:STRING, '
	'DEPARTAMENTO1:STRING, '
	'DIRECCION2:STRING, '
	'BARRIO2:STRING, '
	'CIUDAD2:STRING, '
	'DEPARTAMENTO2:STRING, '
	'NOMBRE2:STRING, '
	'APELLIDO1:STRING, '
	'PARENTESCO1:STRING, '
	'CELULAR2:STRING, '
	'NOMBRE3:STRING, '
	'APELLIDO2:STRING, '
	'PARENTESCO2:STRING, '
	'TELEFONO2:STRING, '
	'CELULAR3:STRING, '
	'DIRECCION3:STRING, '
	'CIUDAD3:STRING, '
	'DEPARTAMENTO3:STRING, '
	'NOMBRE4:STRING, '
	'APELLIDO3:STRING, '
	'TELEFONO3:STRING, '
	'CELULAR4:STRING, '
	'DIRECCION4:STRING, '
	'CIUDAD4:STRING, '
	'DEPARTAMENTO4:STRING, '
	'NOMBRE5:STRING, '
	'APELLIDO4:STRING, '
	'DIRECCION5:STRING, '
	'TELEFONO4:STRING, '
	'CELULAR5:STRING, '
	'CIUDAD5:STRING, '
	'DEPARTAMENTO5:STRING, '
	'ABOGAD:STRING, '
	'DIVSION:STRING, '
	'PAIS:STRING, '
	'FECHA_DE_PROXIMA_CONFERENCIA:STRING, '
	'CODIGO_DE_GESTION:STRING, '
	'FECHA_DE_GESTION:STRING, '
	'FECHA_DE_PROMESA_DE_PAGO:STRING '
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
				'ZONA' : arrayCSV[0].replace('"',''),
				'CODIGO_DE_CIUDAD' : arrayCSV[1].replace('"',''),
				'CEDULA_CIUDADANIA' : arrayCSV[2].replace('"',''),
				'CODIGO_INTERNO' : arrayCSV[3].replace('"',''),
				'TIPO_COMPRADORA' : arrayCSV[4].replace('"',''),
				'CUSTOMER_CLASS' : arrayCSV[5].replace('"',''),
				'CUPO' : arrayCSV[6].replace('"',''),
				'NUMERO_DE_OBLIGACION' : arrayCSV[7].replace('"',''),
				'VALOR_FACTURA' : arrayCSV[8].replace('"',''),
				'FECHA_FACTURA' : arrayCSV[9].replace('"',''),
				'FECHA_VENCIMIENTO' : arrayCSV[10].replace('"',''),
				'VALOR_SALDO_EN_CARTERA' : arrayCSV[11].replace('"',''),
				'DIAS_DE_VENCIMIENTO' : arrayCSV[12].replace('"',''),
				'CAMPANA_ORIGINAL' : arrayCSV[13].replace('"',''),
				'ULTIMA_CAMPANA' : arrayCSV[14].replace('"',''),
				'CODIGO' : arrayCSV[15].replace('"',''),
				'NOMBRE1' : arrayCSV[16].replace('"',''),
				'APELLIDOS' : arrayCSV[17].replace('"',''),
				'TELEFONO_1' : arrayCSV[18].replace('"',''),
				'CELULAR1' : arrayCSV[19].replace('"',''),
				'TEL_CEL_2' : arrayCSV[20].replace('"',''),
				'E_MAIL' : arrayCSV[21].replace('"',''),
				'AUTORIZO_ENVIO_DE_MENSAJES_DE_TEXTO_A_MI_CELULAR_SI_NO' : arrayCSV[22].replace('"',''),
				'AUTORIZO_CORREOS_DE_VOZ_A_MI_CELULAR_SI_NO' : arrayCSV[23].replace('"',''),
				'AUTORIZO_ENVIO_DE_E_MAIL_SI_NO' : arrayCSV[24].replace('"',''),
				'DIRECCION1' : arrayCSV[25].replace('"',''),
				'BARRIO1' : arrayCSV[26].replace('"',''),
				'CIUDAD1' : arrayCSV[27].replace('"',''),
				'DEPARTAMENTO1' : arrayCSV[28].replace('"',''),
				'DIRECCION2' : arrayCSV[29].replace('"',''),
				'BARRIO2' : arrayCSV[30].replace('"',''),
				'CIUDAD2' : arrayCSV[31].replace('"',''),
				'DEPARTAMENTO2' : arrayCSV[32].replace('"',''),
				'NOMBRE2' : arrayCSV[33].replace('"',''),
				'APELLIDO1' : arrayCSV[34].replace('"',''),
				'PARENTESCO1' : arrayCSV[35].replace('"',''),
				'CELULAR2' : arrayCSV[36].replace('"',''),
				'NOMBRE3' : arrayCSV[37].replace('"',''),
				'APELLIDO2' : arrayCSV[38].replace('"',''),
				'PARENTESCO2' : arrayCSV[39].replace('"',''),
				'TELEFONO2' : arrayCSV[40].replace('"',''),
				'CELULAR3' : arrayCSV[41].replace('"',''),
				'DIRECCION3' : arrayCSV[42].replace('"',''),
				'CIUDAD3' : arrayCSV[43].replace('"',''),
				'DEPARTAMENTO3' : arrayCSV[44].replace('"',''),
				'NOMBRE4' : arrayCSV[45].replace('"',''),
				'APELLIDO3' : arrayCSV[46].replace('"',''),
				'TELEFONO3' : arrayCSV[47].replace('"',''),
				'CELULAR4' : arrayCSV[48].replace('"',''),
				'DIRECCION4' : arrayCSV[49].replace('"',''),
				'CIUDAD4' : arrayCSV[50].replace('"',''),
				'DEPARTAMENTO4' : arrayCSV[51].replace('"',''),
				'NOMBRE5' : arrayCSV[52].replace('"',''),
				'APELLIDO4' : arrayCSV[53].replace('"',''),
				'DIRECCION5' : arrayCSV[54].replace('"',''),
				'TELEFONO4' : arrayCSV[55].replace('"',''),
				'CELULAR5' : arrayCSV[56].replace('"',''),
				'CIUDAD5' : arrayCSV[57].replace('"',''),
				'DEPARTAMENTO5' : arrayCSV[58].replace('"',''),
				'ABOGAD' : arrayCSV[59].replace('"',''),
				'DIVSION' : arrayCSV[60].replace('"',''),
				'PAIS' : arrayCSV[61].replace('"',''),
				'FECHA_DE_PROXIMA_CONFERENCIA' : arrayCSV[62].replace('"','')
				
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-leonisa" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery leonisa' >> beam.io.WriteToBigQuery(
		gcs_project + ":leonisa.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



