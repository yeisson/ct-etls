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
    'IDKEY:STRING, ' 
	'FECHA:STRING, ' 
	'CEDULA_TECNICO:STRING, '
	'NOMBRE_TECNICO:STRING, '
	'CEDULA_ASESOR:STRING, '
	'NOMBRE_ASESOR:STRING, '
	'NOMBRE_TEAM_LEADER:STRING, '
	'NOMBRE_EJECUTIVO:STRING, '
	'PRODUCTO:STRING, '
	'FECHA_ASEGURAMIENTO:STRING, '
	'FECHA_Y_HORA_AUDIO:STRING, '
	'FECHA_AUDIO:STRING, '
	'ASPECTOS_MEJORAR:STRING, '
	'ASPECTOS_POSITIVOS:STRING, '
	'OBSERVACIONES:STRING, '
	'ID_CALL__IPDIAL_:STRING, '
	'DOC_CLIENTE:STRING, '
	'TELEFONO_CLIENTE:STRING, '
	'TIPIFICACION:STRING, '
	'VERBATIM:STRING, '
	'RESPUESTA_CORREO:STRING, '
	'A_QUE_SE_COMPROMETIO:STRING, '
	'EC_REQUISITO_DE_NEGOCIO_RESERVA_BANCARIA_POLITICA_DE_SEGURIDAD_DE_INFORMACION:STRING, '
	'EC_REQUISITO_DE_NEGOCIO_REALIZA_LA_MEDICION_DE_INTENCION_DE_PAGO:STRING, '
	'EC_REQUISITO_DE_NEGOCIO_CODIFICA_DE_FORMA_COMPLETA_EN_LOS_APLICATIVOS:STRING, '
	'EC_REQUISITO_DE_NEGOCIO_LA_DOCUMENTACION_ES_CLARA_Y_COHERENTE_EN_LOS_APLICATIVOS:STRING, '
	'EC_REQUISITO_DE_NEGOCIO_USA_EL_ESQUEMA_DE_MANEJO_DE_OBJECIONES:STRING, '
	'EC_REQUISITO_DE_NEGOCIO_GENERA_CIERRE_DEL_ACUERDO_O_ALTERNATIVA:STRING, '
	'ENC_HABILIDADES_BLANDAS_DEMUESTRA_DURANTE_TODA_LA_INTERACCION_CERCANIA_Y_COMPRENSION_POR_LA_SITUACION_DEL_CLIENTE:STRING, '
	'PEC:STRING, '
	'PENC:STRING '




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
				'CEDULA_TECNICO' : arrayCSV[0],
				'NOMBRE_TECNICO' : arrayCSV[1],
				'CEDULA_ASESOR' : arrayCSV[2],
				'NOMBRE_ASESOR' : arrayCSV[3],
				'NOMBRE_TEAM_LEADER' : arrayCSV[4],
				'NOMBRE_EJECUTIVO' : arrayCSV[5],
				'PRODUCTO' : arrayCSV[6],
				'FECHA_ASEGURAMIENTO' : arrayCSV[7],
				'FECHA_Y_HORA_AUDIO' : arrayCSV[8],
				'FECHA_AUDIO' : arrayCSV[9],
				'ASPECTOS_MEJORAR' : arrayCSV[10],
				'ASPECTOS_POSITIVOS' : arrayCSV[11],
				'OBSERVACIONES' : arrayCSV[12],
				'ID_CALL__IPDIAL_' : arrayCSV[13],
				'DOC_CLIENTE' : arrayCSV[14],
				'TELEFONO_CLIENTE' : arrayCSV[15],
				'TIPIFICACION' : arrayCSV[16],
				'VERBATIM' : arrayCSV[17],
				'RESPUESTA_CORREO' : arrayCSV[18],
				'A_QUE_SE_COMPROMETIO' : arrayCSV[19],
				'EC_REQUISITO_DE_NEGOCIO_RESERVA_BANCARIA_POLITICA_DE_SEGURIDAD_DE_INFORMACION' : arrayCSV[20],
				'EC_REQUISITO_DE_NEGOCIO_REALIZA_LA_MEDICION_DE_INTENCION_DE_PAGO' : arrayCSV[21],
				'EC_REQUISITO_DE_NEGOCIO_CODIFICA_DE_FORMA_COMPLETA_EN_LOS_APLICATIVOS' : arrayCSV[22],
				'EC_REQUISITO_DE_NEGOCIO_LA_DOCUMENTACION_ES_CLARA_Y_COHERENTE_EN_LOS_APLICATIVOS' : arrayCSV[23],
				'EC_REQUISITO_DE_NEGOCIO_USA_EL_ESQUEMA_DE_MANEJO_DE_OBJECIONES' : arrayCSV[24],
				'EC_REQUISITO_DE_NEGOCIO_GENERA_CIERRE_DEL_ACUERDO_O_ALTERNATIVA' : arrayCSV[25],
				'ENC_HABILIDADES_BLANDAS_DEMUESTRA_DURANTE_TODA_LA_INTERACCION_CERCANIA_Y_COMPRENSION_POR_LA_SITUACION_DEL_CLIENTE' : arrayCSV[26],
				'PEC' : arrayCSV[27],
				'PENC' : arrayCSV[28]



				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-sensus" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Sensus_Estrategia' >> beam.io.WriteToBigQuery(
		gcs_project + ":sensus.estrategy", 
		schema=TABLE_SCHEMA, 	
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")