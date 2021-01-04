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
	'ID_CC:STRING, '
	'DESCRIPCION:STRING, '
	'ID_GUIA:STRING, '
	'NOMBRE_GUIA:STRING, '
	'DOC_ASESOR:STRING, '
	'NOMBRE_ASESOR:STRING, '
	'DOC_LIDER:STRING, '
	'NOMBRE_TEAM_LEADER:STRING, '
	'DOC_EJECUTIVO:STRING, '
	'NOMBRE_EJECUTIVO:STRING, '
	'DOC_GERENTE:STRING, '
	'NOMBRE_GERENTE:STRING, '
	'DOC_ASEGURADOR:STRING, '
	'NOMBRE_QA:STRING, '
	'FECHA_AUDIO:STRING, '
	'HORA_AUDIO:STRING, '
	'ASPECTOS_MEJORAR:STRING, '
	'ASPECTOS_POSITIVOS:STRING, '
	'OBSERVACIONES:STRING, '
	'ID_CALL:STRING, '
	'DOC_CLIENTE:STRING, '
	'TELEFONO_CLIENTE:STRING, '
	'TIPIFICACION:STRING, '
	'FECHA_ASEGURAMIENTO:STRING, '
	'HORA_ASEGURAMIENTO:STRING, '
	'PEC:STRING, '
	'PENC:STRING, '
	'GOS:STRING, '
	'ID_RESULTADO:STRING, '
	'EC_REQUISITOS_DE_NEGOCIO__RESERVA_BANCARIA__POLITICA_DE_SEGURIDAD_DE_INFORMACION:STRING, '
	'EC_REQUISITOS_DE_NEGOCIO_INFORMACION_CORRECTA_Y_COMPLETA:STRING, '
	'EC_REQUISITOS_DE_NEGOCIO_ACTUALIZACION_DE_DATOS:STRING, '
	'EC_REQUISITOS_DE_NEGOCIO_SE_ADHIERE_A_LOS_LINEAMIENTOS_DE_ABANSA:STRING, '
	'EC_REQUISITOS_DE_NEGOCIO__CODIFICACION_COMPLETA_Y_CORRECTA_EN_TEK:STRING, '
	'EC_REQUISITOS_DE_NEGOCIO__CODIFICACION_COMPLETA_Y_CORRECTA_EN_TEMPUS:STRING, '
	'ENC_HABILIDADES_BLANDAS___USA_PREGUNTAS_ABIERTAS:STRING, '
	'ENC_HABILIDADES_BLANDAS___USA_TECNICA_ECCP:STRING, '
	'ENC_HABILIDADES_BLANDAS___USA_TECNICA_AIB:STRING, '
	'ENC_HABILIDADES_BLANDAS___REALIZA_ACUERDO_DE_ENTENDIMIENTO:STRING, '
	'ENC_HABILIDADES_BLANDAS___USA_TECNICAS_PARA_CAPTAR_LA_ATENCION:STRING, '
	'ENC_HABILIDADES_BLANDAS___ESCUCHA_Y_DEMUESTRA_ENTENDIMIENTO_DE_LA_SITUACION_ACTUAL_DE_CADA_CLIENTE:STRING, '
	'ENC_HABILIDADES_BLANDAS_REALIZA_UN_ACUERDO_GANA___GANA_Y_LO_RATIFICA:STRING, '
	'ENC_HABILIDADES_BLANDAS_DEMUESTRA_ADECUADA_COMUNICACION_VERBAL_Y_URBANIDAD_TONO_DE_VOZ__VOLUMEN_Y_DICCION:STRING, '
	'EC_EXPERIENCIA___EVITA_MALTRATO_AL_CLIENTE:STRING, '
	'ESTADO_ASEG:STRING '






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
				'ID_CC' : arrayCSV[0],
				'DESCRIPCION' : arrayCSV[1],
				'ID_GUIA' : arrayCSV[2],
				'NOMBRE_GUIA' : arrayCSV[3],
				'DOC_ASESOR' : arrayCSV[4],
				'NOMBRE_ASESOR' : arrayCSV[5],
				'DOC_LIDER' : arrayCSV[6],
				'NOMBRE_TEAM_LEADER' : arrayCSV[7],
				'DOC_EJECUTIVO' : arrayCSV[8],
				'NOMBRE_EJECUTIVO' : arrayCSV[9],
				'DOC_GERENTE' : arrayCSV[10],
				'NOMBRE_GERENTE' : arrayCSV[11],
				'DOC_ASEGURADOR' : arrayCSV[12],
				'NOMBRE_QA' : arrayCSV[13],
				'FECHA_AUDIO' : arrayCSV[14],
				'HORA_AUDIO' : arrayCSV[15],
				'ASPECTOS_MEJORAR' : arrayCSV[16],
				'ASPECTOS_POSITIVOS' : arrayCSV[17],
				'OBSERVACIONES' : arrayCSV[18],
				'ID_CALL' : arrayCSV[19],
				'DOC_CLIENTE' : arrayCSV[20],
				'TELEFONO_CLIENTE' : arrayCSV[21],
				'TIPIFICACION' : arrayCSV[22],
				'FECHA_ASEGURAMIENTO' : arrayCSV[23],
				'HORA_ASEGURAMIENTO' : arrayCSV[24],
				'PEC' : arrayCSV[25],
				'PENC' : arrayCSV[26],
				'GOS' : arrayCSV[27],
				'ID_RESULTADO' : arrayCSV[28],
				'EC_REQUISITOS_DE_NEGOCIO__RESERVA_BANCARIA__POLITICA_DE_SEGURIDAD_DE_INFORMACION' : arrayCSV[29],
				'EC_REQUISITOS_DE_NEGOCIO_INFORMACION_CORRECTA_Y_COMPLETA' : arrayCSV[30],
				'EC_REQUISITOS_DE_NEGOCIO_ACTUALIZACION_DE_DATOS' : arrayCSV[31],
				'EC_REQUISITOS_DE_NEGOCIO_SE_ADHIERE_A_LOS_LINEAMIENTOS_DE_ABANSA' : arrayCSV[32],
				'EC_REQUISITOS_DE_NEGOCIO__CODIFICACION_COMPLETA_Y_CORRECTA_EN_TEK' : arrayCSV[33],
				'EC_REQUISITOS_DE_NEGOCIO__CODIFICACION_COMPLETA_Y_CORRECTA_EN_TEMPUS' : arrayCSV[34],
				'ENC_HABILIDADES_BLANDAS___USA_PREGUNTAS_ABIERTAS' : arrayCSV[35],
				'ENC_HABILIDADES_BLANDAS___USA_TECNICA_ECCP' : arrayCSV[36],
				'ENC_HABILIDADES_BLANDAS___USA_TECNICA_AIB' : arrayCSV[37],
				'ENC_HABILIDADES_BLANDAS___REALIZA_ACUERDO_DE_ENTENDIMIENTO' : arrayCSV[38],
				'ENC_HABILIDADES_BLANDAS___USA_TECNICAS_PARA_CAPTAR_LA_ATENCION' : arrayCSV[39],
				'ENC_HABILIDADES_BLANDAS___ESCUCHA_Y_DEMUESTRA_ENTENDIMIENTO_DE_LA_SITUACION_ACTUAL_DE_CADA_CLIENTE' : arrayCSV[40],
				'ENC_HABILIDADES_BLANDAS_REALIZA_UN_ACUERDO_GANA___GANA_Y_LO_RATIFICA' : arrayCSV[41],
				'ENC_HABILIDADES_BLANDAS_DEMUESTRA_ADECUADA_COMUNICACION_VERBAL_Y_URBANIDAD_TONO_DE_VOZ__VOLUMEN_Y_DICCION' : arrayCSV[42],
				'EC_EXPERIENCIA___EVITA_MALTRATO_AL_CLIENTE' : arrayCSV[43],
				'ESTADO_ASEG' : arrayCSV[44]





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

	transformed | 'Escritura a BigQuery Sensus_Banco' >> beam.io.WriteToBigQuery(
		gcs_project + ":sensus.agricolacast", 
		schema=TABLE_SCHEMA, 	
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")