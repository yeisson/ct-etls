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
		'ENC_HABILIDADES_BLANDAS_REALIZA_EL_SALUDO_O_INICIO_DE_LA_CONVERSACION_DE_MANERA_ADECUADA_Y_CORDIAL__QUE_SEA_CERCANO:STRING, '
		'EC_REQUISITOS_DE_NEGOCIO_POLITICA_DE_SEGURIDAD_DE_LA_INFORMACION:STRING, '
		'ENC_HABILIDADES_BLANDAS_SE_EXPRESA_DE_MANERA_CLARA_PRECISA_Y_CONCRETA:STRING, '
		'EC_REQUISITOS_DE_NEGOCIO_REALIZA_UN_CORRECTO_ESCALAMIENTO__SI_APLICA_DE_ACUERDO_A_LA_SOLICITUD_DEL_CLIENTE:STRING, '
		'EC_REQUISITOS_DE_NEGOCIO_INVITA_AL_CLIENTE_A_CALIFICAR_LA_ATENCION_RECIBIDA:STRING, '
		'EC_REQUISITOS_DE_NEGOCIO_SE_DESPIDE_DE_MANERA_CORDIAL:STRING, '
		'EC_REQUISITOS_DE_NEGOCIO_LOS_TIEMPOS_DE_RESPUESTA_SON_ACORDES_A_LA_SOLICITUD:STRING, '
		'EC_REQUISITOS_DE_NEGOCIO_USA_DE_MANERA_ADECUADA_LAS_RESPUESTAS_PREDETERMINADAS:STRING, '
		'EC_REQUISITOS_DE_NEGOCIO_REDACTA_USANDO_UNA_ORTOGRAFIA_IMPECABLE:STRING, '
		'EC_REQUISITOS_DE_NEGOCIO_CODIFICACION_COMPLETA_Y_CORRECTA_EN_TODOS_LOS_APLICATIVOS:STRING, '
		'EC_EXPERIENCIA_FINALIZA_LA_LLAMADA_VALIDANDO_LA_SATISFACCION_CON_LA_SOLUCION_ENTREGADA_E_INDAGA_SI_PUEDE_AYUDARLE_EN_ALGO_MAS:STRING, '
		'EC_EXPERIENCIA_EVITA_MALTRATO_AL_CLIENTE:STRING, '
		'ENC_HABILIDADES_BLANDAS_DEMUESTRA_EMPATIA_Y_COHERENCIA_EN_LA_RESPUESTA:STRING, '
		'EC_RESULTADO__ASESOR_ENTREGA_INFORMACION_SOLICITADA_SIEMPRE_CON_UN_ENFOQUE_COMERCIAL:STRING, '
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
				'ID_CALL' : arrayCSV[16],
				'DOC_CLIENTE' : arrayCSV[17],
				'TELEFONO_CLIENTE' : arrayCSV[18],
				'TIPIFICACION' : arrayCSV[19],
				'FECHA_ASEGURAMIENTO' : arrayCSV[20],
				'HORA_ASEGURAMIENTO' : arrayCSV[21],
				'PEC' : arrayCSV[22],
				'PENC' : arrayCSV[23],
				'GOS' : arrayCSV[24],
				'ID_RESULTADO' : arrayCSV[25],
				'ENC_HABILIDADES_BLANDAS_REALIZA_EL_SALUDO_O_INICIO_DE_LA_CONVERSACION_DE_MANERA_ADECUADA_Y_CORDIAL__QUE_SEA_CERCANO' : arrayCSV[26],
				'EC_REQUISITOS_DE_NEGOCIO_POLITICA_DE_SEGURIDAD_DE_LA_INFORMACION' : arrayCSV[27],
				'ENC_HABILIDADES_BLANDAS_SE_EXPRESA_DE_MANERA_CLARA_PRECISA_Y_CONCRETA' : arrayCSV[28],
				'EC_REQUISITOS_DE_NEGOCIO_REALIZA_UN_CORRECTO_ESCALAMIENTO__SI_APLICA_DE_ACUERDO_A_LA_SOLICITUD_DEL_CLIENTE' : arrayCSV[29],
				'EC_REQUISITOS_DE_NEGOCIO_INVITA_AL_CLIENTE_A_CALIFICAR_LA_ATENCION_RECIBIDA' : arrayCSV[30],
				'EC_REQUISITOS_DE_NEGOCIO_SE_DESPIDE_DE_MANERA_CORDIAL' : arrayCSV[31],
				'EC_REQUISITOS_DE_NEGOCIO_LOS_TIEMPOS_DE_RESPUESTA_SON_ACORDES_A_LA_SOLICITUD' : arrayCSV[32],
				'EC_REQUISITOS_DE_NEGOCIO_USA_DE_MANERA_ADECUADA_LAS_RESPUESTAS_PREDETERMINADAS' : arrayCSV[33],
				'EC_REQUISITOS_DE_NEGOCIO_REDACTA_USANDO_UNA_ORTOGRAFIA_IMPECABLE' : arrayCSV[34],
				'EC_REQUISITOS_DE_NEGOCIO_CODIFICACION_COMPLETA_Y_CORRECTA_EN_TODOS_LOS_APLICATIVOS' : arrayCSV[35],
				'EC_EXPERIENCIA_FINALIZA_LA_LLAMADA_VALIDANDO_LA_SATISFACCION_CON_LA_SOLUCION_ENTREGADA_E_INDAGA_SI_PUEDE_AYUDARLE_EN_ALGO_MAS' : arrayCSV[36],
				'EC_EXPERIENCIA_EVITA_MALTRATO_AL_CLIENTE' : arrayCSV[37],
				'ENC_HABILIDADES_BLANDAS_DEMUESTRA_EMPATIA_Y_COHERENCIA_EN_LA_RESPUESTA' : arrayCSV[38],
				'EC_RESULTADO__ASESOR_ENTREGA_INFORMACION_SOLICITADA_SIEMPRE_CON_UN_ENFOQUE_COMERCIAL' : arrayCSV[39],
				'ESTADO_ASEG' : arrayCSV[40]




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

	transformed | 'Escritura a BigQuery lal' >> beam.io.WriteToBigQuery(
		gcs_project + ":sensus.dchat", 
		schema=TABLE_SCHEMA, 	
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)
	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")