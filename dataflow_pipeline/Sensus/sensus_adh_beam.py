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
	'EMPRESA:STRING, '
	'CONTADOR:STRING, '
	'CODIGO:STRING, '
	'DESCRIPCION:STRING, '
	'CATEGORIA:STRING, '
	'UEN:STRING, '
	'DOCUMENTO:STRING, '
	'NOMBRE_GERENTE:STRING, '
	'DOC_EJEC:STRING, '
	'NOMBRE_EJECUTIVO:STRING, '
	'DOC_NEG:STRING, '
	'NOMBRES_NEG:STRING, '
	'VINCULACION:STRING, '
	'ID_PROGRAMA:STRING, '
	'PRODUCTO:STRING, '
	'DOC_TEAM:STRING, '
	'NOMBRE_TEAM_LEADER:STRING, '
	'NOTA_GESTION:STRING, '
	'ASEGURAMIENTOS:STRING, '
	'NO_PEC:STRING, '
	'SI_PEC:STRING, '
	'PEC:STRING, '
	'PENC:STRING, '
	'GOS:STRING, '
	'AFECTACIONES_ATR:STRING, '
	'ID_CC:STRING, '
	'SS:STRING, '
	'M1_USA_PALABRAS_PUENTE:STRING, '
	'M2_USA_PREGUNTAS_ABIERTAS:STRING, '
	'M2_REALIZA_ACUERDO_DE_ENTENDIMIENTO:STRING, '
	'M3_USA_EL_ESQUEMA_DE_MANEJO_DE_OBJECIONES:STRING, '
	'M3_USA_TECNICA_ECCP:STRING, '
	'M4_USA_CIERRE_GANA_Y_GANA__RATIFICACION_DE_ACUERDO_:STRING, '
	'M3_USA_CIRCULO_DE_ORO:STRING, '
	'M3_USA_TECNICA_AIB:STRING, '
	'M3_USA_TECNICA_SVB:STRING, '
	'M4_USA_CIERRE_GANA_Y_GANA__RATIFICACION_DE_LA_VENTA_:STRING, '
	'M1:STRING, '
	'M2:STRING, '
	'M3:STRING, '
	'M4:STRING, '
	'ADH_TOTAL:STRING, '
	'NIVEL:STRING, '
	'C1:STRING, '
	'C2:STRING, '
	'C3:STRING, '
	'C4:STRING '










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
				'EMPRESA' : arrayCSV[0],
				'CONTADOR' : arrayCSV[1],
				'CODIGO' : arrayCSV[2],
				'DESCRIPCION' : arrayCSV[3],
				'CATEGORIA' : arrayCSV[4],
				'UEN' : arrayCSV[5],
				'DOCUMENTO' : arrayCSV[6],
				'NOMBRE_GERENTE' : arrayCSV[7],
				'DOC_EJEC' : arrayCSV[8],
				'NOMBRE_EJECUTIVO' : arrayCSV[9],
				'DOC_NEG' : arrayCSV[10],
				'NOMBRES_NEG' : arrayCSV[11],
				'VINCULACION' : arrayCSV[12],
				'ID_PROGRAMA' : arrayCSV[13],
				'PRODUCTO' : arrayCSV[14],
				'DOC_TEAM' : arrayCSV[15],
				'NOMBRE_TEAM_LEADER' : arrayCSV[16],
				'NOTA_GESTION' : arrayCSV[17],
				'ASEGURAMIENTOS' : arrayCSV[18],
				'NO_PEC' : arrayCSV[19],
				'SI_PEC' : arrayCSV[20],
				'PEC' : arrayCSV[21],
				'PENC' : arrayCSV[22],
				'GOS' : arrayCSV[23],
				'AFECTACIONES_ATR' : arrayCSV[24],
				'ID_CC' : arrayCSV[25],
				'SS' : arrayCSV[26],
				'M1_USA_PALABRAS_PUENTE' : arrayCSV[27],
				'M2_USA_PREGUNTAS_ABIERTAS' : arrayCSV[28],
				'M2_REALIZA_ACUERDO_DE_ENTENDIMIENTO' : arrayCSV[29],
				'M3_USA_EL_ESQUEMA_DE_MANEJO_DE_OBJECIONES' : arrayCSV[30],
				'M3_USA_TECNICA_ECCP' : arrayCSV[31],
				'M4_USA_CIERRE_GANA_Y_GANA__RATIFICACION_DE_ACUERDO_' : arrayCSV[32],
				'M3_USA_CIRCULO_DE_ORO' : arrayCSV[33],
				'M3_USA_TECNICA_AIB' : arrayCSV[34],
				'M3_USA_TECNICA_SVB' : arrayCSV[35],
				'M4_USA_CIERRE_GANA_Y_GANA__RATIFICACION_DE_LA_VENTA_' : arrayCSV[36],
				'M1' : arrayCSV[37],
				'M2' : arrayCSV[38],
				'M3' : arrayCSV[39],
				'M4' : arrayCSV[40],
				'ADH_TOTAL' : arrayCSV[41],
				'NIVEL' : arrayCSV[42],
				'C1' : arrayCSV[43],
				'C2' : arrayCSV[44],
				'C3' : arrayCSV[45],
				'C4' : arrayCSV[46]
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

	transformed | 'Escritura a BigQuery seguimiento' >> beam.io.WriteToBigQuery(
		gcs_project + ":sensus.adh", 
		schema=TABLE_SCHEMA, 	
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")