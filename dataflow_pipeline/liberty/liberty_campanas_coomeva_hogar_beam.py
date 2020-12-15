#encoding: utf-8 
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
	'id_campana:STRING, '
	'LAST_NAME:STRING, '
	'TIPO_DE_IDENTIFICACION:STRING, '
	'IDENTIFICACION:STRING, '
	'REGIONAL:STRING, '
	'CODIGO_CIUDAD:STRING, '
	'CIUDAD_1:STRING, '
	'DIRECCION:STRING, '
	'TELEFONO_1:STRING, '
	'TELEFONO_2:STRING, '
	'TELEFONO_3:STRING, '
	'CELULAR_1:STRING, '
	'CELULAR_2:STRING, '
	'CELULAR_3:STRING, '
	'ESTADO_CIVIL:STRING, '
	'EDAD:STRING, '
	'GENERO:STRING, '
	'ESTRATO:STRING, '
	'TIPO_DE_VIVIENDA:STRING, '
	'FECHA_NACIMIENTO:STRING, '
	'ESTADO_COOMEVA:STRING, '
	'RANK_PROP_COMPRA_HOGAR:STRING, '
	'SARLAFT:STRING, '
	'EMAIL:STRING, '
	'CODIGO_ESTADO:STRING, '
	'ESTADO:STRING, '
	'CUOTAS:STRING, '
	'ESTADO_BASE:STRING, '
	'NOMBRE_DE_CAMPANA:STRING, '
	'IPDIAL_CODE:STRING '

)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha, id_campana):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
		self.id_campana = id_campana
		
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha' : self.mifecha,
				'id_campana' : self.id_campana,
				'LAST_NAME' : arrayCSV[0],
				'TIPO_DE_IDENTIFICACION' : arrayCSV[1],
				'IDENTIFICACION' : arrayCSV[2],
				'REGIONAL' : arrayCSV[3],
				'CODIGO_CIUDAD' : arrayCSV[4],
				'CIUDAD_1' : arrayCSV[5],
				'DIRECCION' : arrayCSV[6],
				'TELEFONO_1' : arrayCSV[7],
				'TELEFONO_2' : arrayCSV[8],
				'TELEFONO_3' : arrayCSV[9],
				'CELULAR_1' : arrayCSV[10],
				'CELULAR_2' : arrayCSV[11],
				'CELULAR_3' : arrayCSV[12],
				'ESTADO_CIVIL' : arrayCSV[13],
				'EDAD' : arrayCSV[14],
				'GENERO' : arrayCSV[15],
				'ESTRATO' : arrayCSV[16],
				'TIPO_DE_VIVIENDA' : arrayCSV[17],
				'FECHA_NACIMIENTO' : arrayCSV[18],
				'ESTADO_COOMEVA' : arrayCSV[19],
				'RANK_PROP_COMPRA_HOGAR' : arrayCSV[20],
				'SARLAFT' : arrayCSV[21],
				'EMAIL' : arrayCSV[22],
				'CODIGO_ESTADO' : arrayCSV[23],
				'ESTADO' : arrayCSV[24],
				'CUOTAS' : arrayCSV[25],
				'ESTADO_BASE' : arrayCSV[26],
				'NOMBRE_DE_CAMPANA' : arrayCSV[27],
				'IPDIAL_CODE' : arrayCSV[28]
				}
		
		return [tupla]



def run(archivo, mifecha, id_campana):

	gcs_path = "gs://ct-telefonia" #Definicion de la raiz del bucket
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

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha, id_campana)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_seg", file_name_suffix='.csv',shard_name_template='')
	#transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/info-segumiento/info_carga_banco_seg",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery liberty_campanas' >> beam.io.WriteToBigQuery(
		gcs_project + ":telefonia.liberty_campanas_coomeva_hogar", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



