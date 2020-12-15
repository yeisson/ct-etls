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
	'tipo_doc:STRING, '
	'NUMERO_IDENTIFICACION:STRING, '
	'NOMBRE:STRING, '
	'EDAD:STRING, '
	'RANGO_EDAD:STRING, '
	'TIPO_VIVIENDA:STRING, '
	'ULTIMA_GESTION:STRING, '
	'CORREO_ELECTRONICO:STRING, '
	'PERFIL_COMPORTAMIENTO:STRING, '
	'PERFIL_BUENCLIENTE:STRING, '
	'DEFINICION_BUEN_CLIENTE:STRING, '
	'TOTAL_CUENTAS_BCSC:STRING, '
	'PROMEDIO_SEMESTRE_CUENTAS_BCSC:STRING, '
	'MARCA_1:STRING, '
	'NUMERO_CUENTA_1:STRING, '
	'TIPO_CUENTA_1:STRING, '
	'CODIGO_OFICINA_1:STRING, '
	'OFICINA_1:STRING, '
	'REGIONAL_1:STRING, '
	'MARCA_2:STRING, '
	'NUMERO_CUENTA_2:STRING, '
	'TIPO_CUENTA_2:STRING, '
	'CODIGO_OFICINA_2:STRING, '
	'OFICINA_2:STRING, '
	'REGIONAL_2:STRING, '
	'MARCA_3:STRING, '
	'NUMERO_CUENTA_3:STRING, '
	'TIPO_CUENTA_3:STRING, '
	'CODIGO_OFICINA_3:STRING, '
	'OFICINA_3:STRING, '
	'REGIONAL_3:STRING, '
	'DIRECCION_01:STRING, '
	'TELEFONO_01:STRING, '
	'CIUDAD_01:STRING, '
	'CELULAR_01:STRING, '
	'DIRECCION_02:STRING, '
	'TELEFONO_02:STRING, '
	'CIUDAD_02:STRING, '
	'CELULAR_02:STRING, '
	'DIRECCION_03:STRING, '
	'TELEFONO_03:STRING, '
	'CIUDAD_03:STRING, '
	'CELULAR_03:STRING, '
	'DIRECCION_04:STRING, '
	'TELEFONO_04:STRING, '
	'CIUDAD_04:STRING, '
	'CELULAR_04:STRING, '
	'DIRECCION_05:STRING, '
	'TELEFONO_05:STRING, '
	'CIUDAD_05:STRING, '
	'CELULAR_05:STRING, '
	'ESTADO:STRING, '
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
				'tipo_doc' : arrayCSV[0],
				'NUMERO_IDENTIFICACION' : arrayCSV[1],
				'NOMBRE' : arrayCSV[2],
				'EDAD' : arrayCSV[3],
				'RANGO_EDAD' : arrayCSV[4],
				'TIPO_VIVIENDA' : arrayCSV[5],
				'ULTIMA_GESTION' : arrayCSV[6],
				'CORREO_ELECTRONICO' : arrayCSV[7],
				'PERFIL_COMPORTAMIENTO' : arrayCSV[8],
				'PERFIL_BUENCLIENTE' : arrayCSV[9],
				'DEFINICION_BUEN_CLIENTE' : arrayCSV[10],
				'TOTAL_CUENTAS_BCSC' : arrayCSV[11],
				'PROMEDIO_SEMESTRE_CUENTAS_BCSC' : arrayCSV[12],
				'MARCA_1' : arrayCSV[13],
				'NUMERO_CUENTA_1' : arrayCSV[14],
				'TIPO_CUENTA_1' : arrayCSV[15],
				'CODIGO_OFICINA_1' : arrayCSV[16],
				'OFICINA_1' : arrayCSV[17],
				'REGIONAL_1' : arrayCSV[18],
				'MARCA_2' : arrayCSV[19],
				'NUMERO_CUENTA_2' : arrayCSV[20],
				'TIPO_CUENTA_2' : arrayCSV[21],
				'CODIGO_OFICINA_2' : arrayCSV[22],
				'OFICINA_2' : arrayCSV[23],
				'REGIONAL_2' : arrayCSV[24],
				'MARCA_3' : arrayCSV[25],
				'NUMERO_CUENTA_3' : arrayCSV[26],
				'TIPO_CUENTA_3' : arrayCSV[27],
				'CODIGO_OFICINA_3' : arrayCSV[28],
				'OFICINA_3' : arrayCSV[29],
				'REGIONAL_3' : arrayCSV[30],
				'DIRECCION_01' : arrayCSV[31],
				'TELEFONO_01' : arrayCSV[32],
				'CIUDAD_01' : arrayCSV[33],
				'CELULAR_01' : arrayCSV[34],
				'DIRECCION_02' : arrayCSV[35],
				'TELEFONO_02' : arrayCSV[36],
				'CIUDAD_02' : arrayCSV[37],
				'CELULAR_02' : arrayCSV[38],
				'DIRECCION_03' : arrayCSV[39],
				'TELEFONO_03' : arrayCSV[40],
				'CIUDAD_03' : arrayCSV[41],
				'CELULAR_03' : arrayCSV[42],
				'DIRECCION_04' : arrayCSV[43],
				'TELEFONO_04' : arrayCSV[44],
				'CIUDAD_04' : arrayCSV[45],
				'CELULAR_04' : arrayCSV[46],
				'DIRECCION_05' : arrayCSV[47],
				'TELEFONO_05' : arrayCSV[48],
				'CIUDAD_05' : arrayCSV[49],
				'CELULAR_05' : arrayCSV[50],
				'ESTADO' : arrayCSV[51],
				'NOMBRE_DE_CAMPANA' : arrayCSV[52],
				'IPDIAL_CODE' : arrayCSV[52]
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
		gcs_project + ":telefonia.liberty_poliza", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



