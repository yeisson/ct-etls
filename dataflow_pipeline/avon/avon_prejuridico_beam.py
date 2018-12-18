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
	'fecha:DATE, '
	'ano:STRING, '
	'campana:STRING, '
	'codigo:STRING, '
	'zona:STRING, '
	'unidad:STRING, '
	'seccion:STRING, '
	'territorio:STRING, '
	'cedula:STRING, '
	'apellido:STRING, '
	'nombre:STRING, '
	'direccion1:STRING, '
	'direccion2:STRING, '
	'barrio:STRING, '
	'departamento:STRING, '
	'ciudad:STRING, '
	'telefono1:STRING, '
	'telefono2:STRING, '
	'numero_de_campanas:STRING, '
	'past_due:STRING, '
	'ultimo_numero_de_factura:STRING, '
	'last_amount_1:STRING, '
	'ultimo_ano_pedido:STRING, '
	'ultima_campana_pedido:STRING, '
	'balance:STRING, '
	'email:STRING, '
	'fecha_caida_pd1:DATE, '
	'valor_pd1:STRING, '
	'telefono3:STRING, '
	'ct:STRING, '
	'nombre_ref_personal1:STRING, '
	'tel_ref_personal1:STRING, '
	'nombre_ref_personal2:STRING, '
	'tel_ref_personal2:STRING, '
	'nombre_ref_comercial1:STRING, '
	'tel_ref_comercial1:STRING, '
	'nombre_ref_comercial2:STRING, '
	'tel_ref_comercial2:STRING, '
	'est_disp:STRING, '
	'segmento:STRING, '
	'riesgo:STRING '
)

class formatearData(beam.DoFn):
	
	def process(self, element):
		print(element)
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'ano' : arrayCSV[0],
				'campana' : arrayCSV[1],
				'codigo' : arrayCSV[2],
				'zona' : arrayCSV[3],
				'unidad' : arrayCSV[4],
				'seccion' : arrayCSV[5],
				'territorio' : arrayCSV[6],
				'cedula' : arrayCSV[7],
				'apellido' : arrayCSV[8],
				'nombre' : arrayCSV[9],
				'direccion1' : arrayCSV[10],
				'direccion2' : arrayCSV[11],
				'barrio' : arrayCSV[12],
				'departamento' : arrayCSV[13],
				'ciudad' : arrayCSV[14],
				'telefono1' : arrayCSV[15],
				'telefono2' : arrayCSV[16],
				'numero_de_campanas' : arrayCSV[17],
				'past_due' : arrayCSV[18],
				'ultimo_numero_de_factura' : arrayCSV[19],
				'last_amount_1' : arrayCSV[20],
				'ultimo_ano_pedido' : arrayCSV[21],
				'ultima_campana_pedido' : arrayCSV[22],
				'balance' : arrayCSV[23],
				'email' : arrayCSV[24],
				'fecha_caida_pd1' : arrayCSV[25][0:4]+"-"+arrayCSV[25][4:6]+"-"+arrayCSV[25][6:8],
				'valor_pd1' : arrayCSV[26],
				'telefono3' : arrayCSV[27],
				'ct' : arrayCSV[28],
				'nombre_ref_personal1' : arrayCSV[29],
				'tel_ref_personal1' : arrayCSV[30],
				'nombre_ref_personal2' : arrayCSV[31],
				'tel_ref_personal2' : arrayCSV[32],
				'nombre_ref_comercial1' : arrayCSV[33],
				'tel_ref_comercial1' : arrayCSV[34],
				'nombre_ref_comercial2' : arrayCSV[35],
				'tel_ref_comercial2' : arrayCSV[36],
				'est_disp' : arrayCSV[37],
				'segmento' : arrayCSV[38],
				'riesgo' : arrayCSV[39]
				}
		
		return [tupla]



def run():

	#gcs_path = "gs://ct-avon" #Definicion de la raiz del bucket
	#gcs_project = "contento-bi"

	pipeline =  beam.Pipeline(runner="DirectRunner")
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT")
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText("archivos/AVON_INF_PREJ_20181111.TXT")

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))

	transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_avon",file_name_suffix='.txt',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-avon/prejuridico/info_carga_avon",file_name_suffix='.txt',shard_name_template='')

	# transformed | 'Escritura a BigQuery Avon' >> beam.io.WriteToBigQuery(
    #     gcs_project + ":avon.prejuridico2",
    #     schema=TABLE_SCHEMA,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")



