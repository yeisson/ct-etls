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
	'Fecha:STRING, '
	'Ano:STRING, '
	'Campana:STRING, '
	'Factura:STRING, '
	'Zona:STRING, '
	'Unidad:STRING, '
	'Seccion:STRING, '
	'Territorio:STRING, '
	'Nit:STRING, '
	'Apellidos:STRING, '
	'Nombres:STRING, '
	'Direccion_Deudor:STRING, '
	'Direccion_Deudor_1:STRING, '
	'Barrio_Deudor:STRING, '
	'Departamento_Deudor:STRING, '
	'Ciudad_Deudor:STRING, '
	'Telefono_Deudor:STRING, '
	'Telefono_Deudor_1:STRING, '
	'Num_Campanas:STRING, '
	'Past Due:STRING, '
	'Ultim_Num_Invoice:STRING, '
	'Valor_Factura:STRING, '
	'Ultim_Ano_Pedido:STRING, '
	'Ultim_Campana_Pedido:STRING, '
	'Saldo:STRING, '
	'Email:STRING, '
	'Fecha_Factura:DATE, '
	'Valor_PD1:STRING, '
	'Telefono_Deudor_2:STRING, '
	'CT:STRING, '
	'Nombres_Referencia_Personal_1:STRING, '
	'Telefono_Referencia_Personal_1:STRING, '
	'Nombres_Referencia_Personal_2:STRING, '
	'Telefono_Referencia_Personal_2:STRING, '
	'Nombres_Referencia_Comercial_1:STRING, '
	'Telefono_Referencia_Comercial_1:STRING, '
	'Nombres_Referencia_Comercial_2:STRING, '
	'Telefono_Referencia_Comercial_2:STRING, '
	'Est.Disp:STRING, '
	'Ciclo:STRING, '
	'Vlr_redimir:STRING '
	'Origen:STRING '
)

class formatearData(beam.DoFn):
	
	def process(self, element):
		print(element)
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				'Fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'Ano' : arrayCSV[0],
				'Campana' : arrayCSV[1],
				'Factura' : arrayCSV[2],
				'Zona' : arrayCSV[3],
				'Unidad' : arrayCSV[4],
				'Seccion' : arrayCSV[5],
				'Territorio' : arrayCSV[6],
				'Nit' : arrayCSV[7],
				'Apellidos' : arrayCSV[8],
				'Nombres' : arrayCSV[9],
				'Direccion_Deudor' : arrayCSV[10],
				'Direccion_Deudor_1' : arrayCSV[11],
				'Barrio_Deudor' : arrayCSV[12],
				'Departamento_Deudor' : arrayCSV[13],
				'Ciudad_Deudor' : arrayCSV[14],
				'Telefono_Deudor' : arrayCSV[15],
				'Telefono_Deudor_1' : arrayCSV[16],
				'Num_Campanas' : arrayCSV[17],
				'Past Due' : arrayCSV[18],
				'Ultim_Num_Invoice' : arrayCSV[19],
				'Valor_Factura' : arrayCSV[20],
				'Ultim_Ano_Pedido' : arrayCSV[21],
				'Ultim_Campana_Pedido' : arrayCSV[22],
				'Saldo' : arrayCSV[23],
				'Email' : arrayCSV[24],
				'Fecha_Factura' : arrayCSV[25][0:4]+"-"+arrayCSV[25][4:6]+"-"+arrayCSV[25][6:8],
				'Valor_PD1' : arrayCSV[26],
				'Telefono_Deudor_2' : arrayCSV[27],
				'CT' : arrayCSV[28],
				'Nombres_Referencia_Personal_1' : arrayCSV[29],
				'Telefono_Referencia_Personal_1' : arrayCSV[30],
				'Nombres_Referencia_Personal_2' : arrayCSV[31],
				'Telefono_Referencia_Personal_2' : arrayCSV[32],
				'Nombres_Referencia_Comercial_1' : arrayCSV[33],
				'Telefono_Referencia_Comercial_1' : arrayCSV[34],
				'Nombres_Referencia_Comercial_2' : arrayCSV[35],
				'Telefono_Referencia_Comercial_2' : arrayCSV[36],
				'Est.Disp' : arrayCSV[37],
				'Ciclo' : arrayCSV[38],
				'Vlr_redimir' : arrayCSV[39]
				'Origen' : arrayCSV[40]
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



