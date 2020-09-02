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
	'Item:STRING, '
	'Sponsor:STRING, '
	'campana:STRING, '
	'Num_Doc_Cliente:STRING, '
	'fecha_llamada:STRING, '
	'id_asesor:STRING, '
	'nombre_asesor:STRING, '
	'tipo_contacto:STRING, '
	'tipificacion:STRING, '
	'Unico:STRING, '
	'Contacto_General:INTEGER, '
	'Contacto_Efectivo:INTEGER, '
	'Venta:INTEGER, '
	'Dia:STRING, '
	'Tipo_Base:STRING, '
	'Genero:STRING, '
	'Rango_Edad:STRING, '
	'Segmento_Edad:STRING, '
	'Estado_Civil:STRING, '
	'Beneficiarios:STRING, '
	'Categoria:STRING, '
	'Rango_Cupo:STRING, '
	'Perfil:STRING, '
	'Campana_2:STRING, '
	'Franja:STRING, '
	'Fecha_Asignada:STRING, '
	'Fecha_Gestion_x:STRING, '
	'Fecha_Gestion:STRING, '
	'Val:STRING '
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
				'Item': arrayCSV[0],
				'Sponsor': arrayCSV[1],
				'campana': arrayCSV[2],
				'Num_Doc_Cliente': arrayCSV[3],
				'fecha_llamada': arrayCSV[4],
				'id_asesor': arrayCSV[5],
				'nombre_asesor': arrayCSV[6],
				'tipo_contacto': arrayCSV[7],
				'tipificacion': arrayCSV[8],
				'Unico': arrayCSV[9],
				'Contacto_General': arrayCSV[10],
				'Contacto_Efectivo': arrayCSV[11],
				'Venta': arrayCSV[12],
				'Dia': arrayCSV[13],
				'Tipo_Base': arrayCSV[14],
				'Genero': arrayCSV[15],
				'Rango_Edad': arrayCSV[16],
				'Segmento_Edad': arrayCSV[17],
				'Estado_Civil': arrayCSV[18],
				'Beneficiarios': arrayCSV[19],
				'Categoria': arrayCSV[20],
				'Rango_Cupo': arrayCSV[21],
				'Perfil': arrayCSV[22],
				'Campana_2': arrayCSV[23],
				'Franja': arrayCSV[24],
				'Fecha_Asignada': arrayCSV[25],
				'Fecha_Gestion_x': arrayCSV[26],
				'Fecha_Gestion': arrayCSV[27],
				'Val': arrayCSV[28]
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-metlife" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Metlife' >> beam.io.WriteToBigQuery(
		gcs_project + ":MetLife.Seguimiento_Diario", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")

    


