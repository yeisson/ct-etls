from __future__ import print_function, absolute_import
import logging
import re
import json
import requests
import uuid
import time
import os
import socket
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

####################### PARAMETROS DE LA TABLA EN BQ ##########################

TABLE_SCHEMA = (
			'Nit:STRING,'
			'Fecha_Gestion:STRING,'
			'Desc_Ultimo_Codigo_De_Gestion_Prejuridico:STRING,'
			'Grabador:STRING,'
			'Consdocdeu:STRING,'
			'Contacto:STRING,'
			'Tipo_Gest:STRING,'
			'Prioridad:STRING,'
			'Dias_Mora:STRING,'
			'Rango_Mora:STRING,'
			'Valor_Obligacion:STRING,'
			'Valor_Vencido:STRING,'
			'Endeudamiento:STRING,'
			'Valor_Cuota:STRING,'
			'Probabilidad_de_Propension_de_pago:STRING,'
			'Grupo_de_Priorizacion:STRING,'
			'Grupo:STRING,'
			'Nombre_De_Producto:STRING,'
			'Region:STRING,'
			'Segmento:STRING,'
			'Calificacion_Real:STRING,'
			'Cuadrante:STRING,'
			'Causal:STRING,'
			'Sector_Economico:STRING,'
			'Cosecha:STRING,'
			'Mono_Multi:STRING,'
			'Nombre_Asesor:STRING,'
			'Producto:STRING,'
			'Sede:STRING,'
			'Fecha_CtaDia:STRING,'
			'DiaSem:STRING,'
			'Sem:STRING'
			)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {
				'Nit': arrayCSV[0],
				'Fecha_Gestion': arrayCSV[1],
				'Desc_Ultimo_Codigo_De_Gestion_Prejuridico': arrayCSV[2],
				'Grabador': arrayCSV[3],
				'Consdocdeu': arrayCSV[4],
				'Contacto': arrayCSV[5],
				'Tipo_Gest': arrayCSV[6],
				'Prioridad': arrayCSV[7],
				'Dias_Mora': arrayCSV[8],
				'Rango_Mora': arrayCSV[9],
				'Valor_Obligacion': arrayCSV[10],
				'Valor_Vencido': arrayCSV[11],
				'Endeudamiento': arrayCSV[12],
				'Valor_Cuota': arrayCSV[13],
				'Probabilidad_de_Propension_de_pago': arrayCSV[14],
				'Grupo_de_Priorizacion': arrayCSV[15],
				'Grupo': arrayCSV[16],
				'Nombre_De_Producto': arrayCSV[17],
				'Region': arrayCSV[18],
				'Segmento': arrayCSV[19],
				'Calificacion_Real': arrayCSV[20],
				'Cuadrante': arrayCSV[21],
				'Causal': arrayCSV[22],
				'Sector_Economico': arrayCSV[23],
				'Cosecha': arrayCSV[24],
				'Mono_Multi': arrayCSV[25],
				'Nombre_Asesor': arrayCSV[26],
				'Producto': arrayCSV[27],
				'Sede': arrayCSV[28],
				'Fecha_CtaDia': arrayCSV[29],
				'DiaSem': arrayCSV[30],
				'Sem': arrayCSV[31]
				}
		
		return [tupla]

############################ CODIGO DE EJECUCION ###################################
def run(table):

	gcs_path = 'gs://ct-bridge' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"
	FECHA_CARGUE = str(datetime.date.today())

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/" + FECHA_CARGUE + "_6" +".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/" + "REWORK",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Bridge' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contactabilidad."+ table, 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")


################################################################################