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
                'BASE:STRING, '
                'ESTADO:STRING, '
                'ANOCREACION:STRING, '
                'MESCREACION:STRING, '
                'FECHACREACION:STRING, '
                'TERCEROPROPIETARIO:STRING, '
                'NITPROPIETARIO:STRING, '
                'DIRECCIONPROPIETARIO:STRING, '
                'CIUDAD:STRING, '
                'DEPARTAMENTO:STRING, '
                'TELEFONOFIJO:STRING, '
                'CELULAR:STRING, '
                'OTROTELEFONO:STRING, '
                'EMAIL:STRING, '
                'AUTORIZAHABEASDATAPROPIETARIO:STRING, '
                'CHASISSERIE:STRING, '
                'MODELO:STRING, '
                'MARCA:STRING, '
                'LINEA:STRING, '
                'RAZONSOCIALESTABLECIMIENTO:STRING, '
                'MODELO_LINEA:STRING, '
                'UNIDAD_DE_NEGOCIO:STRING, '
                'CATEGORIA:STRING, '
                'SUBCATEGORIA:STRING, '
                'SEGMENTO:STRING, '
                'SUBSEGMENTO:STRING, '
                'MARCA2:STRING, '
                'FECHA_CARGUE:STRING '
                )
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'BASE' : arrayCSV[0],
                'ESTADO' : arrayCSV[1],
                'ANOCREACION' : arrayCSV[2],
                'MESCREACION' : arrayCSV[3],
                'FECHACREACION' : arrayCSV[4],
                'TERCEROPROPIETARIO' : arrayCSV[5],
                'NITPROPIETARIO' : arrayCSV[6],
                'DIRECCIONPROPIETARIO' : arrayCSV[7],
                'CIUDAD' : arrayCSV[8],
                'DEPARTAMENTO' : arrayCSV[9],
                'TELEFONOFIJO' : arrayCSV[10],
                'CELULAR' : arrayCSV[11],
                'OTROTELEFONO' : arrayCSV[12],
                'EMAIL' : arrayCSV[13],
                'AUTORIZAHABEASDATAPROPIETARIO' : arrayCSV[14],
                'CHASISSERIE' : arrayCSV[15],
                'MODELO' : arrayCSV[16],
                'MARCA' : arrayCSV[17],
                'LINEA' : arrayCSV[18],
                'RAZONSOCIALESTABLECIMIENTO' : arrayCSV[19],
                'MODELO_LINEA' : arrayCSV[20],
                'UNIDAD_DE_NEGOCIO' : arrayCSV[21],
                'CATEGORIA' : arrayCSV[22],
                'SUBCATEGORIA' : arrayCSV[23],
                'SEGMENTO' : arrayCSV[24],
                'SUBSEGMENTO' : arrayCSV[25],
                'MARCA2' : arrayCSV[26],
                'FECHA_CARGUE': self.mifecha
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-auteco" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runer = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runer, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "10",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"

	])
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery Mobility' >> beam.io.WriteToBigQuery(
		gcs_project + ":Auteco_Mobility.Base_perfil_cliente", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)


	jobObject = pipeline.run()
	return ("Corrio Full HD")



