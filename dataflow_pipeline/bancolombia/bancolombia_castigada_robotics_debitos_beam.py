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

TABLE_SCHEMA = ('IDKEY:STRING, '
				'FECHA:STRING, '
				'CONSECUTIVO_GESTION:STRING, '
				'DURACION:STRING, '
				'CONSECUTIVO_OBLIGACION:STRING, '
				'NIT:STRING, '
				'NOMBRE:STRING, '
				'NRO_DOCUMENTO:STRING, '
				'FECHA_GESTION:STRING, '
				'DIAS_MORA:STRING, '
				'CUOTAS_VENCIDAS:STRING, '
				'ASESOR:STRING, '
				'REGIONAL:STRING, '
				'TIPO:STRING, '
				'TELEFONO:STRING, '
				'NOTA:STRING, '
				'GRABADOR:STRING, '
				'NOMBRE_ABOGADO:STRING, '
				'CODIGO_ABOGADO:STRING, '
				'ULTIMO_ABOGADO:STRING, '
				'CODIGO_DE_GESTION:STRING, '
				'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO:STRING, '
				'DESCRIPCION_MEJOR_CODIGO_DE_GESTION:STRING, '
				'MEJOR_CODIGO_DE_GESTION:STRING, '
				'FECHA_MEJOR_CODIGO_DE_GESTION:STRING, '
				'CUENTA:STRING, '
				'CONSECUTIVO_DEMANDA:STRING, '
				'FECHA_DE_PROMESA_2:STRING, '
				'FECHA_VENCI:STRING, '
				'NIT_DEUDOR:STRING, '
				'NIT_REFERENCIA:STRING, '
				'NIT_COMPANIA:STRING, '
				'NIT_TERCERO:STRING, '
				'OCUPACION:STRING, '
				'PROFESION:STRING, '
				'T_COMPROMI:STRING, '
				'T_ENTRADA:STRING, '
				'HORA_GRABACION:STRING, '
				'T_IGRABA:STRING, '
				'SITIO:STRING, '
				'LOTE:STRING, '
				'HORA_DE_COMPROMISO:STRING, '
				'COBRADOR:STRING, '
				'CODIGO_DE_COBRO_ANTERIOR:STRING, '
				'CODCOB_MEC_NO:STRING, '
				'CODCOB_MEC_NOR_TRA:STRING, '
				'CODCOB_MEC_NOR_UTIL:STRING, '
				'CODCOB_T:STRING, '
				'CODIGO_CAUSAL:STRING, '
				'DESCRIPCION_CAUSAL:STRING, '
				'CODIGO_DE_CONTACTO:STRING, '
				'CONTROL:STRING, '
				'CONSDIREC:STRING, '
				'CONSSECECO:STRING, '
				'CUADRANTE:STRING, '
				'FECHA_DE_ACTUACION:STRING, '
				'FECHA_CORTE:STRING, '
				'FECHA_PROMESA:STRING '
				)

class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla = {'IDKEY' : str(uuid.uuid4()),
				'FECHA': self.mifecha,
				'CONSECUTIVO_GESTION' : arrayCSV[0],
				'DURACION' : arrayCSV[1],
				'CONSECUTIVO_OBLIGACION' : arrayCSV[2],
				'NIT' : arrayCSV[3],
				'NOMBRE' : arrayCSV[4],
				'NRO_DOCUMENTO' : arrayCSV[5],
				'FECHA_GESTION' : arrayCSV[6],
				'DIAS_MORA' : arrayCSV[7],
				'CUOTAS_VENCIDAS' : arrayCSV[8],
				'ASESOR' : arrayCSV[9],
				'REGIONAL' : arrayCSV[10],
				'TIPO' : arrayCSV[11],
				'TELEFONO' : arrayCSV[12],
				'NOTA' : arrayCSV[13],
				'GRABADOR' : arrayCSV[14],
				'NOMBRE_ABOGADO' : arrayCSV[15],
				'CODIGO_ABOGADO' : arrayCSV[16],
				'ULTIMO_ABOGADO' : arrayCSV[17],
				'CODIGO_DE_GESTION' : arrayCSV[18],
				'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO' : arrayCSV[19],
				'DESCRIPCION_MEJOR_CODIGO_DE_GESTION' : arrayCSV[20],
				'MEJOR_CODIGO_DE_GESTION' : arrayCSV[21],
				'FECHA_MEJOR_CODIGO_DE_GESTION' : arrayCSV[22],
				'CUENTA' : arrayCSV[23],
				'CONSECUTIVO_DEMANDA' : arrayCSV[24],
				'FECHA_DE_PROMESA_2' : arrayCSV[25],
				'FECHA_VENCI' : arrayCSV[26],
				'NIT_DEUDOR' : arrayCSV[27],
				'NIT_REFERENCIA' : arrayCSV[28],
				'NIT_COMPANIA' : arrayCSV[29],
				'NIT_TERCERO' : arrayCSV[30],
				'OCUPACION' : arrayCSV[31],
				'PROFESION' : arrayCSV[32],
				'T_COMPROMI' : arrayCSV[33],
				'T_ENTRADA' : arrayCSV[34],
				'HORA_GRABACION' : arrayCSV[35],
				'T_IGRABA' : arrayCSV[36],
				'SITIO' : arrayCSV[37],
				'LOTE' : arrayCSV[38],
				'HORA_DE_COMPROMISO' : arrayCSV[39],
				'COBRADOR' : arrayCSV[40],
				'CODIGO_DE_COBRO_ANTERIOR' : arrayCSV[41],
				'CODCOB_MEC_NO' : arrayCSV[42],
				'CODCOB_MEC_NOR_TRA' : arrayCSV[43],
				'CODCOB_MEC_NOR_UTIL' : arrayCSV[44],
				'CODCOB_T' : arrayCSV[45],
				'CODIGO_CAUSAL' : arrayCSV[46],
				'DESCRIPCION_CAUSAL' : arrayCSV[47],
				'CODIGO_DE_CONTACTO' : arrayCSV[48],
				'CONTROL' : arrayCSV[49],
				'CONSDIREC' : arrayCSV[50],
				'CONSSECECO' : arrayCSV[51],
				'CUADRANTE' : arrayCSV[52],
				'FECHA_DE_ACTUACION' : arrayCSV[53],
				'FECHA_CORTE' : arrayCSV[54],
				'FECHA_PROMESA' : arrayCSV[55]
				}
		
		return [tupla]

def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia_castigada" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_castigada.rob_aux_debitos", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	jobObject.wait_until_finish()

	return ("Corrio Full HD")
