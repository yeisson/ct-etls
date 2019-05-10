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

#coding: utf-8 

TABLE_SCHEMA = (
	'id:STRING,'
	'Completado:STRING,'
	'ultima_pagina:STRING,'
	'Lenguaje_inicial:STRING,'
	'Fecha_de_inicio:STRING,'
	'Fecha_de_la_ultima_accion:STRING,'
	'Agente:STRING,'
	'Agente_Otro:STRING,'
	'Medio_de_Contacto:STRING,'
	'Nombre_cliente:STRING,'
	'Cedula_Cliente:STRING,'
	'Celular:STRING,'
	'Correo:STRING,'
	'Tipo_aspirante:STRING,'
	'Tipologia:STRING,'
	'Estado_interesado:STRING,'
	'Tipo_de_programa:STRING,'
	'Tipo_de_programa_Otro:STRING,'
	'Programas_educacion_continua:STRING,'
	'Programas_educacion_continua_Otro:STRING,'
	'Diplomado:STRING,'
	'Diplomado_Otro:STRING,'
	'Cursos:STRING,'
	'Cursos_Otro:STRING,'
	'Programas_de_educacion_tecnica:STRING,'
	'Programas_de_educacion_tecnica_Otro:STRING,'
	'Programas_educacion_virtual:STRING,'
	'Programas_educacion_virtual_Otro:STRING,'
	'Medio_de_pago_Contado:STRING,'
	'Medio_de_pago_Credito:STRING,'
	'Medio_de_pago_Beca:STRING,'
	'Medio_de_pago_Otro:STRING,'
	'Estado_Documentacion:STRING,'
	'Estado_Documentacion_Otro:STRING,'
	'Documentacion_pendiente_Cedula:STRING,'
	'Documentacion_pendiente_Actas_de_estudio:STRING,'
	'Documentacion_pendiente_cedula_y_actas_de_estudio:STRING,'
	'Documentacion_pendiente_Visa_estudiantil:STRING,'
	'Documentacion_pendiente_Diplomas_convalidados_y_apostillados:STRING,'
	'Documentacion_pendiente_Todos:STRING,'
	'Documentacion_pendiente_Otro:STRING,'
	'Causal_no_continua_proceso:STRING,'
	'Causal_no_continua_proceso_Otro:STRING,'
	'Estado_Liquidacion:STRING,'
	'Estado_Liquidacion_Otro:STRING,'
	'Estado_pago:STRING,'
	'Estado_pago_Otro:STRING,'
	'No_esta_interesado:STRING,'
	'No_esta_interesado_Otro:STRING,'
	'Esperando_decision:STRING,'
	'Esperando_decision_Otro:STRING,'
	'Interesado_en_otro_tipo_de_educacion:STRING,'
	'Interesado_en_otro_tipo_de_educacion_Otro:STRING,'
	'No_cumple_requisitos:STRING,'
	'No_cumple_requisitos_Otro:STRING'
)

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split(',')

		tupla= {'id': arrayCSV[0],
				'Completado': arrayCSV[1],
				'ultima_pagina': arrayCSV[2],
				'Lenguaje_inicial': arrayCSV[3],
				'Fecha_de_inicio': arrayCSV[4],
				'Fecha_de_la_ultima_accion': arrayCSV[5],
				'Agente': arrayCSV[6],
				'Agente_Otro': arrayCSV[7],
				'Medio_de_Contacto': arrayCSV[8],
				'Nombre_cliente': arrayCSV[9],
				'Cedula_Cliente': arrayCSV[10],
				'Celular': arrayCSV[11],
				'Correo': arrayCSV[12],
				'Tipo_aspirante': arrayCSV[13],
				'Tipologia': arrayCSV[14],
				'Estado_interesado': arrayCSV[15],
				'Tipo_de_programa': arrayCSV[16],
				'Tipo_de_programa_Otro': arrayCSV[17],
				'Programas_educacion_continua': arrayCSV[18],
				'Programas_educacion_continua_Otro': arrayCSV[19],
				'Diplomado': arrayCSV[20],
				'Diplomado_Otro': arrayCSV[21],
				'Cursos': arrayCSV[22],
				'Cursos_Otro': arrayCSV[23],
				'Programas_de_educacion_tecnica': arrayCSV[24],
				'Programas_de_educacion_tecnica_Otro': arrayCSV[25],
				'Programas_educacion_virtual': arrayCSV[26],
				'Programas_educacion_virtual_Otro': arrayCSV[27],
				'Medio_de_pago_Contado': arrayCSV[28],
				'Medio_de_pago_Credito': arrayCSV[29],
				'Medio_de_pago_Beca': arrayCSV[30],
				'Medio_de_pago_Otro': arrayCSV[31],
				'Estado_Documentacion': arrayCSV[32],
				'Estado_Documentacion_Otro': arrayCSV[33],
				'Documentacion_pendiente_Cedula': arrayCSV[34],
				'Documentacion_pendiente_Actas_de_estudio': arrayCSV[35],
				'Documentacion_pendiente_cedula_y_actas_de_estudio': arrayCSV[36],
				'Documentacion_pendiente_Visa_estudiantil': arrayCSV[37],
				'Documentacion_pendiente_Diplomas_convalidados_y_apostillados': arrayCSV[38],
				'Documentacion_pendiente_Todos': arrayCSV[39],
				'Documentacion_pendiente_Otro': arrayCSV[40],
				'Causal_no_continua_proceso': arrayCSV[41],
				'Causal_no_continua_proceso_Otro': arrayCSV[42],
				'Estado_Liquidacion': arrayCSV[43],
				'Estado_Liquidacion_Otro': arrayCSV[44],
				'Estado_pago': arrayCSV[45],
				'Estado_pago_Otro': arrayCSV[46],
				'No_esta_interesado': arrayCSV[47],
				'No_esta_interesado_Otro': arrayCSV[48],
				'Esperando_decision': arrayCSV[49],
				'Esperando_decision_Otro': arrayCSV[50],
				'Interesado_en_otro_tipo_de_educacion': arrayCSV[51],
				'Interesado_en_otro_tipo_de_educacion_Otro': arrayCSV[52],
				'No_cumple_requisitos': arrayCSV[53],
				'No_cumple_requisitos_Otro': arrayCSV[54]
				}

		return [tupla]
	
def run(nombre,Fecha):

	gcs_path = 'gs://ct-cesde' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "10",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(nombre,skip_header_lines=1)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	
	transformed | 'Escritura a BigQuery Telefonia' >> beam.io.WriteToBigQuery(
		gcs_project + ":cesde." + "hora_hora", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")