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
from apache_beam.io import WriteToText, textio
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
	'idkey:STRING, '
	'fecha:STRING, '
	'consecutivo_documento_deudor:STRING, '
	'valor_cuota:STRING, '
	'clasificacion_producto:STRING, '
	'nit:STRING, '
	'nombres:STRING, '
	'numero_documento:STRING, '
	'tipo_producto:STRING, '
	'fecha_pago_cuota:STRING, '
	'modalidad:STRING, '
	'nombre_de_producto:STRING, '
	'plan:STRING, '
	'fecha_de_perfeccionamiento:STRING, '
	'fecha_vencimiento_def:STRING, '
	'numero_cuotas:STRING, '
	'cant_oblig:STRING, '
	'cuotas_en_mora:STRING, '
	'dia_de_vencimiento_de_cuota:STRING, '
	'valor_obligacion:STRING, '
	'valor_vencido:STRING, '
	'saldo_activo:STRING, '
	'saldo_orden:STRING, '
	'regional:STRING, '
	'ciudad:STRING, '
	'oficina_radicacion:STRING, '
	'grabador:STRING, '
	'nombre_asesor:STRING, '
	'codigo_agente:STRING, '
	'nombre_abogado:STRING, '
	'codigo_abogado:STRING, '
	'fecha_ultima_gestion_prejuridica:STRING, '
	'ultimo_codigo_de_gestion_prejuridico:STRING, '
	'ultimo_codigo_de_gestion_paralelo:STRING, '
	'desc_ultimo_codigo_de_gestion_prejuridico:STRING, '
	'codigo_anterior_de_gestion_prejuridico:STRING, '
	'desc_codigo_anterior_de_gestion_prejuridico:STRING, '
	'ultima_fecha_pago:STRING, '
	'ultima_fecha_de_actuacion_juridica:STRING, '
	'fecha_ultima_gestion_juridica:STRING, '
	'ejec_ultimo_codigo_de_gestion_juridico:STRING, '
	'rest_ultimo_codigo_gestion_juridico:STRING, '
	'desc_ultimo_codigo_de_gestion_juridico:STRING, '
	'tipo_de_cartera:STRING, '
	'dias_mora:STRING, '
	'calificacion:STRING, '
	'radicacion:STRING, '
	'estado_de_la_obligacion:STRING, '
	'fondo_nacional_garantias:STRING, '
	'region:STRING, '
	'segmento:STRING, '
	'fecha_importacion:STRING, '
	'titular_universal:STRING, '
	'negocio_titutularizado:STRING, '
	'red:STRING, '
	'fecha_traslado_para_cobro:STRING, '
	'calificacion_real:STRING, '
	'fecha_ultima_facturacion:STRING, '
	'cuadrante:STRING, '
	'causal:STRING, '
	'sector_economico:STRING, '
	'fecha_promesa:STRING, '
	'endeudamiento:STRING, '
	'probabilidad_de_propension_de_pago:STRING, '
	'priorizacion_final:STRING, '
	'grupo_de_priorizacion:STRING, '
	'ultimo_abogado:STRING, '
	'credito_cobertura_frech:STRING, '
	'endeudamiento_sufi:STRING '
)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),	#datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'consecutivo_documento_deudor': arrayCSV[0],
				'valor_cuota': arrayCSV[1],
				'clasificacion_producto': arrayCSV[2],
				'nit': arrayCSV[3],
				'nombres': arrayCSV[4],
				'numero_documento': arrayCSV[5],
				'tipo_producto': arrayCSV[6],
				'fecha_pago_cuota': arrayCSV[7],
				'modalidad': arrayCSV[8],
				'nombre_de_producto': arrayCSV[9],
				'plan': arrayCSV[10],
				'fecha_de_perfeccionamiento': arrayCSV[11],
				'fecha_vencimiento_def': arrayCSV[12],
				'numero_cuotas': arrayCSV[13],
				'cant_oblig': arrayCSV[14],
				'cuotas_en_mora': arrayCSV[15],
				'dia_de_vencimiento_de_cuota': arrayCSV[16],
				'valor_obligacion': arrayCSV[17],
				'valor_vencido': arrayCSV[18],
				'saldo_activo': arrayCSV[19],
				'saldo_orden': arrayCSV[20],
				'regional': arrayCSV[21],
				'ciudad': arrayCSV[22],
				'oficina_radicacion': arrayCSV[23],
				'grabador': arrayCSV[24],
				'nombre_asesor': arrayCSV[25],
				'codigo_agente': arrayCSV[26],
				'nombre_abogado': arrayCSV[27],
				'codigo_abogado': arrayCSV[28],
				'fecha_ultima_gestion_prejuridica': arrayCSV[29],
				'ultimo_codigo_de_gestion_prejuridico': arrayCSV[30],
				'ultimo_codigo_de_gestion_paralelo': arrayCSV[31],
				'desc_ultimo_codigo_de_gestion_prejuridico': arrayCSV[32],
				'codigo_anterior_de_gestion_prejuridico': arrayCSV[33],
				'desc_codigo_anterior_de_gestion_prejuridico': arrayCSV[34],
				'ultima_fecha_pago': arrayCSV[35],
				'ultima_fecha_de_actuacion_juridica': arrayCSV[36],
				'fecha_ultima_gestion_juridica': arrayCSV[37],
				'ejec_ultimo_codigo_de_gestion_juridico': arrayCSV[38],
				'rest_ultimo_codigo_gestion_juridico': arrayCSV[39],
				'desc_ultimo_codigo_de_gestion_juridico': arrayCSV[40],
				'tipo_de_cartera': arrayCSV[41],
				'dias_mora': arrayCSV[42],
				'calificacion': arrayCSV[43],
				'radicacion': arrayCSV[44],
				'estado_de_la_obligacion': arrayCSV[45],
				'fondo_nacional_garantias': arrayCSV[46],
				'region': arrayCSV[47],
				'segmento': arrayCSV[48],
				'fecha_importacion': arrayCSV[49],
				'titular_universal': arrayCSV[50],
				'negocio_titutularizado': arrayCSV[51],
				'red': arrayCSV[52],
				'fecha_traslado_para_cobro': arrayCSV[53],
				'calificacion_real': arrayCSV[54],
				'fecha_ultima_facturacion': arrayCSV[55],
				'cuadrante': arrayCSV[56],
				'causal': arrayCSV[57],
				'sector_economico': arrayCSV[58],
				'fecha_promesa': arrayCSV[59],
				'endeudamiento': arrayCSV[60],
				'probabilidad_de_propension_de_pago': arrayCSV[61],
				'priorizacion_final': arrayCSV[62],
				'grupo_de_priorizacion': arrayCSV[63],
				'ultimo_abogado': arrayCSV[64],
				'credito_cobertura_frech': arrayCSV[65],
				'endeudamiento_sufi': arrayCSV[66]
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	# pipeline =  beam.Pipeline(runner="DirectRunner")
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
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
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT")
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/prejuridico/info_carga_banco_prej",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_admin.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



