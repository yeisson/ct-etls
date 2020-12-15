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
	'NOPLACAVEH:STRING, '
	'ID_ASEGURADO:STRING, '
	'NOMBRE_ASEGURADO:STRING, '
	'TELFIJOASEGURADO:STRING, '
	'NOMBRE_DEL_CONTACTO:STRING, '
	'CELULAR:STRING, '
	'EMAILASEGURADO:STRING, '
	'EMAILASEGURADOCONFIRMADO:STRING, '
	'DIRECCIONASEGURADO:STRING, '
	'DEPARTAMENTO_CUIDAD:STRING, '
	'CODIGODANECIUDADDPTOASEG:STRING, '
	'CODIGOFASECOLDAVEH:STRING, '
	'MODELOANOVEH:STRING, '
	'NOPLACAVEH2:STRING, '
	'NOMOTORVEH:STRING, '
	'NOCHASISVEH:STRING, '
	'NOVINVEH:STRING, '
	'USOVEH:STRING, '
	'LINEAVEH:STRING, '
	'CLASE_VEH:STRING, '
	'TIPO_VEH:STRING, '
	'NUMERO_DE_CREDITO:STRING, '
	'LINEA_NEGOCIO:STRING, '
	'VALOR_ASEGURADO:STRING, '
	'DIA_FACTURACION:STRING, '
	'TIPO_DE_CAMPANA:STRING, '
	'FECHA_INICIO_VIGENCIA:STRING, '
	'FECHA_FIN_DE_VIGENCIA:STRING, '
	'MARCA:STRING, '
	'PRIMA_ANUALIDAD_SIN_IVA:STRING, '
	'PRIMA_ANUALIDAD_CON_IVA:STRING, '
	'PRIMA_MENSUAL_SIN_IVA:STRING, '
	'PRIMA_MENSUAL_CON_IVA:STRING, '
	'FECHA_INICIO_ANUALIDAD:STRING, '
	'FECHA_FIN_ANUALIDAD:STRING, '
	'PRODUCTO_ELEGIDO:STRING, '
	'ESTADO_DEL_CREDITO:STRING, '
	'FECHA_DE_NACIMIENTO_DEL_CLIENTE:STRING, '
	'EDAD:STRING, '
	'SEXO:STRING, '
	'TIPO_DE_PERSONA:STRING, '
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
				'NOPLACAVEH' : arrayCSV[0],
				'ID_ASEGURADO' : arrayCSV[1],
				'NOMBRE_ASEGURADO' : arrayCSV[2],
				'TELFIJOASEGURADO' : arrayCSV[3],
				'NOMBRE_DEL_CONTACTO' : arrayCSV[4],
				'CELULAR' : arrayCSV[5],
				'EMAILASEGURADO' : arrayCSV[6],
				'EMAILASEGURADOCONFIRMADO' : arrayCSV[7],
				'DIRECCIONASEGURADO' : arrayCSV[8],
				'DEPARTAMENTO_CUIDAD' : arrayCSV[9],
				'CODIGODANECIUDADDPTOASEG' : arrayCSV[10],
				'CODIGOFASECOLDAVEH' : arrayCSV[11],
				'MODELOANOVEH' : arrayCSV[12],
				'NOPLACAVEH2' : arrayCSV[13],
				'NOMOTORVEH' : arrayCSV[14],
				'NOCHASISVEH' : arrayCSV[15],
				'NOVINVEH' : arrayCSV[16],
				'USOVEH' : arrayCSV[17],
				'LINEAVEH' : arrayCSV[18],
				'CLASE_VEH' : arrayCSV[19],
				'TIPO_VEH' : arrayCSV[20],
				'NUMERO_DE_CREDITO' : arrayCSV[21],
				'LINEA_NEGOCIO' : arrayCSV[22],
				'VALOR_ASEGURADO' : arrayCSV[23],
				'DIA_FACTURACION' : arrayCSV[24],
				'TIPO_DE_CAMPANA' : arrayCSV[25],
				'FECHA_INICIO_VIGENCIA' : arrayCSV[26],
				'FECHA_FIN_DE_VIGENCIA' : arrayCSV[27],
				'MARCA' : arrayCSV[28],
				'PRIMA_ANUALIDAD_SIN_IVA' : arrayCSV[29],
				'PRIMA_ANUALIDAD_CON_IVA' : arrayCSV[30],
				'PRIMA_MENSUAL_SIN_IVA' : arrayCSV[31],
				'PRIMA_MENSUAL_CON_IVA' : arrayCSV[32],
				'FECHA_INICIO_ANUALIDAD' : arrayCSV[33],
				'FECHA_FIN_ANUALIDAD' : arrayCSV[34],
				'PRODUCTO_ELEGIDO' : arrayCSV[35],
				'ESTADO_DEL_CREDITO' : arrayCSV[36],
				'FECHA_DE_NACIMIENTO_DEL_CLIENTE' : arrayCSV[37],
				'EDAD' : arrayCSV[38],
				'SEXO' : arrayCSV[39],
				'TIPO_DE_PERSONA' : arrayCSV[40],
				'ESTADO' : arrayCSV[41],
				'NOMBRE_DE_CAMPANA' : arrayCSV[42],
				'IPDIAL_CODE' : arrayCSV[43],

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
		gcs_project + ":telefonia.liberty_campanas_bdb", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



