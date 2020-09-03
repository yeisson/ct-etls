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



TABLE_SCHEMA = ('IDKEY:STRING, '
                'CAMPANA:STRING, '
                'NOMBRE_DE_CUENTA:STRING, '
                'TRAFFIC_SOURCE:STRING, '
                'NOMBRE_DE_COMUNICACION:STRING, '
                'COMUNICACION_PROGRAMADA_PARA:STRING, '
                'FECHA_DE_INICIO_DE_LA_COMUNICACION:STRING, '
                'PLANTILLA_DE_COMUNICACION:STRING, '
                'DE:STRING, '
                'A:STRING, '
                'ID_DEL_MENSAJE:STRING, '
                'ENVIAR_EL:STRING, '
                'PREFIJO_DE_PAIS:STRING, '
                'NOMBRE_DE_PAIS:STRING, '
                'NOMBRE_DE_RED:STRING, '
                'ORIGINAL_MCC:STRING, '
                'ORIGINAL_MNC:STRING, '
                'MCC_MNC_ORIGINAL:STRING, '
                'PRECIO_DE_COMPRA:STRING, '
                'MONEDA_DE_COMPRA:STRING, '
                'PRECIO_DE_VENTA:STRING, '
                'MONEDA_DE_VENTA:STRING, '
                'TIPO_DE_MENSAJE:STRING, '
                'SMS_TYPE:STRING, '
                'ESTADO:STRING, '
                'RAZON:STRING, '
                'ACCION:STRING, '
                'GRUPO_DE_ERROR:STRING, '
                'NOMBRE_DE_ERROR:STRING, '
                'HECHO_EL:STRING, '
                'TEXTO:STRING, '
                'MESSAGE_LENGTH:STRING, '
                'CONTEO_DE_MENSAJES:STRING, '
                'NOMBRE_DE_SERVICIO:STRING, '
                'NOMBRE_DE_USUARIO:STRING, '
                'ID_DE_MENSAJE_SINCRONIZADO:STRING, '
                'CLICKS:STRING, '
                'DATA_PAYLOAD:STRING '


	            )

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
                        'campana' : self.mifecha,
                        'NOMBRE_DE_CUENTA' : arrayCSV[0],
                        'TRAFFIC_SOURCE' : arrayCSV[1],
                        'NOMBRE_DE_COMUNICACION' : arrayCSV[2],
                        'COMUNICACION_PROGRAMADA_PARA' : arrayCSV[3],
                        'FECHA_DE_INICIO_DE_LA_COMUNICACION' : arrayCSV[4],
                        'PLANTILLA_DE_COMUNICACION' : arrayCSV[5],
                        'DE' : arrayCSV[6],
                        'A' : arrayCSV[7],
                        'ID_DEL_MENSAJE' : arrayCSV[8],
                        'ENVIAR_EL' : arrayCSV[9],
                        'PREFIJO_DE_PAIS' : arrayCSV[10],
                        'NOMBRE_DE_PAIS' : arrayCSV[11],
                        'NOMBRE_DE_RED' : arrayCSV[12],
                        'ORIGINAL_MCC' : arrayCSV[13],
                        'ORIGINAL_MNC' : arrayCSV[14],
                        'MCC_MNC_ORIGINAL' : arrayCSV[15],
                        'PRECIO_DE_COMPRA' : arrayCSV[16],
                        'MONEDA_DE_COMPRA' : arrayCSV[17],
                        'PRECIO_DE_VENTA' : arrayCSV[18],
                        'MONEDA_DE_VENTA' : arrayCSV[19],
                        'TIPO_DE_MENSAJE' : arrayCSV[20],
                        'SMS_TYPE' : arrayCSV[21],
                        'ESTADO' : arrayCSV[22],
                        'RAZON' : arrayCSV[23],
                        'ACCION' : arrayCSV[24],
                        'GRUPO_DE_ERROR' : arrayCSV[25],
                        'NOMBRE_DE_ERROR' : arrayCSV[26],
                        'HECHO_EL' : arrayCSV[27],
                        'TEXTO' : arrayCSV[28],
                        'MESSAGE_LENGTH' : arrayCSV[29],
                        'CONTEO_DE_MENSAJES' : arrayCSV[30],
                        'NOMBRE_DE_SERVICIO' : arrayCSV[31],
                        'NOMBRE_DE_USUARIO' : arrayCSV[32],
                        'ID_DE_MENSAJE_SINCRONIZADO' : arrayCSV[33],
                        'CLICKS' : arrayCSV[34],
                        'DATA_PAYLOAD' : arrayCSV[35]


                        }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-ucc" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo,skip_header_lines=1)
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery ucc' >> beam.io.WriteToBigQuery(
		gcs_project + ":ucc.sms_doble_via", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")