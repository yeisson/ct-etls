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



TABLE_SCHEMA = (
                'IDKEY:STRING, '
                'FECHA:STRING, '
                'NOMBRE_DE_LA_CUENTA:STRING, '
                'NUMERO_DE_DOCUMENTO:STRING, '
                'CORREO_ELECTRONICO_WEB:STRING, '
                'NIVEL_DE_SERVICIO:STRING, '
                'FECHA_ATENCION:STRING, '
                'TIEMPO_DE_ATENCION:STRING, '
                'MOTIVO_DE_LA_DEVOLUCION:STRING, '
                'ESTADO:STRING, '
                'DIAS_SOLUCION:STRING, '
                'FECHA_SOLUCION:STRING, '
                'NUMERO_DEL_CASO:STRING, '
                'ORIGEN_DEL_CASO:STRING, '
                'PROCESO:STRING, '
                'TIPIFICACION_NIVEL_2:STRING, '
                'PLU:STRING, '
                'OTRO_MOTIVO_DEVOLUCION:STRING, '
                'MEDIO_DE_REEMBOLSO:STRING, '
                'VALOR_A_DEVOLVER:STRING, '
                'VALOR_RESARCIMIENTO:STRING, '
                'NUMERO_DE_CASO_PRINCIPAL:STRING, '
                'FECHA_HORA_DE_APERTURA:STRING, '
                'NIVEL_DE_ATENCION:STRING '

	            )

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
                'NOMBRE_DE_LA_CUENTA' : arrayCSV[0],
                'NUMERO_DE_DOCUMENTO' : arrayCSV[1],
                'CORREO_ELECTRONICO_WEB' : arrayCSV[2],
                'NIVEL_DE_SERVICIO' : arrayCSV[3],
                'FECHA_ATENCION' : arrayCSV[4],
                'TIEMPO_DE_ATENCION' : arrayCSV[5],
                'MOTIVO_DE_LA_DEVOLUCION' : arrayCSV[6],
                'ESTADO' : arrayCSV[7],
                'DIAS_SOLUCION' : arrayCSV[8],
                'FECHA_SOLUCION' : arrayCSV[9],
                'NUMERO_DEL_CASO' : arrayCSV[10],
                'ORIGEN_DEL_CASO' : arrayCSV[11],
                'PROCESO' : arrayCSV[12],
                'TIPIFICACION_NIVEL_2' : arrayCSV[13],
                'PLU' : arrayCSV[14],
                'OTRO_MOTIVO_DEVOLUCION' : arrayCSV[15],
                'MEDIO_DE_REEMBOLSO' : arrayCSV[16],
                'VALOR_A_DEVOLVER' : arrayCSV[17],
                'VALOR_RESARCIMIENTO' : arrayCSV[18],
                'NUMERO_DE_CASO_PRINCIPAL' : arrayCSV[19],
                'FECHA_HORA_DE_APERTURA' : arrayCSV[20],
                'NIVEL_DE_ATENCION' : arrayCSV[21]

                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-linead" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery SF_Nivel_atencion' >> beam.io.WriteToBigQuery(
		gcs_project + ":linea_directa.SF_Nivel_atencion", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")