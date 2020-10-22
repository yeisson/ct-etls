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
                'NUMERO_DEL_CASO:STRING, '
                'NOMBRE_CUENTA:STRING, '
                'APELLIDO_CUENTA:STRING, '
                'TIPO_DE_CLIENTE:STRING, '
                'NOMBRE_DE_TRANSCRIPCION_DE_CHAT:STRING, '
                'DESCRIPCION:STRING, '
                'ESTADO:STRING, '
                'HABILIDAD_PRINCIPAL_NOMBRE:STRING, '
                'CICLO_DE_VIDA:STRING, '
                'CORREO_ELECTRONICO:STRING, '
                'TIEMPO_DE_ESPERA:STRING, '
                'ABANDONADO_DESPUES_DE:STRING, '
                'HORA_DE_SOLICITUD:STRING, '
                'HORA_DE_INICIO:STRING, '
                'HORA_DE_FINALIZACION:STRING, '
                'TIEMPO_MAXIMO_DE_RESPUESTA_DEL_AGENTE:STRING, '
                'TIEMPO_MAXIMO_DE_RESPUESTA_DEL_VISITANTE:STRING, '
                'TIEMPO_MEDIO_DE_RESPUESTA_DEL_AGENTE:STRING, '
                'TIEMPO_MEDIO_DE_RESPUESTA_DEL_VISITANTE:STRING, '
                'FINALIZADO_POR:STRING, '
                'DURACION_DEL_CHAT:STRING, '
                'FECHA_HORA_DE_APERTURA:STRING, '
                'AGENTE_SAC:STRING '


	            )

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				'fecha' : self.mifecha,
                'NUMERO_DEL_CASO' : arrayCSV[0],
                'NOMBRE_CUENTA' : arrayCSV[1],
                'APELLIDO_CUENTA' : arrayCSV[2],
                'TIPO_DE_CLIENTE' : arrayCSV[3],
                'NOMBRE_DE_TRANSCRIPCION_DE_CHAT' : arrayCSV[4],
                'DESCRIPCION' : arrayCSV[5],
                'ESTADO' : arrayCSV[6],
                'HABILIDAD_PRINCIPAL_NOMBRE' : arrayCSV[7],
                'CICLO_DE_VIDA' : arrayCSV[8],
                'CORREO_ELECTRONICO' : arrayCSV[9],
                'TIEMPO_DE_ESPERA' : arrayCSV[10],
                'ABANDONADO_DESPUES_DE' : arrayCSV[11],
                'HORA_DE_SOLICITUD' : arrayCSV[12],
                'HORA_DE_INICIO' : arrayCSV[13],
                'HORA_DE_FINALIZACION' : arrayCSV[14],
                'TIEMPO_MAXIMO_DE_RESPUESTA_DEL_AGENTE' : arrayCSV[15],
                'TIEMPO_MAXIMO_DE_RESPUESTA_DEL_VISITANTE' : arrayCSV[16],
                'TIEMPO_MEDIO_DE_RESPUESTA_DEL_AGENTE' : arrayCSV[17],
                'TIEMPO_MEDIO_DE_RESPUESTA_DEL_VISITANTE' : arrayCSV[18],
                'FINALIZADO_POR' : arrayCSV[19],
                'DURACION_DEL_CHAT' : arrayCSV[20],
                'FECHA_HORA_DE_APERTURA' : arrayCSV[21],
                'AGENTE_SAC' : arrayCSV[22]

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
	transformed | 'Escritura a BigQuery SF_Chats_agente' >> beam.io.WriteToBigQuery(
		gcs_project + ":linea_directa.SF_Chats_por_agente", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")




