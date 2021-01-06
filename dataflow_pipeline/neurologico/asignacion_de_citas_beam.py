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
from datetime import date
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



TABLE_SCHEMA = ('FECHA:STRING,'
                'SEDE:STRING, '
                'PROFESIONAL:STRING, '
                'FECHA_DE_LA_CITA:STRING, '
                'HORA_DE_LA_CITA:STRING, '
                'FECHA_FINAL_DE_LA_CITA:STRING, '
                'HORA_FINAL_DE_LA_CITA:STRING, '
                'SERVICIO:STRING, '
                'PACIENTE:STRING, '
                'TELEFONO:STRING, '
                'TELEFONO_ALTERNO:STRING, '
                'EMAIL:STRING, '
                'FECHA_DE_LA_SOLICITUD:STRING, '
                'HORA_DE_LA_SOLICITUD:STRING, '
                'ASIGNADA_POR:STRING, '
                'TIPO_DE_ATENCION:STRING, '
                'NUMERO_DE_IDENTIFICACION:STRING, '
                'TIPO_DE_IDENTIFICACION:STRING, '
                'TELEFONO_FIJO:STRING, '
                'TELEFONO_FIJO_2:STRING, '
                'FECHA_DE_NACIMIENTO:STRING, '
                'EPS:STRING, '
                'SEXO:STRING, '
                'DIRECCION_DE_RESIDENCIA:STRING, '
                'CIUDAD_MUNICIPIO:STRING, '
                'DEPARTAMENTO_ESTADO:STRING, '
                'PACIENTE_NUEVO:STRING, '
                'CITAPREVIA:STRING, '
                'OPORTUNIDAD:STRING, '
                'OBSERVACIONES:STRING, '
                'ESTADO:STRING, '
                'OBSERVACIONES_EN_LA_EDICION:STRING, '
                'FECHA_DE_LA_EDICION:STRING, '
                'HORA_DE_LA_EDICION:STRING, '
                'ESTADO_DE_LA_LLAMADA:STRING, '
                'NRO_INTENTOS_DE_LA_LLAMADA:STRING, '
                'NRO_LLAMADAS_CONTESTADAS:STRING '

	            )

class formatearData(beam.DoFn):
    
        def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split(';')

		tupla= {'fecha' : self.mifecha,
                'SEDE' : arrayCSV[0],
                'PROFESIONAL' : arrayCSV[1],
                'FECHA_DE_LA_CITA' : arrayCSV[2],
                'HORA_DE_LA_CITA' : arrayCSV[3],
                'FECHA_FINAL_DE_LA_CITA' : arrayCSV[4],
                'HORA_FINAL_DE_LA_CITA' : arrayCSV[5],
                'SERVICIO' : arrayCSV[6],
                'PACIENTE' : arrayCSV[7],
                'TELEFONO' : arrayCSV[8],
                'TELEFONO_ALTERNO' : arrayCSV[9],
                'EMAIL' : arrayCSV[10],
                'FECHA_DE_LA_SOLICITUD' : arrayCSV[11],
                'HORA_DE_LA_SOLICITUD' : arrayCSV[12],
                'ASIGNADA_POR' : arrayCSV[13],
                'TIPO_DE_ATENCION' : arrayCSV[14],
                'NUMERO_DE_IDENTIFICACION' : arrayCSV[15],
                'TIPO_DE_IDENTIFICACION' : arrayCSV[16],
                'TELEFONO_FIJO' : arrayCSV[17],
                'TELEFONO_FIJO_2' : arrayCSV[18],
                'FECHA_DE_NACIMIENTO' : arrayCSV[19],
                'EPS' : arrayCSV[20],
                'SEXO' : arrayCSV[21],
                'DIRECCION_DE_RESIDENCIA' : arrayCSV[22],
                'CIUDAD_MUNICIPIO' : arrayCSV[23],
                'DEPARTAMENTO_ESTADO' : arrayCSV[24],
                'PACIENTE_NUEVO' : arrayCSV[25],
                'CITAPREVIA' : arrayCSV[26],
                'OPORTUNIDAD' : arrayCSV[27],
                'OBSERVACIONES' : arrayCSV[28],
                'ESTADO' : arrayCSV[29],
                'OBSERVACIONES_EN_LA_EDICION' : arrayCSV[30],
                'FECHA_DE_LA_EDICION' : arrayCSV[31],
                'HORA_DE_LA_EDICION' : arrayCSV[32],
                'ESTADO_DE_LA_LLAMADA' : arrayCSV[33],
                'NRO_INTENTOS_DE_LA_LLAMADA' : arrayCSV[34],
                'NRO_LLAMADAS_CONTESTADAS' : arrayCSV[35]
                }
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-neurologico" #Definicion de la raiz del bucket
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
	transformed | 'Escritura a BigQuery B2chat' >> beam.io.WriteToBigQuery(
		gcs_project + ":Neurologico.Asignacion_citas", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")