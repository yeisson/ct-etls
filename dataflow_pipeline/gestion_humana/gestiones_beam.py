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
                'ID_GESTION:STRING, '
                'PERSONA_QUE_REPORTA:STRING, '
                'SEDE:STRING, '
                'CEDULA:STRING, '
                'NOMBRE_COMPLETO:STRING, '
                'EPS:STRING, '
                'COD_CENTRO_COSTOS:STRING, '
                'NOMBRE_CENTRO_COSTOS:STRING, '
                'TIPO_INCAPACIDAD:STRING, '
                'BUSCAR_DIAGNOSTICO:STRING, '
                'CODIGO_DIAGNOSTICO:STRING, '
                'NOMBRE_DIAGNOSTICO:STRING, '
                'NUMERO_INCAPACIDAD:STRING, '
                'DIAS_INCAPACIDAD:STRING, '
                'FECHA_INICIAL_LIQUIDACION:STRING, '
                'FECHA_REAL_INICIAL:STRING, '
                'FECHA_FINAL_INCAPACIDAD:STRING, '
                'AJUSTE_INCAPACIDAD_SALARIO_MINIMO:STRING, '
                'PRORROGA:STRING, '
                'DOCUMENTO_PRORROGA:STRING, '
                'ACCIDENTE_TRANSITO:STRING, '
                'IBC_MES_ANTERIOR:STRING, '
                'IBC_COTIZACION_ESPECIFICO:STRING, '
                'FECHA_RECIBIDO_INCAPACIDAD:STRING, '
                'MES_APLICACO_NOMINA:STRING, '
                'VOBO_EJECUCION_RPA:STRING, '
                'TIENE_TRANSCRIPCION:STRING, '
                'DOCUMENTACION_TRANSCRIPCION:STRING, '
                'DOCUMENTO_PENDIENTE:STRING, '
                'CORREO_RESPONSABLE:STRING, '
                'FECHA_NOTIFICACION_DOCS_INCOMPLETOS:STRING, '
                'FECHA_ENVIO_DOCS_INCOMPLETOS:STRING, '
                'FECHA_ENVIO_DOCS_CORRECTO:STRING, '
                'DOCUMENTACION_COMPLETA:STRING, '
                'NRO_INCAPACIDAD:STRING, '
                'INSERT_DATE:STRING, '
                'TRANSCRITO_POR:STRING, '
                'FECHA_SOL_TRANSCRIPCION:STRING, '
                'FECHA_TRANSCRIPCION:STRING, '
                'FECHA_SOL_COBRO:STRING, '
                'FECHA_COBRO:STRING, '
                'VALOR_PAGADO:STRING, '
                'FECHA_PAGO:STRING, '
                'ESTADO_GOSSEM:STRING, '
                'MARCA_GOSEM:STRING, '
                'FECHA_PROCESO_GOSSEM:STRING, '
                'DURACION_GOSSEM:STRING '


                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'ID_GESTION' : arrayCSV[0],
                'PERSONA_QUE_REPORTA' : arrayCSV[1],
                'SEDE' : arrayCSV[2],
                'CEDULA' : arrayCSV[3],
                'NOMBRE_COMPLETO' : arrayCSV[4],
                'EPS' : arrayCSV[5],
                'COD_CENTRO_COSTOS' : arrayCSV[6],
                'NOMBRE_CENTRO_COSTOS' : arrayCSV[7],
                'TIPO_INCAPACIDAD' : arrayCSV[8],
                'BUSCAR_DIAGNOSTICO' : arrayCSV[9],
                'CODIGO_DIAGNOSTICO' : arrayCSV[10],
                'NOMBRE_DIAGNOSTICO' : arrayCSV[11],
                'NUMERO_INCAPACIDAD' : arrayCSV[12],
                'DIAS_INCAPACIDAD' : arrayCSV[13],
                'FECHA_INICIAL_LIQUIDACION' : arrayCSV[14],
                'FECHA_REAL_INICIAL' : arrayCSV[15],
                'FECHA_FINAL_INCAPACIDAD' : arrayCSV[16],
                'AJUSTE_INCAPACIDAD_SALARIO_MINIMO' : arrayCSV[17],
                'PRORROGA' : arrayCSV[18],
                'DOCUMENTO_PRORROGA' : arrayCSV[19],
                'ACCIDENTE_TRANSITO' : arrayCSV[20],
                'IBC_MES_ANTERIOR' : arrayCSV[21],
                'IBC_COTIZACION_ESPECIFICO' : arrayCSV[22],
                'FECHA_RECIBIDO_INCAPACIDAD' : arrayCSV[23],
                'MES_APLICACO_NOMINA' : arrayCSV[24],
                'VOBO_EJECUCION_RPA' : arrayCSV[25],
                'TIENE_TRANSCRIPCION' : arrayCSV[26],
                'DOCUMENTACION_TRANSCRIPCION' : arrayCSV[27],
                'DOCUMENTO_PENDIENTE' : arrayCSV[28],
                'CORREO_RESPONSABLE' : arrayCSV[29],
                'FECHA_NOTIFICACION_DOCS_INCOMPLETOS' : arrayCSV[30],
                'FECHA_ENVIO_DOCS_INCOMPLETOS' : arrayCSV[31],
                'FECHA_ENVIO_DOCS_CORRECTO' : arrayCSV[32],
                'DOCUMENTACION_COMPLETA' : arrayCSV[33],
                'NRO_INCAPACIDAD' : arrayCSV[34],
                'INSERT_DATE' : arrayCSV[35],
                'TRANSCRITO_POR' : arrayCSV[36],
                'FECHA_SOL_TRANSCRIPCION' : arrayCSV[37],
                'FECHA_TRANSCRIPCION' : arrayCSV[38],
                'FECHA_SOL_COBRO' : arrayCSV[39],
                'FECHA_COBRO' : arrayCSV[40],
                'VALOR_PAGADO' : arrayCSV[41],
                'FECHA_PAGO' : arrayCSV[42],
                'ESTADO_GOSSEM' : arrayCSV[43],
                'MARCA_GOSEM' : arrayCSV[44],
                'FECHA_PROCESO_GOSSEM' : arrayCSV[45],
                'DURACION_GOSSEM' : arrayCSV[46]

				}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-gto" #Definicion de la raiz del bucket
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
	])
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/gestiones" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery gestion_humana' >> beam.io.WriteToBigQuery(
        gcs_project + ":gestion_humana.Gestiones",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run();jobObject.wait_until_finish()

    
    # jobID = jobObject.job_id()

	return ("Corrio sin problema")
