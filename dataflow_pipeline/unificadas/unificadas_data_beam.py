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
                'ID_DATA:STRING, '
                'ID_CAMPANA:STRING, '
                'ZONA:STRING, '
                'COD_CIUDAD:STRING, '
                'DOCUMENTO:STRING, '
                'COD_INTERNO:STRING, '
                'TIPO_COMPRADOR:STRING, '
                'CUSTOMER_CLASS:STRING, '
                'CUPO:STRING, '
                'NUM_OBLIGACION:STRING, '
                'VLR_FACTURA:STRING, '
                'FECHA_FACTURA:STRING, '
                'FECHA_VENCIMIENTO:STRING, '
                'VLR_SALDO_CARTERA:STRING, '
                'DIAS_VENCIMIENTO:STRING, '
                'CAMPANA_ORIG:STRING, '
                'ULT_CAMPANA:STRING, '
                'CODIGO:STRING, '
                'ABOGADO:STRING, '
                'DIVISION:STRING, '
                'PAIS:STRING, '
                'FECHA_PROX_CONFERENCIA:STRING, '
                'COD_GESTION:STRING, '
                'FECHA_GESTION:STRING, '
                'FECHA_PROMESA_PAGO:STRING, '
                'ACTIVIDAD_ECONOMICA:STRING, '
                'SALDO_CAPITAL:STRING, '
                'NUM_CUOTAS:STRING, '
                'NUM_CUOTAS_PAGADAS:STRING, '
                'NUM_CUOTAS_FALTANTES:STRING, '
                'NUM_CUOTAS_MORA:STRING, '
                'CANT_VECES_MORA:STRING, '
                'FECHA_ULT_PAGO:STRING, '
                'SALDO_TOTAL_VENCIDO:STRING, '
                'PLAN:STRING, '
                'COD_CONSECIONARIO:STRING, '
                'CONCESIONARIO:STRING, '
                'COD_GESTION_ANT:STRING, '
                'GRABADOR:STRING, '
                'ESTADO:STRING, '
                'FECHA_CARGUE:STRING, '
                'USUARIO_CARGUE:STRING, '
                'INTERES_MORA:STRING, '
                'VLR_CUOTAS_VENCIDAS:STRING '

                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'ID_DATA' : arrayCSV[0],
                'ID_CAMPANA' : arrayCSV[1],
                'ZONA' : arrayCSV[2],
                'COD_CIUDAD' : arrayCSV[3],
                'DOCUMENTO' : arrayCSV[4],
                'COD_INTERNO' : arrayCSV[5],
                'TIPO_COMPRADOR' : arrayCSV[6],
                'CUSTOMER_CLASS' : arrayCSV[7],
                'CUPO' : arrayCSV[8],
                'NUM_OBLIGACION' : arrayCSV[9],
                'VLR_FACTURA' : arrayCSV[10],
                'FECHA_FACTURA' : arrayCSV[11],
                'FECHA_VENCIMIENTO' : arrayCSV[12],
                'VLR_SALDO_CARTERA' : arrayCSV[13],
                'DIAS_VENCIMIENTO' : arrayCSV[14],
                'CAMPANA_ORIG' : arrayCSV[15],
                'ULT_CAMPANA' : arrayCSV[16],
                'CODIGO' : arrayCSV[17],
                'ABOGADO' : arrayCSV[18],
                'DIVISION' : arrayCSV[19],
                'PAIS' : arrayCSV[20],
                'FECHA_PROX_CONFERENCIA' : arrayCSV[21],
                'COD_GESTION' : arrayCSV[22],
                'FECHA_GESTION' : arrayCSV[23],
                'FECHA_PROMESA_PAGO' : arrayCSV[24],
                'ACTIVIDAD_ECONOMICA' : arrayCSV[25],
                'SALDO_CAPITAL' : arrayCSV[26],
                'NUM_CUOTAS' : arrayCSV[27],
                'NUM_CUOTAS_PAGADAS' : arrayCSV[28],
                'NUM_CUOTAS_FALTANTES' : arrayCSV[29],
                'NUM_CUOTAS_MORA' : arrayCSV[30],
                'CANT_VECES_MORA' : arrayCSV[31],
                'FECHA_ULT_PAGO' : arrayCSV[32],
                'SALDO_TOTAL_VENCIDO' : arrayCSV[33],
                'PLAN' : arrayCSV[34],
                'COD_CONSECIONARIO' : arrayCSV[35],
                'CONCESIONARIO' : arrayCSV[36],
                'COD_GESTION_ANT' : arrayCSV[37],
                'GRABADOR' : arrayCSV[38],
                'ESTADO' : arrayCSV[39],
                'FECHA_CARGUE' : arrayCSV[40],
                'USUARIO_CARGUE' : arrayCSV[41],
                'INTERES_MORA' : arrayCSV[42],
                'VLR_CUOTAS_VENCIDAS' : arrayCSV[43]

				}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-unificadas" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/data_unificadas/Unificadas_data" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery unificadas' >> beam.io.WriteToBigQuery(
        gcs_project + ":unificadas.Data",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run();jobObject.wait_until_finish()

    
    # jobID = jobObject.job_id()

	return ("Corrio sin problema")