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
                'DOCUMENTO:STRING, '
                'NUM_OBLIGACION:STRING, '
                'ID_CAMPANA:STRING, '
                'ID_SEGMENTO:STRING, '
                'ID_COD_GESTION:STRING, '
                'ID_COD_CAUSAL:STRING, '
                'ID_COD_SUBCAUSAL:STRING, '
                # 'OBSERVACION:STRING, '
                'VLR_PROMESA:STRING, '
                'FECHA_PROMESA:STRING, '
                'NUM_CUOTAS:STRING, '
                'TELEFONO:STRING, '
                'FECHA_GESTION:STRING, '
                'USUARIO_GESTOR:STRING, '
                'OPT_1:STRING, '
                'OPT_2:STRING, '
                'OPT_3:STRING, '
                'OPT_4:STRING, '
                'OPT_5:STRING, '
                'CUADRANTE:STRING, '
                'MODALIDAD_PAGO:STRING '

                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'ID_GESTION' : arrayCSV[0],
                'DOCUMENTO' : arrayCSV[1],
                'NUM_OBLIGACION' : arrayCSV[2],
                'ID_CAMPANA' : arrayCSV[3],
                'ID_SEGMENTO' : arrayCSV[4],
                'ID_COD_GESTION' : arrayCSV[5],
                'ID_COD_CAUSAL' : arrayCSV[6],
                'ID_COD_SUBCAUSAL' : arrayCSV[7],
                # 'OBSERVACION' : arrayCSV[8],
                'VLR_PROMESA' : arrayCSV[8],
                'FECHA_PROMESA' : arrayCSV[9],
                'NUM_CUOTAS' : arrayCSV[10],
                'TELEFONO' : arrayCSV[11],
                'FECHA_GESTION' : arrayCSV[12],
                'USUARIO_GESTOR' : arrayCSV[13],
                'OPT_1' : arrayCSV[14],
                'OPT_2' : arrayCSV[15],
                'OPT_3' : arrayCSV[16],
                'OPT_4' : arrayCSV[17],
                'OPT_5' : arrayCSV[18],
                'CUADRANTE' : arrayCSV[19],
                'MODALIDAD_PAGO' : arrayCSV[20]


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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/gestiones/Unificadas_gestiones" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery unificadas' >> beam.io.WriteToBigQuery(
        gcs_project + ":unificadas.Gestiones",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run();jobObject.wait_until_finish()

    
    # jobID = jobObject.job_id()

	return ("Corrio sin problema")