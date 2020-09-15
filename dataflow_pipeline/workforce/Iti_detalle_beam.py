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

TABLE_SCHEMA = ('CENTRO_COSTOS:STRING, '
                'FECHA_MALLA:STRING, '
                'H_07_30_00:STRING, '
                'H_07_45_00:STRING, '
                'H_08_00_00:STRING, '
                'H_08_15_00:STRING, '
                'H_08_30_00:STRING, '
                'H_08_45_00:STRING, '
                'H_09_00_00:STRING, '
                'H_09_15_00:STRING, '
                'H_09_30_00:STRING, '
                'H_09_45_00:STRING, '
                'H_10_00_00:STRING, '
                'H_10_15_00:STRING, '
                'H_10_30_00:STRING, '
                'H_10_45_00:STRING, '
                'H_11_00_00:STRING, '
                'H_11_15_00:STRING, '
                'H_11_30_00:STRING, '
                'H_11_45_00:STRING, '
                'H_12_00_00:STRING, '
                'H_12_15_00:STRING, '
                'H_12_30_00:STRING, '
                'H_12_45_00:STRING, '
                'H_13_00_00:STRING, '
                'H_13_15_00:STRING, '
                'H_13_30_00:STRING, '
                'H_13_45_00:STRING, '
                'H_14_00_00:STRING, '
                'H_14_15_00:STRING, '
                'H_14_30_00:STRING, '
                'H_14_45_00:STRING, '
                'H_15_00_00:STRING, '
                'H_15_15_00:STRING, '
                'H_15_30_00:STRING, '
                'H_15_45_00:STRING, '
                'H_16_00_00:STRING, '
                'H_16_15_00:STRING, '
                'H_16_30_00:STRING, '
                'H_16_45_00:STRING, '
                'H_17_00_00:STRING, '
                'H_17_15_00:STRING, '
                'H_17_30_00:STRING, '
                'H_17_45_00:STRING, '
                'H_18_00_00:STRING, '
                'H_18_15_00:STRING, '
                'H_18_30_00:STRING, '
                'H_18_45_00:STRING, '
                'H_19_00_00:STRING, '
                'H_19_15_00:STRING, '
                'H_19_30_00:STRING, '
                'H_19_45_00:STRING, '
                'H_20_00_00:STRING, '
                'H_20_15_00:STRING, '
                'H_20_30_00:STRING, '
                'H_20_45_00:STRING, '
                'H_21_00_00:STRING '



                )

class formatearData(beam.DoFn):

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'CENTRO_COSTOS' : arrayCSV[0],
                'FECHA_MALLA' : arrayCSV[1],
                'H_07_30_00' : arrayCSV[2],
                'H_07_45_00' : arrayCSV[3],
                'H_08_00_00' : arrayCSV[4],
                'H_08_15_00' : arrayCSV[5],
                'H_08_30_00' : arrayCSV[6],
                'H_08_45_00' : arrayCSV[7],
                'H_09_00_00' : arrayCSV[8],
                'H_09_15_00' : arrayCSV[9],
                'H_09_30_00' : arrayCSV[10],
                'H_09_45_00' : arrayCSV[11],
                'H_10_00_00' : arrayCSV[12],
                'H_10_15_00' : arrayCSV[13],
                'H_10_30_00' : arrayCSV[14],
                'H_10_45_00' : arrayCSV[15],
                'H_11_00_00' : arrayCSV[16],
                'H_11_15_00' : arrayCSV[17],
                'H_11_30_00' : arrayCSV[18],
                'H_11_45_00' : arrayCSV[19],
                'H_12_00_00' : arrayCSV[20],
                'H_12_15_00' : arrayCSV[21],
                'H_12_30_00' : arrayCSV[22],
                'H_12_45_00' : arrayCSV[23],
                'H_13_00_00' : arrayCSV[24],
                'H_13_15_00' : arrayCSV[25],
                'H_13_30_00' : arrayCSV[26],
                'H_13_45_00' : arrayCSV[27],
                'H_14_00_00' : arrayCSV[28],
                'H_14_15_00' : arrayCSV[29],
                'H_14_30_00' : arrayCSV[30],
                'H_14_45_00' : arrayCSV[31],
                'H_15_00_00' : arrayCSV[32],
                'H_15_15_00' : arrayCSV[33],
                'H_15_30_00' : arrayCSV[34],
                'H_15_45_00' : arrayCSV[35],
                'H_16_00_00' : arrayCSV[36],
                'H_16_15_00' : arrayCSV[37],
                'H_16_30_00' : arrayCSV[38],
                'H_16_45_00' : arrayCSV[39],
                'H_17_00_00' : arrayCSV[40],
                'H_17_15_00' : arrayCSV[41],
                'H_17_30_00' : arrayCSV[42],
                'H_17_45_00' : arrayCSV[43],
                'H_18_00_00' : arrayCSV[44],
                'H_18_15_00' : arrayCSV[45],
                'H_18_30_00' : arrayCSV[46],
                'H_18_45_00' : arrayCSV[47],
                'H_19_00_00' : arrayCSV[48],
                'H_19_15_00' : arrayCSV[49],
                'H_19_30_00' : arrayCSV[50],
                'H_19_45_00' : arrayCSV[51],
                'H_20_00_00' : arrayCSV[52],
                'H_20_15_00' : arrayCSV[53],
                'H_20_30_00' : arrayCSV[54],
                'H_20_45_00' : arrayCSV[55],
                'H_21_00_00' : arrayCSV[56]

				}
		
		return [tupla]

def run():

	gcs_path = "gs://ct-workforce" #Definicion de la raiz del bucket
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
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/workforce/iti_detalle" + ".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/Seguimiento/Avon_inf_seg_2",file_name_suffix='.csv',shard_name_template='')
	
	transformed | 'Escritura a BigQuery Workforce' >> beam.io.WriteToBigQuery(
        gcs_project + ":Workforce.Iti_detalle",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio sin problema")