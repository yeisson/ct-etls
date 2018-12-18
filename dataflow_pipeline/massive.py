from __future__ import print_function, absolute_import

import logging
import re
import json
import requests
import uuid
import time
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


TABLE_SCHEMA = ('zona:STRING, '
                'codigo:STRING, '
                'balance:STRING ')

class formatearData(beam.DoFn):

  def __init__(self):
    super(formatearData, self).__init__()
    self.insertadas = Metrics.counter(self.__class__, 'insertado')

  def process(self, element):

    arrayCSV = element.split('|')

    return [{'zona' : arrayCSV[0],
             'codigo' : arrayCSV[1],
             'balance' : arrayCSV[2]}]


def run(inputFile):
  logging.info("Corriendo..")
  
  gcs_path = "gs://ct-avon" #Definicion de la raiz del bucket
  gcs_project = "contento-bi"

  # DataflowRunner or DirectRunner for local test
  pipeline =  beam.Pipeline(runner="DataflowRunner", argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5"
        # "--num_workers", "30",
        # "--autoscaling_algorithm", "NONE"
    ])

  # Leemos el fichero y lo ingresamos en una PColletion.
  lines = pipeline | 'Lectura del fichero Balance' >> ReadFromText("gs://ct-avon/Balance/" + inputFile)


  transformed = (lines
            | 'Formatear Data de Balance' >> beam.ParDo(formatearData()))

  #Escribimos a BigQuery
  transformed | 'Escritura a BigQuery Avon' >> beam.io.WriteToBigQuery(
        gcs_project + ":avon.balance2",
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


  jobObject = pipeline.run()
  jobID = jobObject.job_id()
