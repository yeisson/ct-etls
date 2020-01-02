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
	'idkey:STRING,'
	'fecha_cargue:STRING,'
	'Organizacion_de_ventas:STRING,'
	'Unidad_organizativa:STRING,'
	'Creado_por_Oferta_de_venta:STRING,'
	'Asesores:STRING,'
	'Tipo_de_Pago:STRING,'
	'Fase_de_Venta:STRING,'
	'Clasificacion_ABC_de_cuenta:STRING,'
	'Actividad:STRING,'
	'Cliente:STRING,'
	'Estado_Cliente:STRING,'
	'Numero_de_Documento:STRING,'
	'Sexo:STRING,'
	'Telefono:STRING,'
	'Tipo_de_documento_Numero_de_cliente:STRING,'
	'Telefono_movil:STRING,'
	'Correo_electronico:STRING,'
	'Cliente_gente_buena_Honda:STRING,'
	'Fecha_de_nacimiento_Cliente:STRING,'
	'Fecha_de_Cierre:STRING,'
	'Oferta_de_venta:STRING,'
	'Entidad_Financiera:STRING,'
	'Numero_de_factura_ECC:STRING,'
	'Requisitos:STRING,'
	'Creados_el_Fecha_y_hora:STRING,'
	'Creado_el:STRING,'
	'Producto:STRING,'
	'Fuentes_de_contacto:STRING,'
	'Motivo_de_rechazo_articulo:STRING,'
	'Campana:STRING,'
	'Como_se_entero_de_la_motocicleta_Oferta_de_venta:STRING,'
	'Prueba_de_Manejo_Oferta_de_venta:STRING,'
	'Consulta_central_riesgo1:STRING,'
	'Consulta_central_riesgo2:STRING,'
	'Valor_Oferta_Motos_Ajustada:STRING'

)


class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),				
				'fecha_cargue' : self.mifecha,
				'Organizacion_de_ventas': arrayCSV[0],
				'Unidad_organizativa': arrayCSV[1],
				'Creado_por_Oferta_de_venta': arrayCSV[2],
				'Asesores': arrayCSV[3],
				'Tipo_de_Pago': arrayCSV[4],
				'Fase_de_Venta': arrayCSV[5],
				'Clasificacion_ABC_de_cuenta': arrayCSV[6],
				'Actividad': arrayCSV[7],
				'Cliente': arrayCSV[8],
				'Estado_Cliente': arrayCSV[9],
				'Numero_de_Documento': arrayCSV[10],
				'Sexo': arrayCSV[11],
				'Telefono': arrayCSV[12],
				'Tipo_de_documento_Numero_de_cliente': arrayCSV[13],
				'Telefono_movil': arrayCSV[14],
				'Correo_electronico': arrayCSV[15],
				'Cliente_gente_buena_Honda': arrayCSV[16],
				'Fecha_de_nacimiento_Cliente': arrayCSV[17],
				'Fecha_de_Cierre': arrayCSV[18],
				'Oferta_de_venta': arrayCSV[19],
				'Entidad_Financiera': arrayCSV[20],
				'Numero_de_factura_ECC': arrayCSV[21],
				'Requisitos': arrayCSV[22],
				'Creados_el_Fecha_y_hora': arrayCSV[23],
				'Creado_el': arrayCSV[28],
				'Producto': arrayCSV[29],
				'Fuentes_de_contacto': arrayCSV[30],
				'Motivo_de_rechazo_articulo': arrayCSV[31],
				'Campana': arrayCSV[32],
				'Como_se_entero_de_la_motocicleta_Oferta_de_venta': arrayCSV[33],
				'Prueba_de_Manejo_Oferta_de_venta': arrayCSV[34],
				'Consulta_central_riesgo1': arrayCSV[35],
				'Consulta_central_riesgo2': arrayCSV[36],
				'Valor_Oferta_Motos_Ajustada': arrayCSV[38]
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-fanalca" #Definicion de la raiz del bucket
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
	lines = pipeline | 'Lectura de Archivo HONDA-DIGITAL-GESTIONC' >> ReadFromText(archivo, skip_header_lines=1)
	transformed = (lines | 'Formatear Data HONDA-DIGITAL-GESTIONC' >> beam.ParDo(formatearData(mifecha)))
	transformed | 'Escritura a BigQuery fanalca HONDA-DIGITAL-GESTIONC' >> beam.io.WriteToBigQuery(
		gcs_project + ":fanalca.gestion_cotizados_digital", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	return ("Corrio Full HD")



