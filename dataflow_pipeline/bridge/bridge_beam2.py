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

####################### PARAMETROS DE LA TABLA EN BQ ##########################

TABLE_SCHEMA = (
			'Consecutivo_Documento_Deudor:STRING,'
			'Valor_Cuota:STRING,'
			'Nit:STRING,'
			'Nombres:STRING,'
			'Numero_Documento:STRING,'
			'Fecha_Pago_Cuota:STRING,'
			'Nombre_De_Producto:STRING,'
			'Fecha_De_Perfeccionamiento:STRING,'
			'Numero_Cuotas:STRING,'
			'Dia_De_Vencimiento_De_Cuota:STRING,'
			'Valor_Obligacion:STRING,'
			'Valor_Vencido:STRING,'
			'Saldo_Activo:STRING,'
			'Saldo_Orden:STRING,'
			'Ciudad:STRING,'
			'Nombre_Abogado:STRING,'
			'Fecha_Ultima_Gestion_Prejuridica:STRING,'
			'Desc_Ultimo_Codigo_De_Gestion_Prejuridico:STRING,'
			'Desc_Cod_Anterior_de_Gestion_Prejuridica:STRING,'
			'Fecha_Ultimo_Pago:STRING,'
			'Dias_Mora:STRING,'
			'Calificacion:STRING,'
			'Fondo_Nacional_Garantias:STRING,'
			'Region:STRING,'
			'Segmento:STRING,'
			'Calificacion_Real:STRING,'
			'Fecha_Ultima_Facturacion:STRING,'
			'Cuadrante:STRING,'
			'Causal:STRING,'
			'Sector_Economico:STRING,'
			'Fecha_De_Promesa:STRING,'
			'Endeudamiento:STRING,'
			'Probabilidad_de_Propension_de_pago:STRING,'
			'Grupo_de_Priorizacion:STRING,'
			'Mono__Multi:STRING,'
			'Grupo:STRING,'
			'Tipo_TC:STRING,'
			'CICLO:STRING,'
			'Estrategia_Cliente:STRING,'
			'Impacto:STRING,'
			'Rango_Mora:STRING,'
			'Tipo_Base_Inicial:STRING,'
			'Rango_desfase:STRING,'
			'Mora_Base_Inicio:STRING,'
			'Mora_Bini_Definitiva:STRING,'
			'Base_ini_Inter:STRING,'
			'Cosecha:STRING,'
			'Trimestre:STRING,'
			'Fecha_Retiro:STRING,'
			'Contacto:STRING,'
			'Tipo_Gest:STRING,'
			'Cta_Dia:STRING,'
			'Sem:STRING,'
			'Decil_DM:STRING,'
			'Decil_VrVenc:STRING,'
			'Clasif_Vr_Oblig:STRING,'
			'Percentil_VrOblig:STRING,'
			'Clasif_Vr_Venc:STRING,'
			'Percentil_VrVenc:STRING,'
			'Cta:STRING,'
			'Asignado:STRING,'
			'Gestionado:STRING,'
			'Contacto_General:STRING,'
			'Contacto_Directo:STRING,'
			'Productivo:STRING,'
			'Efectivo:STRING,'
			'Efectivo_Productivo:STRING,'
			'Clasif:STRING'
			)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {
				'Consecutivo_Documento_Deudor': arrayCSV[0],
				'Valor_Cuota': arrayCSV[1],
				'Nit': arrayCSV[2],
				'Nombres': arrayCSV[3],
				'Numero_Documento': arrayCSV[4],
				'Fecha_Pago_Cuota': arrayCSV[5],
				'Nombre_De_Producto': arrayCSV[6],
				'Fecha_De_Perfeccionamiento': arrayCSV[7],
				'Numero_Cuotas': arrayCSV[8],
				'Dia_De_Vencimiento_De_Cuota': arrayCSV[9],
				'Valor_Obligacion': arrayCSV[10],
				'Valor_Vencido': arrayCSV[11],
				'Saldo_Activo': arrayCSV[12],
				'Saldo_Orden': arrayCSV[13],
				'Ciudad': arrayCSV[14],
				'Nombre_Abogado': arrayCSV[15],
				'Fecha_Ultima_Gestion_Prejuridica': arrayCSV[16],
				'Desc_Ultimo_Codigo_De_Gestion_Prejuridico': arrayCSV[17],
				'Desc_Cod_Anterior_de_Gestion_Prejuridica': arrayCSV[18],
				'Fecha_Ultimo_Pago': arrayCSV[19],
				'Dias_Mora': arrayCSV[20],
				'Calificacion': arrayCSV[21],
				'Fondo_Nacional_Garantias': arrayCSV[22],
				'Region': arrayCSV[23],
				'Segmento': arrayCSV[24],
				'Calificacion_Real': arrayCSV[25],
				'Fecha_Ultima_Facturacion': arrayCSV[26],
				'Cuadrante': arrayCSV[27],
				'Causal': arrayCSV[28],
				'Sector_Economico': arrayCSV[29],
				'Fecha_De_Promesa': arrayCSV[30],
				'Endeudamiento': arrayCSV[31],
				'Probabilidad_de_Propension_de_pago': arrayCSV[32],
				'Grupo_de_Priorizacion': arrayCSV[33],
				'Mono__Multi': arrayCSV[34],
				'Grupo': arrayCSV[35],
				'Tipo_TC': arrayCSV[36],
				'CICLO': arrayCSV[37],
				'Estrategia_Cliente': arrayCSV[38],
				'Impacto': arrayCSV[39],
				'Rango_Mora': arrayCSV[40],
				'Tipo_Base_Inicial': arrayCSV[41],
				'Rango_desfase': arrayCSV[42],
				'Mora_Base_Inicio': arrayCSV[43],
				'Mora_Bini_Definitiva': arrayCSV[44],
				'Base_ini_Inter': arrayCSV[45],
				'Cosecha': arrayCSV[46],
				'Trimestre': arrayCSV[47],
				'Fecha_Retiro': arrayCSV[48],
				'Contacto': arrayCSV[49],
				'Tipo_Gest': arrayCSV[50],
				'Cta_Dia': arrayCSV[51],
				'Sem': arrayCSV[52],
				'Decil_DM': arrayCSV[53],
				'Decil_VrVenc': arrayCSV[54],
				'Clasif_Vr_Oblig': arrayCSV[55],
				'Percentil_VrOblig': arrayCSV[56],
				'Clasif_Vr_Venc': arrayCSV[57],
				'Percentil_VrVenc': arrayCSV[58],
				'Cta': arrayCSV[59],
				'Asignado': arrayCSV[60],
				'Gestionado': arrayCSV[61],
				'Contacto_General': arrayCSV[62],
				'Contacto_Directo': arrayCSV[63],
				'Productivo': arrayCSV[64],
				'Efectivo': arrayCSV[65],
				'Efectivo_Productivo': arrayCSV[66],
				'Clasif': arrayCSV[67]
				}
		
		return [tupla]

############################ CODIGO DE EJECUCION ###################################
def run(table):

	gcs_path = 'gs://ct-bridge' #Definicion de la raiz del bucket
	gcs_project = "contento-bi"
	FECHA_CARGUE = str(datetime.date.today())

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runner, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "5",
		"--subnetwork", "https://www.googleapis.com/compute/v1/projects/contento-bi/regions/us-central1/subnetworks/contento-subnet1"
    ])

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/" + FECHA_CARGUE + "_2" +".csv")
	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData()))
	# transformed | 'Escribir en Archivo' >> WriteToText(gcs_path + "/" + "REWORK",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Bridge' >> beam.io.WriteToBigQuery(
		gcs_project + ":Contactabilidad."+ table, 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

	jobObject = pipeline.run()
	return ("Proceso de transformacion y cargue, completado")


################################################################################