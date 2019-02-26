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
			'idkey:STRING,'
			'Fecha_Cargue:STRING,'
			'Consecutivo_Documento_Deudor:STRING,'
			'Valor_Cuota:STRING,'
			'Clasificacion_Producto:STRING,'
			'Nit:STRING,'
			'Nombres:STRING,'
			'Numero_Documento:STRING,'
			'Tipo_De_Producto:STRING,'
			'Fecha_Pago_Cuota:STRING,'
			'Modalidad:STRING,'
			'Nombre_De_Producto:STRING,'
			'Plan:STRING,'
			'Fecha_De_Perfeccionamiento:STRING,'
			'Fecha_Vencimiento_Def_:STRING,'
			'Numero_Cuotas:STRING,'
			'Cant_oblig:STRING,'
			'Cuotas_En_Mora:STRING,'
			'Dia_De_Vencimiento_De_Cuota:STRING,'
			'Valor_Obligacion:STRING,'
			'Valor_Vencido:STRING,'
			'Saldo_Activo:STRING,'
			'Saldo_Orden:STRING,'
			'Regional:STRING,'
			'Ciudad:STRING,'
			'Oficina_Radicacion:STRING,'
			'Grabador:STRING,'
			'Nombre_Asesor:STRING,'
			'Asesor:STRING,'
			'Nombre_Abogado:STRING,'
			'Abogado:STRING,'
			'Fecha_Ultima_Gestion_Prejuridica:STRING,'
			'Ultimo_Codigo_De_Gestion_Prejuridico:STRING,'
			'Codigo_De_Gestion_Paralela:STRING,'
			'Desc_Ultimo_Codigo_De_Gestion_Prejuridico:STRING,'
			'Cod__Anterior_De_Gestion_Prejuridico:STRING,'
			'Desc_Cod__Anterior_de_Gestion_Prejuridica:STRING,'
			'Ejec_ultima_Fecha_Actuacion_Juridica:STRING,'
			'Fecha_Grabacion_Ult_Jur:STRING,'
			'Fecha_Ultimo_Pago:STRING,'
			'Ejec_ultimo_Codigo_Gestion_Juridico:STRING,'
			'Desc_Ultimo_Codigo_Gestion_Juridica:STRING,'
			'Tipo_De_Credito:STRING,'
			'Tipo_De_Cartera:STRING,'
			'Dias_Mora:STRING,'
			'Calificacion:STRING,'
			'Radicacion:STRING,'
			'Estado_De_La_Obligacion:STRING,'
			'Fondo_Nacional_Garantias:STRING,'
			'Region:STRING,'
			'Segmento:STRING,'
			'Fecha_Importacion:STRING,'
			'Universa_Titular:STRING,'
			'Negocio_Titularizado:STRING,'
			'Red:STRING,'
			'Fecha_Traslado_Para_Cobro:STRING,'
			'Calificacion_Real:STRING,'
			'Fecha_Ultima_Facturacion:STRING,'
			'Cuadrante:STRING,'
			'Causal:STRING,'
			'Sector_Economico:STRING,'
			'Fecha_De_Promesa:STRING,'
			'Endeudamiento:STRING,'
			'Probabilidad_de_Propension_de_pago:STRING,'
			'Grupo_de_Priorizacion:STRING,'
			'Unicos:STRING,'
			'Mono_Multi:STRING,'
			'Grupo:STRING,'
			'Tipo_TC:STRING,'
			'CICLO:STRING,'
			'Codigo_Estrategia_Cliente:STRING,'
			'Estrategia_Cliente:STRING,'
			'Impacto:STRING,'
			'Provisiona:STRING,'
			'Rango_Mora:STRING,'
			'Tipo_de_mora:STRING,'
			'Rango_Gestion:STRING,'
			'Estrategia_gestion:STRING,'
			'Rango_Valor_Obligacion:STRING,'
			'Dia_Para_Rodar:STRING,'
			'Tipo_Base_Inicial:STRING,'
			'Rango_desfase:STRING,'
			'Fecha_Ultima_Gestion_Adminfo:STRING,'
			'Desc_Ultimo_Codigo_De_Gestion_Adminfo:STRING,'
			'Mora_Base_Inicio:STRING,'
			'Mora_Bini_Definitiva:STRING,'
			'Base_ini_Inter:STRING,'
			'Cosecha:STRING,'
			'Trimestre:STRING'
)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {
				'idkey': str(uuid.uuid4()),
				'Fecha_Cargue': datetime.datetime.today().strftime('%Y-%m-%d'),
				'Consecutivo_Documento_Deudor': arrayCSV[0],
				'Valor_Cuota': arrayCSV[1],
				'Clasificacion_Producto': arrayCSV[2],
				'Nit': arrayCSV[3],
				'Nombres': arrayCSV[4],
				'Numero_Documento': arrayCSV[5],
				'Tipo_De_Producto': arrayCSV[6],
				'Fecha_Pago_Cuota': arrayCSV[7],
				'Modalidad': arrayCSV[8],
				'Nombre_De_Producto': arrayCSV[9],
				'Plan': arrayCSV[10],
				'Fecha_De_Perfeccionamiento': arrayCSV[11],
				'Fecha_Vencimiento_Def_': arrayCSV[12],
				'Numero_Cuotas': arrayCSV[13],
				'Cant_oblig': arrayCSV[14],
				'Cuotas_En_Mora': arrayCSV[15],
				'Dia_De_Vencimiento_De_Cuota': arrayCSV[16],
				'Valor_Obligacion': arrayCSV[17],
				'Valor_Vencido': arrayCSV[18],
				'Saldo_Activo': arrayCSV[19],
				'Saldo_Orden': arrayCSV[20],
				'Regional': arrayCSV[21],
				'Ciudad': arrayCSV[22],
				'Oficina_Radicacion': arrayCSV[23],
				'Grabador': arrayCSV[24],
				'Nombre_Asesor': arrayCSV[25],
				'Asesor': arrayCSV[26],
				'Nombre_Abogado': arrayCSV[27],
				'Abogado': arrayCSV[28],
				'Fecha_Ultima_Gestion_Prejuridica': arrayCSV[29],
				'Ultimo_Codigo_De_Gestion_Prejuridico': arrayCSV[30],
				'Codigo_De_Gestion_Paralela': arrayCSV[31],
				'Desc_Ultimo_Codigo_De_Gestion_Prejuridico': arrayCSV[32],
				'Cod__Anterior_De_Gestion_Prejuridico': arrayCSV[33],
				'Desc_Cod__Anterior_de_Gestion_Prejuridica': arrayCSV[34],
				'Ejec_ultima_Fecha_Actuacion_Juridica': arrayCSV[35],
				'Fecha_Grabacion_Ult_Jur': arrayCSV[36],
				'Fecha_Ultimo_Pago': arrayCSV[37],
				'Ejec_ultimo_Codigo_Gestion_Juridico': arrayCSV[38],
				'Desc_Ultimo_Codigo_Gestion_Juridica': arrayCSV[39],
				'Tipo_De_Credito': arrayCSV[40],
				'Tipo_De_Cartera': arrayCSV[41],
				'Dias_Mora': arrayCSV[42],
				'Calificacion': arrayCSV[43],
				'Radicacion': arrayCSV[44],
				'Estado_De_La_Obligacion': arrayCSV[45],
				'Fondo_Nacional_Garantias': arrayCSV[46],
				'Region': arrayCSV[47],
				'Segmento': arrayCSV[48],
				'Fecha_Importacion': arrayCSV[49],
				'Universa_Titular': arrayCSV[50],
				'Negocio_Titularizado': arrayCSV[51],
				'Red': arrayCSV[52],
				'Fecha_Traslado_Para_Cobro': arrayCSV[53],
				'Calificacion_Real': arrayCSV[54],
				'Fecha_Ultima_Facturacion': arrayCSV[55],
				'Cuadrante': arrayCSV[56],
				'Causal': arrayCSV[57],
				'Sector_Economico': arrayCSV[58],
				'Fecha_De_Promesa': arrayCSV[59],
				'Endeudamiento': arrayCSV[60],
				'Probabilidad_de_Propension_de_pago': arrayCSV[61],
				'Grupo_de_Priorizacion': arrayCSV[62],
				'Unicos': arrayCSV[63],
				'Mono_Multi': arrayCSV[64],
				'Grupo': arrayCSV[65],
				'Tipo_TC': arrayCSV[66],
				'CICLO': arrayCSV[67],
				'Codigo_Estrategia_Cliente': arrayCSV[68],
				'Estrategia_Cliente': arrayCSV[69],
				'Impacto': arrayCSV[70],
				'Provisiona': arrayCSV[71],
				'Rango_Mora': arrayCSV[72],
				'Tipo_de_mora': arrayCSV[73],
				'Rango_Gestion': arrayCSV[74],
				'Estrategia_gestion': arrayCSV[75],
				'Rango_Valor_Obligacion': arrayCSV[76],
				'Dia_Para_Rodar': arrayCSV[77],
				'Tipo_Base_Inicial': arrayCSV[78],
				'Rango_desfase': arrayCSV[79],
				'Fecha_Ultima_Gestion_Adminfo': arrayCSV[80],
				'Desc_Ultimo_Codigo_De_Gestion_Adminfo': arrayCSV[81],
				'Mora_Base_Inicio': arrayCSV[82],
				'Mora_Bini_Definitiva': arrayCSV[83],
				'Base_ini_Inter': arrayCSV[84],
				'Cosecha': arrayCSV[85],
				'Trimestre': arrayCSV[86]
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

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/" + FECHA_CARGUE + ".csv")
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