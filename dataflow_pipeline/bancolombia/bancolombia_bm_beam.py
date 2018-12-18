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
from apache_beam.io import WriteToText, textio
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

TABLE_SCHEMA = (
	'idkey:STRING, '
	'fecha:STRING, '
	'consecutivo_documento_deudor:STRING, '
	'valor_cuota:STRING, '
	'clasificacion_producto:STRING, '
	'nit:STRING, '
	'nombres:STRING, '
	'numero_documento:STRING, '
	'tipo_producto:STRING, '
	'fecha_pago_cuota:STRING, '
	'modalidad:STRING, '
	'nombre_de_producto:STRING, '
	'plan:STRING, '
	'fecha_de_perfeccionamiento:STRING, '
	'fecha_vencimiento_def:STRING, '
	'numero_cuotas:STRING, '
	'cant_oblig:STRING, '
	'cuotas_en_mora:STRING, '
	'dia_de_vencimiento_de_cuota:STRING, '
	'valor_obligacion:STRING, '
	'valor_vencido:STRING, '
	'saldo_activo:STRING, '
	'saldo_orden:STRING, '
	'regional:STRING, '
	'ciudad:STRING, '
	'oficina_radicacion:STRING, '
	'grabador:STRING, '
	'nombre_asesor:STRING, '
	'codigo_agente:STRING, '
	'nombre_abogado:STRING, '
	'codigo_abogado:STRING, '
	'fecha_ultima_gestion_prejuridica:STRING, '
	'ultimo_codigo_de_gestion_prejuridico:STRING, '
	'ultimo_codigo_de_gestion_paralelo:STRING, '
	'desc_ultimo_codigo_de_gestion_prejuridico:STRING, '
	'codigo_anterior_de_gestion_prejuridico:STRING, '
	'desc_codigo_anterior_de_gestion_prejuridico:STRING, '
	'ultima_fecha_pago:STRING, '
	'ultima_fecha_de_actuacion_juridica:STRING, '
	'fecha_ultima_gestion_juridica:STRING, '
	'ejec_ultimo_codigo_de_gestion_juridico:STRING, '
	'rest_ultimo_codigo_gestion_juridico:STRING, '
	'desc_ultimo_codigo_de_gestion_juridico:STRING, '
	'tipo_de_cartera:STRING, '
	'dias_mora:STRING, '
	'calificacion:STRING, '
	'radicacion:STRING, '
	'estado_de_la_obligacion:STRING, '
	'fondo_nacional_garantias:STRING, '
	'region:STRING, '
	'segmento:STRING, '
	'fecha_importacion:STRING, '
	'titular_universal:STRING, '
	'negocio_titutularizado:STRING, '
	'red:STRING, '
	'fecha_traslado_para_cobro:STRING, '
	'calificacion_real:STRING, '
	'fecha_ultima_facturacion:STRING, '
	'cuadrante:STRING, '
	'causal:STRING, '
	'sector_economico:STRING, '
	'fecha_promesa:STRING, '
	'endeudamiento:STRING, '
	'probabilidad_de_propension_de_pago:STRING, '
	'priorizacion_final:STRING, '
	'grupo_de_priorizacion:STRING, '
	'ultimo_abogado:STRING, '
	'credito_cobertura_frech:STRING, '
	'endeudamiento_sufi:STRING, '
	'mono_multi:STRING, '
	'grupo:STRING, '
	'tipo_tc:STRING, '
	'ciclo:STRING, '
	'estrategia_cliente:STRING, '
	'impacto:STRING, '
	'provisiona:STRING, '
	'rango_mora:STRING, '
	'rango_gestion:STRING, '
	'dia_para_rodar:STRING, '
	'tipo_base:STRING, '
	'rango_desfase:STRING, '
	'fecha_ultima_gestion_adminfo:STRING, '
	'desc_ultimo_codigo_de_gestion_adminfo:STRING, '
	'mora_base_inicio:STRING, '
	'estrategia_clasificacion:STRING, '
	'estado_bini:STRING, '
	'uvr_pesos:STRING, '
	'reestruc_personas:STRING, '
	'reestruct_micropyme:STRING, '
	'tareas_estrategia:STRING, '
	'alternativa:STRING, '
	'hipo_titularizada:STRING, '
	'inconsistencias:STRING, '
	'no_gestionar:STRING, '
	'excluir_bases:STRING, '
	'priorizacion_piloto:STRING, '
	'analisis_contactab:STRING, '
	'grupo_de_priorizacion2:STRING, '
	'prob_pago:STRING, '
	'foco_piloto:STRING, '
	'prioridad_banco_piloto:STRING, '
	'marcar_referencias:STRING, '
	'refuerzo_gestion:STRING, '
	'franja_contacto:STRING, '
	'hora_contacto:STRING, '
	'dia_contacto:STRING, '
	'cel:STRING, '
	'contact:STRING, '
	'clientes:STRING, '
	'estrategia_micro:STRING, '
	'estrategia_final:STRING, '
	'region_final:STRING, '
	'estrategia_clasificacion_final:STRING, '
	'desfase_lotes:STRING, '
	'debug:STRING '
)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha

	def process(self, element):
		# print(element)
		arrayCSV = element.split('|')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),	#datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'consecutivo_documento_deudor': arrayCSV[0],
				'valor_cuota': arrayCSV[1],
				'clasificacion_producto': arrayCSV[2],
				'nit': arrayCSV[3],
				'nombres': arrayCSV[4],
				'numero_documento': arrayCSV[5],
				'tipo_producto': arrayCSV[6],
				'fecha_pago_cuota': arrayCSV[7],
				'modalidad': arrayCSV[8],
				'nombre_de_producto': arrayCSV[9],
				'plan': arrayCSV[10],
				'fecha_de_perfeccionamiento': arrayCSV[11],
				'fecha_vencimiento_def': arrayCSV[12],
				'numero_cuotas': arrayCSV[13],
				'cant_oblig': arrayCSV[14],
				'cuotas_en_mora': arrayCSV[15],
				'dia_de_vencimiento_de_cuota': arrayCSV[16],
				'valor_obligacion': arrayCSV[17],
				'valor_vencido': arrayCSV[18],
				'saldo_activo': arrayCSV[19],
				'saldo_orden': arrayCSV[20],
				'regional': arrayCSV[21],
				'ciudad': arrayCSV[22],
				'oficina_radicacion': arrayCSV[23],
				'grabador': arrayCSV[24],
				'nombre_asesor': arrayCSV[25],
				'codigo_agente': arrayCSV[26],
				'nombre_abogado': arrayCSV[27],
				'codigo_abogado': arrayCSV[28],
				'fecha_ultima_gestion_prejuridica': arrayCSV[29],
				'ultimo_codigo_de_gestion_prejuridico': arrayCSV[30],
				'ultimo_codigo_de_gestion_paralelo': arrayCSV[31],
				'desc_ultimo_codigo_de_gestion_prejuridico': arrayCSV[32],
				'codigo_anterior_de_gestion_prejuridico': arrayCSV[33],
				'desc_codigo_anterior_de_gestion_prejuridico': arrayCSV[34],
				'ultima_fecha_pago': arrayCSV[35],
				'ultima_fecha_de_actuacion_juridica': arrayCSV[36],
				'fecha_ultima_gestion_juridica': arrayCSV[37],
				'ejec_ultimo_codigo_de_gestion_juridico': arrayCSV[38],
				'rest_ultimo_codigo_gestion_juridico': arrayCSV[39],
				'desc_ultimo_codigo_de_gestion_juridico': arrayCSV[40],
				'tipo_de_cartera': arrayCSV[41],
				'dias_mora': arrayCSV[42],
				'calificacion': arrayCSV[43],
				'radicacion': arrayCSV[44],
				'estado_de_la_obligacion': arrayCSV[45],
				'fondo_nacional_garantias': arrayCSV[46],
				'region': arrayCSV[47],
				'segmento': arrayCSV[48],
				'fecha_importacion': arrayCSV[49],
				'titular_universal': arrayCSV[50],
				'negocio_titutularizado': arrayCSV[51],
				'red': arrayCSV[52],
				'fecha_traslado_para_cobro': arrayCSV[53],
				'calificacion_real': arrayCSV[54],
				'fecha_ultima_facturacion': arrayCSV[55],
				'cuadrante': arrayCSV[56],
				'causal': arrayCSV[57],
				'sector_economico': arrayCSV[58],
				'fecha_promesa': arrayCSV[59],
				'endeudamiento': arrayCSV[60],
				'probabilidad_de_propension_de_pago': arrayCSV[61],
				'priorizacion_final': arrayCSV[62],
				'grupo_de_priorizacion': arrayCSV[63],
				'ultimo_abogado': arrayCSV[64],
				'credito_cobertura_frech': arrayCSV[65],
				'endeudamiento_sufi': arrayCSV[66],
				'mono_multi': arrayCSV[67],
				'grupo': arrayCSV[68],
				'tipo_tc': arrayCSV[69],
				'ciclo': arrayCSV[70],
				'estrategia_cliente': arrayCSV[71],
				'impacto': arrayCSV[72],
				'provisiona': arrayCSV[73],
				'rango_mora': arrayCSV[74],
				'rango_gestion': arrayCSV[75],
				'dia_para_rodar': arrayCSV[76],
				'tipo_base': arrayCSV[77],
				'rango_desfase': arrayCSV[78],
				'fecha_ultima_gestion_adminfo': arrayCSV[79],
				'desc_ultimo_codigo_de_gestion_adminfo': arrayCSV[80],
				'mora_base_inicio': arrayCSV[81],
				'estrategia_clasificacion': arrayCSV[82],
				'estado_bini': arrayCSV[83],
				'uvr_pesos': arrayCSV[84],
				'reestruc_personas': arrayCSV[85],
				'reestruct_micropyme': arrayCSV[86],
				'tareas_estrategia': arrayCSV[87],
				'alternativa': arrayCSV[88],
				'hipo_titularizada': arrayCSV[89],
				'inconsistencias': arrayCSV[90],
				'no_gestionar': arrayCSV[91],
				'excluir_bases': arrayCSV[92],
				'priorizacion_piloto': arrayCSV[93],
				'analisis_contactab': arrayCSV[94],
				'grupo_de_priorizacion2': arrayCSV[95],
				'prob_pago': arrayCSV[96],
				'foco_piloto': arrayCSV[97],
				'prioridad_banco_piloto': arrayCSV[98],
				'marcar_referencias': arrayCSV[99],
				'refuerzo_gestion': arrayCSV[100],
				'franja_contacto': arrayCSV[101],
				'hora_contacto': arrayCSV[102],
				'dia_contacto': arrayCSV[103],
				'cel': arrayCSV[104],
				'contact': arrayCSV[105],
				'clientes': arrayCSV[106],
				'estrategia_micro': arrayCSV[107],
				'estrategia_final': arrayCSV[108],
				'region_final': arrayCSV[109],
				'estrategia_clasificacion_final': arrayCSV[110],
				'desfase_lotes': arrayCSV[111],
				'debug': arrayCSV[112]
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runner = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	# pipeline =  beam.Pipeline(runner="DirectRunner")
	pipeline =  beam.Pipeline(runner=mi_runner)
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT")
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("archivos/BANCOLOMBIA_BM_20181203.csv", skip_header_lines=1)
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText("//192.168.20.87/aries/Inteligencia_Negocios/EQUIPO BI/dcaro/Fuente Archivos/"+archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Base_Marcada_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/resultado_archivos_bm/Resultado_Base_Marcada", file_name_suffix='.csv',shard_name_template='')
	# transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/bm/Base_Marcada",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_admin.bm", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



