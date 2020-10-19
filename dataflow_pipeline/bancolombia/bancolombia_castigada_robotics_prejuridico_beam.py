#coding: utf-8 
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

TABLE_SCHEMA = ('IDKEY:STRING, '
				'FECHA:STRING, '
				'CONSECUTIVO_DOCUMENTO_DEUDOR:STRING, '
				'NIT:STRING, '
				'VALOR_CUOTA:STRING, '
				'NOMBRES:STRING, '
				'CLASIFICACION_PRODUCTO:STRING, '
				'NUMERO_DOCUMENTO:STRING, '
				'TIPO_PRODUCTO:STRING, '
				'MODALIDAD:STRING, '
				'FECHA_PAGO_CUOTA:STRING, '
				'FECHA_ACTUALIZACION_PRIORIZACION:STRING, '
				'NOMBRE_DE_PRODUCTO:STRING, '
				'PLAN:STRING, '
				'FECHA_DE_PERFECCIONAMIENTO:STRING, '
				'FECHA_VENCIMIENTO_DEF:STRING, '
				'NUMERO_CUOTAS:STRING, '
				'CUOTAS_EN_MORA:STRING, '
				'DIA_DE_VENCIMIENTO_DE_CUOTA:STRING, '
				'VALOR_OBLIGACION:STRING, '
				'VALOR_VENCIDO:STRING, '
				'SALDO_ACTIVO:STRING, '
				'SALDO_ORDEN:STRING, '
				'REGIONAL:STRING, '
				'CIUDAD:STRING, '
				'OFICINA_RADICACION:STRING, '
				'GRABADOR:STRING, '
				'CODIGO_AGENTE:STRING, '
				'NOMBRE_ASESOR:STRING, '
				'CODIGO_ABOGADO:STRING, '
				'NOMBRE_ABOGADO:STRING, '
				'FECHA_ULTIMA_GESTION_PREJURIDICA:STRING, '
				'ULTIMO_CODIGO_DE_GESTION_PARALELO:STRING, '
				'REST_ULTIMO_CODIGO_GESTION_JURIDICO:STRING, '
				'ULTIMO_CODIGO_DE_GESTION_PREJURIDICO:STRING, '
				'DESCRIPCION_SUBSECTOR:STRING, '
				'DESCRIPCION_CODIGO_SEGMENTO:STRING, '
				'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO:STRING, '
				'DESCRIPCION_SUBSEGMENTO:STRING, '
				'DESCRIPCION_SECTOR:STRING, '
				'DESCRIPCION_CODIGO_CIIU:STRING, '
				'CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO:STRING, '
				'DESC_CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO:STRING, '
				'FECHA_ULTIMA_GESTION_JURIDICA:STRING, '
				'ULTIMA_FECHA_DE_ACTUACION_JURIDICA:STRING, '
				'ULTIMA_FECHA_PAGO:STRING, '
				'EJEC_ULTIMO_CODIGO_DE_GESTION_JURIDICO:STRING, '
				'DESC_ULTIMO_CODIGO_DE_GESTION_JURIDICO:STRING, '
				'CANT_OBLIG:STRING, '
				'TIPO_DE_CREDITO:STRING, '
				'INFLUENCER:STRING, '
				'DESCIPCION_INFLUENCER:STRING, '
				'CLUSTER_PYME:STRING, '
				'NICHO_MERCADO:STRING, '
				'DIAS_MORA:STRING, '
				'DESCRIPCION_MEJOR_CODIGO_DE_GESTION:STRING, '
				'MEJOR_CODIGO_DE_GESTION:STRING, '
				'FECHA_MEJOR_CODIGO_DE_GESTION:STRING, '
				'PAIS_RESIDENCIA:STRING, '
				'CLUSTER_PERSONA:STRING, '
				'TIPO_DE_CARTERA:STRING, '
				'CALIFICACION:STRING, '
				'RADICACION:STRING, '
				'ESTADO_DE_LA_OBLIGACION:STRING, '
				'FONDO_NACIONAL_GARANTIAS:STRING, '
				'REGION:STRING, '
				'SEGMENTO:STRING, '
				'CODIGO_SEGMENTO:STRING, '
				'FECHA_IMPORTACION:STRING, '
				'ESTADO_PRODUCTO:STRING, '
				'CODIGO_PAIS:STRING, '
				'CODIGO_ABOGADO_PARLELO:STRING, '
				'TAREA_PARALELA:STRING, '
				'FECHA_GESTION_PARALELA:STRING, '
				'DESC_ULTIMO_CODIGO_DE_GESTION_PARALELO:STRING, '
				'ESTADO_TARJETA:STRING, '
				'TAREA_DEL_USUARIO_VISITA:STRING, '
				'TAREA_DEL_USUARIO:STRING, '
				'EXCEPCION:STRING, '
				'NIVEL_DE_RIESGO:STRING, '
				'COMPORTAMIENTO_CLIENTE:STRING, '
				'COMPORTAMIENTO_OBLIGACION:STRING, '
				'CATEGORIA_CLIENTE:STRING, '
				'FECHA_ULTIMA_FACTURACION:STRING, '
				'SUBSEGMENTO:STRING, '
				'LOTE:STRING, '
				'TITULAR_UNIVERSAL:STRING, '
				'NEGOCIO_BRP:STRING, '
				'CONSECUTIVO_TRASLADO:STRING, '
				'NEGOCIO_TITUTULARIZADO:STRING, '
				'CONSECUTIVO_TRASLADO_PARALELO:STRING, '
				'REFERENCIA_EMPLEADO:STRING, '
				'SECTOR_ECONOMICO:STRING, '
				'PROFESION:STRING, '
				'CAUSAL:STRING, '
				'CODIGO_CAUSAL:STRING, '
				'OCUPACION:STRING, '
				'CUADRANTE:STRING, '
				'FECHA_TRASLADO_PARA_COBRO:STRING, '
				'DESC_CODIGO_DE_GESTION_VISITA:STRING, '
				'FECHA_GRABACION_VISITA:STRING, '
				'NUMERO_CUOTA_PAGAR:STRING, '
				'CIERRE:STRING, '
				'ENDEUDAMIENTO_SUFI:STRING, '
				'ENDEUDAMIENTO:STRING, '
				'CARTERA:STRING, '
				'CALIFICACION_REAL:STRING, '
				'FECHA_PROMESA:STRING, '
				'MORA_MAXIMA_OTROS:STRING, '
				'MONITOR:STRING, '
				'ASENVIO:STRING, '
				'RED:STRING, '
				'ULTIMO_ABOGADO:STRING, '
				'ESTADO_PERSONA:STRING, '
				'ESTADO_NEGOCIACION:STRING, '
				'NUMERO_DE_CREDITO_ASOCIADO_AL_CAMBIO_DE_MONEDA:STRING, '
				'MODALIDAD_DEL_CREDITO:STRING, '
				'SALDO_CAPITAL:STRING, '
				'PROMEDIO_MORA:STRING, '
				'OTROS_CARGOS:STRING, '
				'CREDITO_COBERTURA_FRECH:STRING, '
				'MARCA_RESIDENTE_EXTERIOR:STRING, '
				'MARCA_CASTIGADO:STRING, '
				'TIPO_CLIENTE_SUFI:STRING, '
				'CLASE:STRING, '
				'FRANQUICIA:STRING, '
				'SALDO_CAPITAL_PESOS:STRING, '
				'SALDO_INTERESES_PESOS:STRING, '
				'SALDO_OTROS_PESOS:STRING, '
				'SALDO_CAPITAL_DOLARES:STRING, '
				'SALDO_INTERESES_DOLARES:STRING, '
				'SALDO_OTROS_DOLARES:STRING, '
				'PAGO_MINIMO_EN_DOLARES:STRING, '
				'CODIGO_DUENO_TAREA_PARALELA:STRING, '
				'NOMBRE_DUENO_TAREA:STRING, '
				'PROBABILIDAD_DE_PROPENSION_DE_PAGO:STRING, '
				'PRIORIZACION_FINAL:STRING, '
				'PRIORIZACION_POR_CLIENTE:STRING, '
				'GRUPO_DE_PRIORIZACION:STRING '
				)

class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla = {'IDKEY' : str(uuid.uuid4()),
				'FECHA': self.mifecha,
				'CONSECUTIVO_DOCUMENTO_DEUDOR' : arrayCSV[0],
				'NIT' : arrayCSV[1],
				'VALOR_CUOTA' : arrayCSV[2],
				'NOMBRES' : arrayCSV[3],
				'CLASIFICACION_PRODUCTO' : arrayCSV[4],
				'NUMERO_DOCUMENTO' : arrayCSV[5],
				'TIPO_PRODUCTO' : arrayCSV[6],
				'MODALIDAD' : arrayCSV[7],
				'FECHA_PAGO_CUOTA' : arrayCSV[8],
				'FECHA_ACTUALIZACION_PRIORIZACION' : arrayCSV[9],
				'NOMBRE_DE_PRODUCTO' : arrayCSV[10],
				'PLAN' : arrayCSV[11],
				'FECHA_DE_PERFECCIONAMIENTO' : arrayCSV[12],
				'FECHA_VENCIMIENTO_DEF' : arrayCSV[13],
				'NUMERO_CUOTAS' : arrayCSV[14],
				'CUOTAS_EN_MORA' : arrayCSV[15],
				'DIA_DE_VENCIMIENTO_DE_CUOTA' : arrayCSV[16],
				'VALOR_OBLIGACION' : arrayCSV[17],
				'VALOR_VENCIDO' : arrayCSV[18],
				'SALDO_ACTIVO' : arrayCSV[19],
				'SALDO_ORDEN' : arrayCSV[20],
				'REGIONAL' : arrayCSV[21],
				'CIUDAD' : arrayCSV[22],
				'OFICINA_RADICACION' : arrayCSV[23],
				'GRABADOR' : arrayCSV[24],
				'CODIGO_AGENTE' : arrayCSV[25],
				'NOMBRE_ASESOR' : arrayCSV[26],
				'CODIGO_ABOGADO' : arrayCSV[27],
				'NOMBRE_ABOGADO' : arrayCSV[28],
				'FECHA_ULTIMA_GESTION_PREJURIDICA' : arrayCSV[29],
				'ULTIMO_CODIGO_DE_GESTION_PARALELO' : arrayCSV[30],
				'REST_ULTIMO_CODIGO_GESTION_JURIDICO' : arrayCSV[31],
				'ULTIMO_CODIGO_DE_GESTION_PREJURIDICO' : arrayCSV[32],
				'DESCRIPCION_SUBSECTOR' : arrayCSV[33],
				'DESCRIPCION_CODIGO_SEGMENTO' : arrayCSV[34],
				'DESC_ULTIMO_CODIGO_DE_GESTION_PREJURIDICO' : arrayCSV[35],
				'DESCRIPCION_SUBSEGMENTO' : arrayCSV[36],
				'DESCRIPCION_SECTOR' : arrayCSV[37],
				'DESCRIPCION_CODIGO_CIIU' : arrayCSV[38],
				'CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO' : arrayCSV[39],
				'DESC_CODIGO_ANTERIOR_DE_GESTION_PREJURIDICO' : arrayCSV[40],
				'FECHA_ULTIMA_GESTION_JURIDICA' : arrayCSV[41],
				'ULTIMA_FECHA_DE_ACTUACION_JURIDICA' : arrayCSV[42],
				'ULTIMA_FECHA_PAGO' : arrayCSV[43],
				'EJEC_ULTIMO_CODIGO_DE_GESTION_JURIDICO' : arrayCSV[44],
				'DESC_ULTIMO_CODIGO_DE_GESTION_JURIDICO' : arrayCSV[45],
				'CANT_OBLIG' : arrayCSV[46],
				'TIPO_DE_CREDITO' : arrayCSV[47],
				'INFLUENCER' : arrayCSV[48],
				'DESCIPCION_INFLUENCER' : arrayCSV[49],
				'CLUSTER_PYME' : arrayCSV[50],
				'NICHO_MERCADO' : arrayCSV[51],
				'DIAS_MORA' : arrayCSV[52],
				'DESCRIPCION_MEJOR_CODIGO_DE_GESTION' : arrayCSV[53],
				'MEJOR_CODIGO_DE_GESTION' : arrayCSV[54],
				'FECHA_MEJOR_CODIGO_DE_GESTION' : arrayCSV[55],
				'PAIS_RESIDENCIA' : arrayCSV[56],
				'CLUSTER_PERSONA' : arrayCSV[57],
				'TIPO_DE_CARTERA' : arrayCSV[58],
				'CALIFICACION' : arrayCSV[59],
				'RADICACION' : arrayCSV[60],
				'ESTADO_DE_LA_OBLIGACION' : arrayCSV[61],
				'FONDO_NACIONAL_GARANTIAS' : arrayCSV[62],
				'REGION' : arrayCSV[63],
				'SEGMENTO' : arrayCSV[64],
				'CODIGO_SEGMENTO' : arrayCSV[65],
				'FECHA_IMPORTACION' : arrayCSV[66],
				'ESTADO_PRODUCTO' : arrayCSV[67],
				'CODIGO_PAIS' : arrayCSV[68],
				'CODIGO_ABOGADO_PARLELO' : arrayCSV[69],
				'TAREA_PARALELA' : arrayCSV[70],
				'FECHA_GESTION_PARALELA' : arrayCSV[71],
				'DESC_ULTIMO_CODIGO_DE_GESTION_PARALELO' : arrayCSV[72],
				'ESTADO_TARJETA' : arrayCSV[73],
				'TAREA_DEL_USUARIO_VISITA' : arrayCSV[74],
				'TAREA_DEL_USUARIO' : arrayCSV[75],
				'EXCEPCION' : arrayCSV[76],
				'NIVEL_DE_RIESGO' : arrayCSV[77],
				'COMPORTAMIENTO_CLIENTE' : arrayCSV[78],
				'COMPORTAMIENTO_OBLIGACION' : arrayCSV[79],
				'CATEGORIA_CLIENTE' : arrayCSV[80],
				'FECHA_ULTIMA_FACTURACION' : arrayCSV[81],
				'SUBSEGMENTO' : arrayCSV[82],
				'LOTE' : arrayCSV[83],
				'TITULAR_UNIVERSAL' : arrayCSV[84],
				'NEGOCIO_BRP' : arrayCSV[85],
				'CONSECUTIVO_TRASLADO' : arrayCSV[86],
				'NEGOCIO_TITUTULARIZADO' : arrayCSV[87],
				'CONSECUTIVO_TRASLADO_PARALELO' : arrayCSV[88],
				'REFERENCIA_EMPLEADO' : arrayCSV[89],
				'SECTOR_ECONOMICO' : arrayCSV[90],
				'PROFESION' : arrayCSV[91],
				'CAUSAL' : arrayCSV[92],
				'CODIGO_CAUSAL' : arrayCSV[93],
				'OCUPACION' : arrayCSV[94],
				'CUADRANTE' : arrayCSV[95],
				'FECHA_TRASLADO_PARA_COBRO' : arrayCSV[96],
				'DESC_CODIGO_DE_GESTION_VISITA' : arrayCSV[97],
				'FECHA_GRABACION_VISITA' : arrayCSV[98],
				'NUMERO_CUOTA_PAGAR' : arrayCSV[99],
				'CIERRE' : arrayCSV[100],
				'ENDEUDAMIENTO_SUFI' : arrayCSV[101],
				'ENDEUDAMIENTO' : arrayCSV[102],
				'CARTERA' : arrayCSV[103],
				'CALIFICACION_REAL' : arrayCSV[104],
				'FECHA_PROMESA' : arrayCSV[105],
				'MORA_MAXIMA_OTROS' : arrayCSV[106],
				'MONITOR' : arrayCSV[107],
				'ASENVIO' : arrayCSV[108],
				'RED' : arrayCSV[109],
				'ULTIMO_ABOGADO' : arrayCSV[110],
				'ESTADO_PERSONA' : arrayCSV[111],
				'ESTADO_NEGOCIACION' : arrayCSV[112],
				'NUMERO_DE_CREDITO_ASOCIADO_AL_CAMBIO_DE_MONEDA' : arrayCSV[113],
				'MODALIDAD_DEL_CREDITO' : arrayCSV[114],
				'SALDO_CAPITAL' : arrayCSV[115],
				'PROMEDIO_MORA' : arrayCSV[116],
				'OTROS_CARGOS' : arrayCSV[117],
				'CREDITO_COBERTURA_FRECH' : arrayCSV[118],
				'MARCA_RESIDENTE_EXTERIOR' : arrayCSV[119],
				'MARCA_CASTIGADO' : arrayCSV[120],
				'TIPO_CLIENTE_SUFI' : arrayCSV[121],
				'CLASE' : arrayCSV[122],
				'FRANQUICIA' : arrayCSV[123],
				'SALDO_CAPITAL_PESOS' : arrayCSV[124],
				'SALDO_INTERESES_PESOS' : arrayCSV[125],
				'SALDO_OTROS_PESOS' : arrayCSV[126],
				'SALDO_CAPITAL_DOLARES' : arrayCSV[127],
				'SALDO_INTERESES_DOLARES' : arrayCSV[128],
				'SALDO_OTROS_DOLARES' : arrayCSV[129],
				'PAGO_MINIMO_EN_DOLARES' : arrayCSV[130],
				'CODIGO_DUENO_TAREA_PARALELA' : arrayCSV[131],
				'NOMBRE_DUENO_TAREA' : arrayCSV[132],
				'PROBABILIDAD_DE_PROPENSION_DE_PAGO' : arrayCSV[133],
				'PRIORIZACION_FINAL' : arrayCSV[134],
				'PRIORIZACION_POR_CLIENTE' : arrayCSV[135],
				'GRUPO_DE_PRIORIZACION' : arrayCSV[136]
				}
		
		return [tupla]

def run(archivo, mifecha):

	gcs_path = "gs://ct-bancolombia_castigada" #Definicion de la raiz del bucket
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
        # "--num_workers", "30",
        # "--autoscaling_algorithm", "NONE"		
	])
	
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	transformed | 'Escritura a BigQuery Bancolombia' >> beam.io.WriteToBigQuery(
		gcs_project + ":bancolombia_castigada.rob_aux_prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	jobObject = pipeline.run()
	jobObject.wait_until_finish()

	return ("Corrio Full HD")
