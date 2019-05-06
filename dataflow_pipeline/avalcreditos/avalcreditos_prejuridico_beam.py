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

TABLE_SCHEMA = (
	'idkey:STRING, '
	'fecha:STRING, '
	'nro_credito:STRING, '
	'nit:STRING, '
	'razon:STRING, '
	'nombre:STRING, '
	'telefono:STRING, '
	'tienda:STRING, '
	'celular:STRING, '
	'valor_a_cobrar:STRING, '
	'valor_incial_credito:STRING, '
	'valor_a_cobrar_maximo:STRING, '
	'rango_mora_obligacion:STRING, '
	'dias_sin_tramite_1:STRING, '
	'rango_mora_cliente:STRING, '
	'fecha_de_vencimiento:STRING, '
	'ano_vencimiento:STRING, '
	'edad_de_mora_maximo:STRING, '
	'edad_de_mora:STRING, '
	'dias_sin_tramite_2:STRING, '
	'ciudad_del_cliente:STRING, '
	'resultado_del_tramite:STRING, '
	'fecha_ultimo_pago:STRING, '
	'asignacion_de_usuarios:STRING, '
	'gestor_del_tramite:STRING, '
	'con_email:STRING, '
	'con_telefono:STRING, '
	'con_direccion:STRING, '
	'con_referencia:STRING, '
	'tipificacion:STRING, '
	'ult_resultado_efectivo:STRING, '
	'ult_resultado_efectivo_fecha:STRING, '
	'ano_venci_oblig_rango:STRING, '
	'ano_venci_cliente_rango:STRING, '
	'estado_de_cartera:STRING, '
	'ano_originacion:STRING, '
	'ano_orig_oblig_rango:STRING, '
	'ano_orig_cliente_rango:STRING, '
	'con_celular:STRING, '
	'cuota_vencida:STRING, '
	'total_cuotas_vencidas:STRING, '
	'cuotas_en_mora:STRING, '
	'fecha_prox_acuerdo:STRING, '
	'usuario_responsable:STRING, '
	'valor_intereses:STRING, '
	'empresa_que_reporta:STRING, '
	'valor_aval:STRING, '
	'valor_cuota:STRING, '
	'valor_abonos:STRING, '
	'ciudad_punto_de_credito:STRING, '
	'fecha_empresa_reporta:STRING, '
	'estado_de_la_cuota:STRING, '
	'empresa_origen:STRING, '
	'intereses_mora:STRING, '
	'total_intereses_mora:STRING, '
	'total_honorarios:STRING, '
	'saldo_capital:STRING, '
	'valcapitalmax:STRING, '
	'tipo_de_credito:STRING, '
	'emails:STRING, '
	'numobligaciongr:STRING, '
	'fecplanpagomax:STRING, '
	'telefono_1:STRING, '
	'telefono_2:STRING, '
	'telefono_3:STRING, '
	'telefono_4:STRING, '
	'telefono_5:STRING, '
	'telefono_6:STRING, '
	'telefono_7:STRING '
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
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha' : self.mifecha,
				'nro_credito' : arrayCSV[0].replace('"',''),
				'nit' : arrayCSV[1].replace('"',''),
				'razon' : arrayCSV[2].replace('"',''),
				'nombre' : arrayCSV[3].replace('"',''),
				'telefono' : arrayCSV[4].replace('"',''),
				'tienda' : arrayCSV[5].replace('"',''),
				'celular' : arrayCSV[6].replace('"',''),
				'valor_a_cobrar' : arrayCSV[7].replace('"',''),
				'valor_incial_credito' : arrayCSV[8].replace('"',''),
				'valor_a_cobrar_maximo' : arrayCSV[9].replace('"',''),
				'rango_mora_obligacion' : arrayCSV[10].replace('"',''),
				'dias_sin_tramite_1' : arrayCSV[11].replace('"',''),
				'rango_mora_cliente' : arrayCSV[12].replace('"',''),
				'fecha_de_vencimiento' : arrayCSV[13].replace('"',''),
				'ano_vencimiento' : arrayCSV[14].replace('"',''),
				'edad_de_mora_maximo' : arrayCSV[15].replace('"',''),
				'edad_de_mora' : arrayCSV[16].replace('"',''),
				'dias_sin_tramite_2' : arrayCSV[17].replace('"',''),
				'ciudad_del_cliente' : arrayCSV[18].replace('"',''),
				'resultado_del_tramite' : arrayCSV[19].replace('"',''),
				'fecha_ultimo_pago' : arrayCSV[20].replace('"',''),
				'asignacion_de_usuarios' : arrayCSV[21].replace('"',''),
				'gestor_del_tramite' : arrayCSV[22].replace('"',''),
				'con_email' : arrayCSV[23].replace('"',''),
				'con_telefono' : arrayCSV[24].replace('"',''),
				'con_direccion' : arrayCSV[25].replace('"',''),
				'con_referencia' : arrayCSV[26].replace('"',''),
				'tipificacion' : arrayCSV[27].replace('"',''),
				'ult_resultado_efectivo' : arrayCSV[28].replace('"',''),
				'ult_resultado_efectivo_fecha' : arrayCSV[29].replace('"',''),
				'ano_venci_oblig_rango' : arrayCSV[30].replace('"',''),
				'ano_venci_cliente_rango' : arrayCSV[31].replace('"',''),
				'estado_de_cartera' : arrayCSV[32].replace('"',''),
				'ano_originacion' : arrayCSV[33].replace('"',''),
				'ano_orig_oblig_rango' : arrayCSV[34].replace('"',''),
				'ano_orig_cliente_rango' : arrayCSV[35].replace('"',''),
				'con_celular' : arrayCSV[36].replace('"',''),
				'cuota_vencida' : arrayCSV[37].replace('"',''),
				'total_cuotas_vencidas' : arrayCSV[38].replace('"',''),
				'cuotas_en_mora' : arrayCSV[39].replace('"',''),
				'fecha_prox_acuerdo' : '',	#arrayCSV[40].replace('"',''),
				'usuario_responsable' : arrayCSV[40].replace('"',''),
				'valor_intereses' : arrayCSV[41].replace('"',''),
				'empresa_que_reporta' : arrayCSV[42].replace('"',''),
				'valor_aval' : arrayCSV[43].replace('"',''),
				'valor_cuota' : arrayCSV[44].replace('"',''),
				'valor_abonos' : arrayCSV[45].replace('"',''),
				'ciudad_punto_de_credito' : arrayCSV[46].replace('"',''),
				'fecha_empresa_reporta' : arrayCSV[47].replace('"',''),
				'estado_de_la_cuota' : arrayCSV[48].replace('"',''),
				'empresa_origen' : arrayCSV[49].replace('"',''),
				'intereses_mora' : arrayCSV[50].replace('"',''),
				'total_intereses_mora' : arrayCSV[51].replace('"',''),
				'total_honorarios' : arrayCSV[52].replace('"',''),
				'saldo_capital' : arrayCSV[53].replace('"',''),
				'valcapitalmax' : arrayCSV[54].replace('"',''),
				'tipo_de_credito' : arrayCSV[55].replace('"',''),
				'emails' : arrayCSV[56].replace('"',''),
				'numobligaciongr' : arrayCSV[57].replace('"',''),
				'fecplanpagomax' : arrayCSV[58].replace('"',''),
				'telefono_1' : arrayCSV[59].replace('"',''),
				'telefono_2' : arrayCSV[60].replace('"',''),
				'telefono_3' : arrayCSV[61].replace('"',''),
				'telefono_4' : arrayCSV[62].replace('"',''),
				'telefono_5' : arrayCSV[63].replace('"',''),
				'telefono_6' : arrayCSV[64].replace('"',''),
				'telefono_7' : arrayCSV[65].replace('"','')
				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-avalcreditos" #Definicion de la raiz del bucket
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
	
	# lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181206 1100.csv", skip_header_lines=1)
	#lines = pipeline | 'Lectura de Archivo' >> ReadFromText("gs://ct-bancolombia/info-segumiento/BANCOLOMBIA_INF_SEG_20181129 0800.csv", skip_header_lines=1)
	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(archivo, skip_header_lines=1)

	transformed = (lines | 'Formatear Data' >> beam.ParDo(formatearData(mifecha)))

	# lines | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_prej_small", file_name_suffix='.csv',shard_name_template='')

	# transformed | 'Escribir en Archivo' >> WriteToText("archivos/Info_carga_banco_seg", file_name_suffix='.csv',shard_name_template='')
	#transformed | 'Escribir en Archivo' >> WriteToText("gs://ct-bancolombia/info-segumiento/info_carga_banco_seg",file_name_suffix='.csv',shard_name_template='')

	transformed | 'Escritura a BigQuery avalcreditos' >> beam.io.WriteToBigQuery(
		gcs_project + ":avalcreditos.prejuridico", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



