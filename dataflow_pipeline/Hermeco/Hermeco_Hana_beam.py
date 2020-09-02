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
    'IDKEY:STRING, ' 
	'FECHA:STRING, ' 
	'NRO__PED__SAP:STRING, '
	'NRO__PED__TV:STRING, '
	'VIA_DE_PAGO:STRING, '
	'C__CLASE_PEDIDO:STRING, '
	'N__CLASE_PEDIDO:STRING, '
	'ESTADO:STRING, '
	'NIVEL_SERVICIO:STRING, '
	'FECHA_TV:STRING, '
	'FECHA_LIBERACION:STRING, '
	'FECHA_INTEGRACION:STRING, '
	'HORA_INTEGRACION:STRING, '
	'HORA:STRING, '
	'FECHA_FACTURA:STRING, '
	'F_VENCIMIENTO_CREDITO:STRING, '
	'CLASE_DE_CONDICION:STRING, '
	'NRO__ENTREGA:STRING, '
	'NRO__FACTURA:STRING, '
	'DOCTO__CLIENTE:STRING, '
	'NRO__CLIENTE:STRING, '
	'NOMBRE_CLIENTE:STRING, '
	'TELEFONO:STRING, '
	'EMAIL_CLIENTE:STRING, '
	'DIRECCION_CLIENTE:STRING, '
	'CIUDAD:STRING, '
	'DEPARTAMENTO:STRING, '
	'C__PEDIDO:STRING, '
	'C__OR__ENTREGA:STRING, '
	'C__DIF__ENTREGA:STRING, '
	'C__F__ENTREGA:STRING, '
	'C__DIF__PICKING:STRING, '
	'C__FACTURA:STRING, '
	'C__NO_ENVIADA:STRING, '
	'VLR__PEDIDO:STRING, '
	'VLR__PAYU:STRING, '
	'VLR__CREDITO_CLIENTE:STRING, '
	'CREDITO_CONTAB_:STRING, '
	'CREDITO_COMPENSADO:STRING, '
	'CREDITO_FALTANTE:STRING, '
	'VLR__COD:STRING, '
	'VLR__BONO:STRING, '
	'VLR__TIENDA:STRING, '
	'VLR__TOTAL:STRING, '
	'VLR__FACTURA:STRING, '
	'VLR__COSTO:STRING, '
	'VLR__UTILIDAD:STRING, '
	'MARGEN:STRING, '
	'VLR__DEBITOS:STRING, '
	'VLR__CREDITOS:STRING, '
	'VLR__SALDO_PEDIDO:STRING, '
	'ESTADO_CARTERA_PEDIDO:STRING, '
	'ESTADO_CARTERA_CLIENTE:STRING, '
	'VLR__SALDO_CLIENTE:STRING, '
	'NRO__GUIA:STRING, '
	'ESTADO_DESPACHO:STRING, '
	'FECHA_DESPACHO:STRING, '
	'TRASPORTADORA:STRING, '
	'CICLO_CAD:STRING, '
	'CICLO_TOTAL:STRING, '
	'EMAIL_ASESOR:STRING, '
	'NOMBRE_ASESOR:STRING, '
	'RECAUDADOR:STRING, '
	'CONTABILIZACION_FACTURA:STRING, '
	'OBSERVACIONES:STRING, '
	'FECHA_DE_PAGO:STRING '




	)
# ?
class formatearData(beam.DoFn):

	def __init__(self, mifecha):
		super(formatearData, self).__init__()
		self.mifecha = mifecha
	
	def process(self, element):
		# print(element)
		arrayCSV = element.split(';')

		tupla= {'idkey' : str(uuid.uuid4()),
				# 'fecha' : datetime.datetime.today().strftime('%Y-%m-%d'),
				'fecha': self.mifecha, 
				'NRO__PED__SAP' : arrayCSV[0],
				'NRO__PED__TV' : arrayCSV[1],
				'VIA_DE_PAGO' : arrayCSV[2],
				'C__CLASE_PEDIDO' : arrayCSV[3],
				'N__CLASE_PEDIDO' : arrayCSV[4],
				'ESTADO' : arrayCSV[5],
				'NIVEL_SERVICIO' : arrayCSV[6],
				'FECHA_TV' : arrayCSV[7],
				'FECHA_LIBERACION' : arrayCSV[8],
				'FECHA_INTEGRACION' : arrayCSV[9],
				'HORA_INTEGRACION' : arrayCSV[10],
				'HORA' : arrayCSV[11],
				'FECHA_FACTURA' : arrayCSV[12],
				'F_VENCIMIENTO_CREDITO' : arrayCSV[13],
				'CLASE_DE_CONDICION' : arrayCSV[14],
				'NRO__ENTREGA' : arrayCSV[15],
				'NRO__FACTURA' : arrayCSV[16],
				'DOCTO__CLIENTE' : arrayCSV[17],
				'NRO__CLIENTE' : arrayCSV[18],
				'NOMBRE_CLIENTE' : arrayCSV[19],
				'TELEFONO' : arrayCSV[20],
				'EMAIL_CLIENTE' : arrayCSV[21],
				'DIRECCION_CLIENTE' : arrayCSV[22],
				'CIUDAD' : arrayCSV[23],
				'DEPARTAMENTO' : arrayCSV[24],
				'C__PEDIDO' : arrayCSV[25],
				'C__OR__ENTREGA' : arrayCSV[26],
				'C__DIF__ENTREGA' : arrayCSV[27],
				'C__F__ENTREGA' : arrayCSV[28],
				'C__DIF__PICKING' : arrayCSV[29],
				'C__FACTURA' : arrayCSV[30],
				'C__NO_ENVIADA' : arrayCSV[31],
				'VLR__PEDIDO' : arrayCSV[32],
				'VLR__PAYU' : arrayCSV[33],
				'VLR__CREDITO_CLIENTE' : arrayCSV[34],
				'CREDITO_CONTAB_' : arrayCSV[35],
				'CREDITO_COMPENSADO' : arrayCSV[36],
				'CREDITO_FALTANTE' : arrayCSV[37],
				'VLR__COD' : arrayCSV[38],
				'VLR__BONO' : arrayCSV[39],
				'VLR__TIENDA' : arrayCSV[40],
				'VLR__TOTAL' : arrayCSV[41],
				'VLR__FACTURA' : arrayCSV[42],
				'VLR__COSTO' : arrayCSV[43],
				'VLR__UTILIDAD' : arrayCSV[44],
				'MARGEN' : arrayCSV[45],
				'VLR__DEBITOS' : arrayCSV[46],
				'VLR__CREDITOS' : arrayCSV[47],
				'VLR__SALDO_PEDIDO' : arrayCSV[48],
				'ESTADO_CARTERA_PEDIDO' : arrayCSV[49],
				'ESTADO_CARTERA_CLIENTE' : arrayCSV[50],
				'VLR__SALDO_CLIENTE' : arrayCSV[51],
				'NRO__GUIA' : arrayCSV[52],
				'ESTADO_DESPACHO' : arrayCSV[53],
				'FECHA_DESPACHO' : arrayCSV[54],
				'TRASPORTADORA' : arrayCSV[55],
				'CICLO_CAD' : arrayCSV[56],
				'CICLO_TOTAL' : arrayCSV[57],
				'EMAIL_ASESOR' : arrayCSV[58],
				'NOMBRE_ASESOR' : arrayCSV[59],
				'RECAUDADOR' : arrayCSV[60],
				'CONTABILIZACION_FACTURA' : arrayCSV[61],
				'OBSERVACIONES' : arrayCSV[62],
				'FECHA_DE_PAGO' : arrayCSV[63]




				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-sensus" #Definicion de la raiz del bucket
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

	transformed | 'Escritura a BigQuery Hana Hermeco' >> beam.io.WriteToBigQuery(
		gcs_project + ":Hermeco.Hana", 
		schema=TABLE_SCHEMA, 	
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")