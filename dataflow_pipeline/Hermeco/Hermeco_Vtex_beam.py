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
	'ORIGIN:STRING, '
	'ORDER_A:STRING, '
	'SEQUENCE:STRING, '
	'CREATION_DATE:STRING, '
	'CLIENT_NAME:STRING, '
	'CLIENT_LAST_NAME:STRING, '
	'CLIENT_DOCUMENT:STRING, '
	'EMAIL:STRING, '
	'PHONE:STRING, '
	'UF:STRING, '
	'CITY:STRING, '
	'ADDRESS_IDENTIFICATION:STRING, '
	'ADDRESS_TYPE:STRING, '
	'RECEIVER_NAME:STRING, '
	'STREET:STRING, '
	'NUMBER:STRING, '
	'COMPLEMENT:STRING, '
	'NEIGHBORHOOD:STRING, '
	'REFERENCE:STRING, '
	'POSTAL_CODE:STRING, '
	'SLA_TYPE:STRING, '
	'COURRIER:STRING, '
	'ESTIMATE_DELIVERY_DATE:STRING, '
	'DELIVERY_DEADLINE:STRING, '
	'STATUS:STRING, '
	'LAST_CHANGE_DATE:STRING, '
	'UTMMEDIUM:STRING, '
	'UTMSOURCE:STRING, '
	'UTMCAMPAIGN:STRING, '
	'COUPON:STRING, '
	'PAYMENT_SYSTEM_NAME:STRING, '
	'INSTALLMENTS:STRING, '
	'PAYMENT_VALUE:STRING, '
	'QUANTITY_SKU:STRING, '
	'ID_SKU:STRING, '
	'CATEGORY_IDS_SKU:STRING, '
	'REFERENCE_CODE:STRING, '
	'SKU_NAME:STRING, '
	'SKU_VALUE:STRING, '
	'SKU_SELLING_PRICE:STRING, '
	'SKU_TOTAL_PRICE:STRING, '
	'SKU_PATH:STRING, '
	'ITEM_ATTACHMENTS:STRING, '
	'LIST_ID:STRING, '
	'LIST_TYPE_NAME:STRING, '
	'SERVICE_PRICE__SELLING_PRICE:STRING, '
	'SHIPPING_LIST_PRICE:STRING, '
	'SHIPPING_VALUE:STRING, '
	'TOTAL_VALUE:STRING, '
	'DISCOUNTS_TOTALS:STRING, '
	'DISCOUNTS_NAMES:STRING, '
	'CALL_CENTER_EMAIL:STRING, '
	'CALL_CENTER_CODE:STRING, '
	'TRACKING_NUMBER:STRING, '
	'HOST:STRING, '
	'GIFTREGISTRY_ID:STRING, '
	'SELLER_NAME:STRING, '
	'STATUS_TIMELINE:STRING, '
	'OBS:STRING, '
	'UTMIPART:STRING, '
	'UTMICAMPAIGN:STRING, '
	'UTMIPAGE:STRING, '
	'SELLER_ORDER_ID:STRING, '
	'ACQUIRER:STRING, '
	'AUTHORIZATION_ID:STRING, '
	'TID:STRING, '
	'NSU:STRING, '
	'CARD_FIRST_DIGITS:STRING, '
	'CARD_LAST_DIGITS:STRING, '
	'PAYMENT_APPROVED_BY:STRING, '
	'CANCELLED_BY:STRING, '
	'CANCELLATION_REASON:STRING, '
	'GIFT_CARD_NAME:STRING, '
	'GIFT_CARD_CAPTION:STRING, '
	'AUTHORIZED_DATE:STRING, '
	'CORPORATE_NAME:STRING, '
	'CORPORATE_DOCUMENT:STRING, '
	'TRANSACTIONID:STRING, '
	'PAYMENTID:STRING, '
	'SALESCHANNEL:STRING, '
	'MARKETINGTAGS:STRING, '
	'DELIVERED:STRING, '
	'SKU_REWARDVALUE:STRING, '
	'IS_MARKETPLACE_CETIFIED:STRING, '
	'IS_CHECKED_IN:STRING, '
	'CURRENCY_CODE:STRING, '
	'TAXES:STRING, '
	'INVOICE_NUMBERS:STRING, '
	'COUNTRY:STRING, '
	'INPUT_INVOICES_NUMBERS:STRING, '
	'OUTPUT_INVOICES_NUMBERS:STRING, '
	'STATUS_RAW_VALUE__TEMPORARY:STRING '




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
				'ORIGIN' : arrayCSV[0],
				'ORDER_A' : arrayCSV[1],
				'SEQUENCE' : arrayCSV[2],
				'CREATION_DATE' : arrayCSV[3],
				'CLIENT_NAME' : arrayCSV[4],
				'CLIENT_LAST_NAME' : arrayCSV[5],
				'CLIENT_DOCUMENT' : arrayCSV[6],
				'EMAIL' : arrayCSV[7],
				'PHONE' : arrayCSV[8],
				'UF' : arrayCSV[9],
				'CITY' : arrayCSV[10],
				'ADDRESS_IDENTIFICATION' : arrayCSV[11],
				'ADDRESS_TYPE' : arrayCSV[12],
				'RECEIVER_NAME' : arrayCSV[13],
				'STREET' : arrayCSV[14],
				'NUMBER' : arrayCSV[15],
				'COMPLEMENT' : arrayCSV[16],
				'NEIGHBORHOOD' : arrayCSV[17],
				'REFERENCE' : arrayCSV[18],
				'POSTAL_CODE' : arrayCSV[19],
				'SLA_TYPE' : arrayCSV[20],
				'COURRIER' : arrayCSV[21],
				'ESTIMATE_DELIVERY_DATE' : arrayCSV[22],
				'DELIVERY_DEADLINE' : arrayCSV[23],
				'STATUS' : arrayCSV[24],
				'LAST_CHANGE_DATE' : arrayCSV[25],
				'UTMMEDIUM' : arrayCSV[26],
				'UTMSOURCE' : arrayCSV[27],
				'UTMCAMPAIGN' : arrayCSV[28],
				'COUPON' : arrayCSV[29],
				'PAYMENT_SYSTEM_NAME' : arrayCSV[30],
				'INSTALLMENTS' : arrayCSV[31],
				'PAYMENT_VALUE' : arrayCSV[32],
				'QUANTITY_SKU' : arrayCSV[33],
				'ID_SKU' : arrayCSV[34],
				'CATEGORY_IDS_SKU' : arrayCSV[35],
				'REFERENCE_CODE' : arrayCSV[36],
				'SKU_NAME' : arrayCSV[37],
				'SKU_VALUE' : arrayCSV[38],
				'SKU_SELLING_PRICE' : arrayCSV[39],
				'SKU_TOTAL_PRICE' : arrayCSV[40],
				'SKU_PATH' : arrayCSV[41],
				'ITEM_ATTACHMENTS' : arrayCSV[42],
				'LIST_ID' : arrayCSV[43],
				'LIST_TYPE_NAME' : arrayCSV[44],
				'SERVICE_PRICE__SELLING_PRICE' : arrayCSV[45],
				'SHIPPING_LIST_PRICE' : arrayCSV[46],
				'SHIPPING_VALUE' : arrayCSV[47],
				'TOTAL_VALUE' : arrayCSV[48],
				'DISCOUNTS_TOTALS' : arrayCSV[49],
				'DISCOUNTS_NAMES' : arrayCSV[50],
				'CALL_CENTER_EMAIL' : arrayCSV[51],
				'CALL_CENTER_CODE' : arrayCSV[52],
				'TRACKING_NUMBER' : arrayCSV[53],
				'HOST' : arrayCSV[54],
				'GIFTREGISTRY_ID' : arrayCSV[55],
				'SELLER_NAME' : arrayCSV[56],
				'STATUS_TIMELINE' : arrayCSV[57],
				'OBS' : arrayCSV[58],
				'UTMIPART' : arrayCSV[59],
				'UTMICAMPAIGN' : arrayCSV[60],
				'UTMIPAGE' : arrayCSV[61],
				'SELLER_ORDER_ID' : arrayCSV[62],
				'ACQUIRER' : arrayCSV[63],
				'AUTHORIZATION_ID' : arrayCSV[64],
				'TID' : arrayCSV[65],
				'NSU' : arrayCSV[66],
				'CARD_FIRST_DIGITS' : arrayCSV[67],
				'CARD_LAST_DIGITS' : arrayCSV[68],
				'PAYMENT_APPROVED_BY' : arrayCSV[69],
				'CANCELLED_BY' : arrayCSV[70],
				'CANCELLATION_REASON' : arrayCSV[71],
				'GIFT_CARD_NAME' : arrayCSV[72],
				'GIFT_CARD_CAPTION' : arrayCSV[73],
				'AUTHORIZED_DATE' : arrayCSV[74],
				'CORPORATE_NAME' : arrayCSV[75],
				'CORPORATE_DOCUMENT' : arrayCSV[76],
				'TRANSACTIONID' : arrayCSV[77],
				'PAYMENTID' : arrayCSV[78],
				'SALESCHANNEL' : arrayCSV[79],
				'MARKETINGTAGS' : arrayCSV[80],
				'DELIVERED' : arrayCSV[81],
				'SKU_REWARDVALUE' : arrayCSV[82],
				'IS_MARKETPLACE_CETIFIED' : arrayCSV[83],
				'IS_CHECKED_IN' : arrayCSV[84],
				'CURRENCY_CODE' : arrayCSV[85],
				'TAXES' : arrayCSV[86],
				'INVOICE_NUMBERS' : arrayCSV[87],
				'COUNTRY' : arrayCSV[88],
				'INPUT_INVOICES_NUMBERS' : arrayCSV[89],
				'OUTPUT_INVOICES_NUMBERS' : arrayCSV[90],
				'STATUS_RAW_VALUE__TEMPORARY' : arrayCSV[91]






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

	transformed | 'Escritura a BigQuery Vtex Hermeco' >> beam.io.WriteToBigQuery(
		gcs_project + ":Hermeco.Vtex", 
		schema=TABLE_SCHEMA, 	
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")