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
			'Nit:STRING,'
			'Consdocdeu:STRING,'
			'Clasif:STRING,'
			'Grupo:STRING,'
			'Segmento:STRING,'
			'Contacto:STRING,'
			'Valor_Obligacion:STRING,'
			'Valor_Vencido:STRING,'
			'Dias_Mora:STRING,'
			'Cant_Pagos:STRING,'
			'Vr_Pagado:STRING,'
			'Cta_Dia:STRING,'
			'Desc_Ultimo_Codigo_De_Gestion_Prejuridico:STRING,'
			'Asignado:STRING,'
			'Contactabilidad:STRING,'
			'Foco_Plan_Trabajo:STRING,'
			'Ult_Contacto_Dir:STRING,'
			'Dia_Ult_contacto:STRING,'
			'T_Ult_contacto:STRING,'
			'T_Asignacion:STRING,'
			'Total_Gest:STRING,'
			'Total_Contactos:STRING,'
			'Total_SMS:STRING,'
			'Gest_Lun-Vier:STRING,'
			'Gest_Sab-Dom:STRING,'
			'Contactos_Lun-Vier:STRING,'
			'Contactos_Sab-Dom:STRING,'
			'Gestiones_<_Ult_Contacto:STRING,'
			'Gest_Agente_>_Ult_Contacto:STRING,'
			'SMS_>_Ult_Contacto:STRING,'
			'Contactab_Lun-Vier:STRING,'
			'Contactab_Sab-Dom:STRING,'
			'Contactab_Lun:STRING,'
			'Contactab_Mar:STRING,'
			'Contactab_Mier:STRING,'
			'Contactab_Jue:STRING,'
			'Contactab_Vier:STRING,'
			'Contactab_Sab:STRING,'
			'Contactab_Dom:STRING,'
			'Contactab_Manana:STRING,'
			'Contactab_Medio_Dia:STRING,'
			'Contactab_Tarde:STRING,'
			'Contactab_Noche:STRING,'
			'6._Contactab_6_am:STRING,'
			'7._Contactab_7_am:STRING,'
			'8._Contactab_8_am:STRING,'
			'9._Contactab_9_am:STRING,'
			'10._Contactab_10_am:STRING,'
			'11._Contactab_11_am:STRING,'
			'12._Contactab_12_pm:STRING,'
			'13._Contactab_1_pm:STRING,'
			'14._Contactab_2_pm:STRING,'
			'15._Contactab_3_pm:STRING,'
			'16._Contactab_4_pm:STRING,'
			'17._Contactab_5_pm:STRING,'
			'18._Contactab_6_pm:STRING,'
			'19._Contactab_7_pm:STRING,'
			'20._Contactab_8_pm:STRING,'
			'21._Contactab_9_pm:STRING,'
			'Gest_6_AM:STRING,'
			'Gest_Manana:STRING,'
			'Gest_Medio_dia:STRING,'
			'Gest_Tarde:STRING,'
			'Gest_Noche:STRING,'
			'Lun:STRING,'
			'Mar:STRING,'
			'Mier:STRING,'
			'Jue:STRING,'
			'Vier:STRING,'
			'Sab:STRING,'
			'Dom:STRING,'
			'06_00_am:STRING,'
			'07_00_am:STRING,'
			'08_00_am:STRING,'
			'09_00_am:STRING,'
			'10_00_am:STRING,'
			'11_00_am:STRING,'
			'12_00_am:STRING,'
			'01_00_pm:STRING,'
			'02_00_pm:STRING,'
			'03_00_pm:STRING,'
			'04_00_pm:STRING,'
			'05_00_pm:STRING,'
			'06_00_pm:STRING,'
			'07_00_pm:STRING,'
			'08_00_pm:STRING,'
			'09_00_pm:STRING'
			)

class formatearData(beam.DoFn):
	
	def process(self, element):
		arrayCSV = element.split('|')

		tupla= {
				'Nit': arrayCSV[0],
				'Consdocdeu': arrayCSV[1],
				'Clasif': arrayCSV[2],
				'Grupo': arrayCSV[3],
				'Segmento': arrayCSV[4],
				'Contacto': arrayCSV[5],
				'Valor_Obligacion': arrayCSV[6],
				'Valor_Vencido': arrayCSV[7],
				'Dias_Mora': arrayCSV[8],
				'Cant_Pagos': arrayCSV[9],
				'Vr_Pagado': arrayCSV[10],
				'Cta_Dia': arrayCSV[11],
				'Desc_Ultimo_Codigo_De_Gestion_Prejuridico': arrayCSV[12],
				'Asignado': arrayCSV[13],
				'Contactabilidad': arrayCSV[14],
				'Foco_Plan_Trabajo': arrayCSV[15],
				'Ult_Contacto_Dir': arrayCSV[16],
				'Dia_Ult_contacto': arrayCSV[17],
				'T_Ult_contacto': arrayCSV[18],
				'T_Asignacion': arrayCSV[19],
				'Total_Gest': arrayCSV[20],
				'Total_Contactos': arrayCSV[21],
				'Total_SMS': arrayCSV[22],
				'Gest_Lun-Vier': arrayCSV[23],
				'Gest_Sab-Dom': arrayCSV[24],
				'Contactos_Lun-Vier': arrayCSV[25],
				'Contactos_Sab-Dom': arrayCSV[26],
				'Gestiones_<_Ult_Contacto': arrayCSV[27],
				'Gest_Agente_>_Ult_Contacto': arrayCSV[28],
				'SMS_>_Ult_Contacto': arrayCSV[29],
				'Contactab_Lun-Vier': arrayCSV[30],
				'Contactab_Sab-Dom': arrayCSV[31],
				'Contactab_Lun': arrayCSV[32],
				'Contactab_Mar': arrayCSV[33],
				'Contactab_Mier': arrayCSV[34],
				'Contactab_Jue': arrayCSV[35],
				'Contactab_Vier': arrayCSV[36],
				'Contactab_Sab': arrayCSV[37],
				'Contactab_Dom': arrayCSV[38],
				'Contactab_Manana': arrayCSV[39],
				'Contactab_Medio_Dia': arrayCSV[40],
				'Contactab_Tarde': arrayCSV[41],
				'Contactab_Noche': arrayCSV[42],
				'6._Contactab_6_am': arrayCSV[43],
				'7._Contactab_7_am': arrayCSV[44],
				'8._Contactab_8_am': arrayCSV[45],
				'9._Contactab_9_am': arrayCSV[46],
				'10._Contactab_10_am': arrayCSV[47],
				'11._Contactab_11_am': arrayCSV[48],
				'12._Contactab_12_pm': arrayCSV[49],
				'13._Contactab_1_pm': arrayCSV[50],
				'14._Contactab_2_pm': arrayCSV[51],
				'15._Contactab_3_pm': arrayCSV[52],
				'16._Contactab_4_pm': arrayCSV[53],
				'17._Contactab_5_pm': arrayCSV[54],
				'18._Contactab_6_pm': arrayCSV[55],
				'19._Contactab_7_pm': arrayCSV[56],
				'20._Contactab_8_pm': arrayCSV[57],
				'21._Contactab_9_pm': arrayCSV[58],
				'Gest_6_AM': arrayCSV[59],
				'Gest_Manana': arrayCSV[60],
				'Gest_Medio_dia': arrayCSV[61],
				'Gest_Tarde': arrayCSV[62],
				'Gest_Noche': arrayCSV[63],
				'Lun': arrayCSV[64],
				'Mar': arrayCSV[65],
				'Mier': arrayCSV[66],
				'Jue': arrayCSV[67],
				'Vier': arrayCSV[68],
				'Sab': arrayCSV[69],
				'Dom': arrayCSV[70],
				'06_00_am': arrayCSV[71],
				'07_00_am': arrayCSV[72],
				'08_00_am': arrayCSV[73],
				'09_00_am': arrayCSV[74],
				'10_00_am': arrayCSV[75],
				'11_00_am': arrayCSV[76],
				'12_00_am': arrayCSV[77],
				'01_00_pm': arrayCSV[78],
				'02_00_pm': arrayCSV[79],
				'03_00_pm': arrayCSV[80],
				'04_00_pm': arrayCSV[81],
				'05_00_pm': arrayCSV[82],
				'06_00_pm': arrayCSV[83],
				'07_00_pm': arrayCSV[84],
				'08_00_pm': arrayCSV[85],
				'09_00_pm': arrayCSV[86]
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

	lines = pipeline | 'Lectura de Archivo' >> ReadFromText(gcs_path + "/" + FECHA_CARGUE + "_5" +".csv")
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