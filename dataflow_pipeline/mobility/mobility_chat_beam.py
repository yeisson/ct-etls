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
		'ACCOUNTID:STRING, '
		'ENGAGEMENTID:STRING, '
		'ENGAGEMENTIDSEQ:STRING, '
		'SHARKENGAGEMENTID:STRING, '
		'CHATREQUESTEDTIME:STRING, '
		'CHATREQUESTEDTIMEL:STRING, '
		'WAITTIME:STRING, '
		'STARTTIMEL:STRING, '
		'STARTTIME:STRING, '
		'STARTTIMEUTC:STRING, '
		'STARTTIMELOCAL:STRING, '
		'STARTTIMEDATE:STRING, '
		'STARTTIMEYEAR:STRING, '
		'STARTTIMEMONTH:STRING, '
		'STARTTIMEMONTHSTR:STRING, '
		'STARTTIMEDAY:STRING, '
		'STARTTIMEWEEKDAY:STRING, '
		'STARTTIMEWEEKDAYSTR:STRING, '
		'STARTTIMETIMESTAMP:STRING, '
		'STARTTIMEHOUR:STRING, '
		'STARTTIMEMINUTE:STRING, '
		'STARTTIMEWEEKSUN:STRING, '
		'STARTTIMEWEEKMON:STRING, '
		'ENDTIMEL:STRING, '
		'ENDTIME:STRING, '
		'ENDTIMEUTC:STRING, '
		'ENDTIMELOCAL:STRING, '
		'ENDTIMEDATE:STRING, '
		'ENDTIMEYEAR:STRING, '
		'ENDTIMEMONTH:STRING, '
		'ENDTIMEMONTHSTR:STRING, '
		'ENDTIMEDAY:STRING, '
		'ENDTIMEWEEKDAY:STRING, '
		'ENDTIMEWEEKDAYSTR:STRING, '
		'ENDTIMETIMESTAMP:STRING, '
		'ENDTIMEHOUR:STRING, '
		'ENDTIMEMINUTE:STRING, '
		'ENDTIMEWEEKSUN:STRING, '
		'ENDTIMEWEEKMON:STRING, '
		'ISINTERACTIVE:STRING, '
		'DURATION:STRING, '
		'VISITORID:STRING, '
		'VISITORNAME:STRING, '
		'SKILLID:STRING, '
		'SKILLNAME:STRING, '
		'AGENTID:STRING, '
		'AGENTFULLNAME:STRING, '
		'AGENTNICKNAME:STRING, '
		'AGENTLOGINNAME:STRING, '
		'AGENTGROUPID:STRING, '
		'AGENTGROUPNAME:STRING, '
		'CHATSTARTURL:STRING, '
		'CHATSTARTPAGE:STRING, '
		'STARTREASON:STRING, '
		'STARTREASONDESC:STRING, '
		'ENDREASON:STRING, '
		'ENDREASONDESC:STRING, '
		'ENGAGEMENTSET:STRING, '
		'ENGAGEMENTSEQUENCE:STRING, '
		'MCS:STRING, '
		'MCSTREND:STRING, '
		'MCSMIN:STRING, '
		'MCSMAX:STRING, '
		'MCSDESCRIPTION:STRING, '
		'MCSGROUP:STRING, '
		'MCSRANK:STRING, '
		'MCSSTANDARD:STRING, '
		'MCSCOUNT:STRING, '
		'ALERTEDMCS:STRING, '
		'CHATMCS:STRING, '
		'CHATDATAENRICHED:STRING, '
		'ISPARTIAL:STRING, '
		'ENDED:STRING, '
		'INTERACTIVE:STRING, '
		'ISPOSTCHATSURVEY:STRING, '
		'ISAGENTSURVEY:STRING, '
		'ISPRECHATSURVEY:STRING, '
		'CHANNEL:STRING, '
		'CAMPAIGNENGAGEMENTID:STRING, '
		'CAMPAIGNENGAGEMENTNAME:STRING, '
		'CAMPAIGNID:STRING, '
		'CAMPAIGNNAME:STRING, '
		'GOALID:STRING, '
		'GOALNAME:STRING, '
		'VISITORBEHAVIORID:STRING, '
		'VISITORBEHAVIORNAME:STRING, '
		'VISITORPROFILEID:STRING, '
		'VISITORPROFILENAME:STRING, '
		'LOBID:STRING, '
		'LOBNAME:STRING, '
		'COUNTRY:STRING, '
		'COUNTRYCODE:STRING, '
		'STATE:STRING, '
		'CITY:STRING, '
		'ISP:STRING, '
		'ORG:STRING, '
		'DEVICE:STRING, '
		'IPADDRESS:STRING, '
		'BROWSER:STRING, '
		'OPERATINGSYSTEM:STRING, '
		'MESSAGECOUNT:STRING, '
		'MESSAGETIME:STRING, '
		'RESPONSETIME:STRING, '
		'RESPONSECOUNT:STRING, '
		'WORDS:STRING, '
		'QUESTIONS:STRING, '
		'MESSAGECOUNTAGENT:STRING, '
		'MESSAGECOUNTCONSUMER:STRING, '
		'MESSAGETIMEAGENT:STRING, '
		'MESSAGETIMECONSUMER:STRING, '
		'AVERAGEMESSAGETIME:STRING, '
		'AVERAGEMESSAGETIMEAGENT:STRING, '
		'AVERAGEMESSAGETIMECONSUMER:STRING, '
		'RESPONSECOUNTAGENT:STRING, '
		'RESPONSECOUNTCONSUMER:STRING, '
		'RESPONSETIMEAGENT:STRING, '
		'RESPONSETIMECONSUMER:STRING, '
		'AVERAGERESPONSETIME:STRING, '
		'AVERAGERESPONSETIMEAGENT:STRING, '
		'AVERAGERESPONSETIMECONSUMER:STRING, '
		'FIRSTRESPONDENT:STRING, '
		'FIRSTRESPONSETIMEAGENTFROMSTART:STRING, '
		'FIRSTRESPONSETIMEAGENTFROMCONSUMER:STRING, '
		'FIRSTRESPONSETIMECONSUMERFROMSTART:STRING, '
		'FIRSTRESPONSETIMECONSUMERFROMAGENT:STRING, '
		'WORDSAGENT:STRING, '
		'WORDSCONSUMER:STRING, '
		'QUESTIONSAGENT:STRING, '
		'QUESTIONSCONSUMER:STRING, '
		'COBROWSESESSIONCOUNT:STRING, '
		'COBROWSESESSIONID:STRING, '
		'COBROWSESTARTTIME:STRING, '
		'COBROWSESTARTTIMEL:STRING, '
		'COBROWSEENDTIME:STRING, '
		'COBROWSEENDTIMEL:STRING, '
		'COBROWSEENDREASON:STRING, '
		'COBROWSEDURATION:STRING, '
		'COBROWSEINTERACTIVE:STRING, '
		'SDES:STRING, '
		'PRECHAT_168637_5_CUESTIONARIO_PQRS_BIENVENIDO_A_AUTECO_CUENTANOS_COMO_PODEMOS_AYUDARTE_CON_TU_REQUERIMIENTO:STRING, '
		'PRECHAT_162445_1_PRE_CHAT_SURVEY_16_11_18_CUL_ES_TU_NOMBRE:STRING, '
		'PRECHAT_162445_5_PRE_CHAT_SURVEY_16_11_18_ELIGE_UNA_MARCA:STRING, '
		'PRECHAT_162445_172_PRE_CHAT_SURVEY_16_11_18_CUNTANOS_COMO_PODEMOS_AYUDARTE:STRING, '
		'POSTCHAT_165252_37_AUTECO_21_11_2018_QU_TAN_SATISFECHO_ESTS_CON_EL_AGENTE_DE_CHAT:STRING, '
		'POSTCHAT_165252_4_AUTECO_21_11_2018_CUL_ES_TU_SATISFACCION_GENERAL_CON_LA_ATENCION_RECIBIDA:STRING, '
		'POSTCHAT_165252_10_AUTECO_21_11_2018_TE_AYUDAMOS_A_RESOLVER_TUS_DUDAS:STRING, '
		'POSTCHAT_165252_29_AUTECO_21_11_2018_NOS_RECOMENDARIAS:STRING, '
		'POSTCHAT_165252_12_AUTECO_21_11_2018_AYUDANOS_A_MEJORAR_CON_TUS_COMENTARIOS_ADICIONALES:STRING, '
		'POSTCHAT_162638_4_AUTECO__POST_09_10_18_QU_TAN_SATISFECHO_ESTS_CON_EL_AGENTE_DE_CHAT:STRING, '
		'POSTCHAT_162638_9_AUTECO__POST_09_10_18_TE_AYUDAMOS_A_RESOLVER_TUS_DUDAS:STRING, '
		'POSTCHAT_162638_11_AUTECO__POST_09_10_18_EN_UNA_ESCALA_DE_0_A_10:STRING, '
		'POSTCHAT_162638_12_AUTECO__POST_09_10_18_AYUDANOS_A_MEJORAR_CON_TUS_COMENTARIOS_ADICIONALES:STRING, '
		'POSTCHAT_166096_9_CUESTIONARIO_1_NUEVA_PREGUNTA:STRING, '
		'POSTCHAT_162446_1_POST_CHAT_SURVEY_WOULD_YOU_LIKE_US_TO_EMAIL_YOU_A_TRANSCRIPT_OF_THIS_CHAT:STRING, '
		'POSTCHAT_162446_2_POST_CHAT_SURVEY_IF_YES_PLEASE_PROVIDE_YOUR_EMAIL:STRING, '
		'OPERATOR_162640_6_AGENT_AUTECO_09_10_18_ECOMMERCE_O_SAC:STRING, '
		'OPERATOR_162640_7_AGENT_AUTECO_09_10_18_VEHICULOS:STRING, '
		'OPERATOR_162640_8_AGENT_AUTECO_09_10_18_REPUESTOS:STRING, '
		'OPERATOR_162640_9_AGENT_AUTECO_09_10_18_SERVICIOS_POSVENTA:STRING, '
		'OPERATOR_162640_10_AGENT_AUTECO_09_10_18_CORPORATIVO:STRING, '
		'OPERATOR_162640_11_AGENT_AUTECO_09_10_18_ACCESORIOS:STRING, '
		'OPERATOR_162640_12_AGENT_AUTECO_09_10_18_CAMPANA_OUTBOUND:STRING, '
		'OPERATOR_162640_13_AGENT_AUTECO_09_10_18_DERECHO_DEL_CONSUMIDOR:STRING, '
		'OPERATOR_162640_14_AGENT_AUTECO_09_10_18_OTROS:STRING, '
		'OPERATOR_162447_6_AGENT_SURVEY_CODIGO_1:STRING, '
		'OPERATOR_162447_42_AGENT_SURVEY_CODIGO_2:STRING, '
		'OPERATOR_162447_43_AGENT_SURVEY_SOLO_PARA_MOBILITY:STRING, '
		'OPERATOR_166097_6_CUESTIONARIO_2_NUEVA_PREGUNTA:STRING '












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
				'fecha': self.mifecha,
				'ACCOUNTID' : arrayCSV[0],
				'ENGAGEMENTID' : arrayCSV[1],
				'ENGAGEMENTIDSEQ' : arrayCSV[2],
				'SHARKENGAGEMENTID' : arrayCSV[3],
				'CHATREQUESTEDTIME' : arrayCSV[4],
				'CHATREQUESTEDTIMEL' : arrayCSV[5],
				'WAITTIME' : arrayCSV[6],
				'STARTTIMEL' : arrayCSV[7],
				'STARTTIME' : arrayCSV[8],
				'STARTTIMEUTC' : arrayCSV[9],
				'STARTTIMELOCAL' : arrayCSV[10],
				'STARTTIMEDATE' : arrayCSV[11],
				'STARTTIMEYEAR' : arrayCSV[12],
				'STARTTIMEMONTH' : arrayCSV[13],
				'STARTTIMEMONTHSTR' : arrayCSV[14],
				'STARTTIMEDAY' : arrayCSV[15],
				'STARTTIMEWEEKDAY' : arrayCSV[16],
				'STARTTIMEWEEKDAYSTR' : arrayCSV[17],
				'STARTTIMETIMESTAMP' : arrayCSV[18],
				'STARTTIMEHOUR' : arrayCSV[19],
				'STARTTIMEMINUTE' : arrayCSV[20],
				'STARTTIMEWEEKSUN' : arrayCSV[21],
				'STARTTIMEWEEKMON' : arrayCSV[22],
				'ENDTIMEL' : arrayCSV[23],
				'ENDTIME' : arrayCSV[24],
				'ENDTIMEUTC' : arrayCSV[25],
				'ENDTIMELOCAL' : arrayCSV[26],
				'ENDTIMEDATE' : arrayCSV[27],
				'ENDTIMEYEAR' : arrayCSV[28],
				'ENDTIMEMONTH' : arrayCSV[29],
				'ENDTIMEMONTHSTR' : arrayCSV[30],
				'ENDTIMEDAY' : arrayCSV[31],
				'ENDTIMEWEEKDAY' : arrayCSV[32],
				'ENDTIMEWEEKDAYSTR' : arrayCSV[33],
				'ENDTIMETIMESTAMP' : arrayCSV[34],
				'ENDTIMEHOUR' : arrayCSV[35],
				'ENDTIMEMINUTE' : arrayCSV[36],
				'ENDTIMEWEEKSUN' : arrayCSV[37],
				'ENDTIMEWEEKMON' : arrayCSV[38],
				'ISINTERACTIVE' : arrayCSV[39],
				'DURATION' : arrayCSV[40],
				'VISITORID' : arrayCSV[41],
				'VISITORNAME' : arrayCSV[42],
				'SKILLID' : arrayCSV[43],
				'SKILLNAME' : arrayCSV[44],
				'AGENTID' : arrayCSV[45],
				'AGENTFULLNAME' : arrayCSV[46],
				'AGENTNICKNAME' : arrayCSV[47],
				'AGENTLOGINNAME' : arrayCSV[48],
				'AGENTGROUPID' : arrayCSV[49],
				'AGENTGROUPNAME' : arrayCSV[50],
				'CHATSTARTURL' : arrayCSV[51],
				'CHATSTARTPAGE' : arrayCSV[52],
				'STARTREASON' : arrayCSV[53],
				'STARTREASONDESC' : arrayCSV[54],
				'ENDREASON' : arrayCSV[55],
				'ENDREASONDESC' : arrayCSV[56],
				'ENGAGEMENTSET' : arrayCSV[57],
				'ENGAGEMENTSEQUENCE' : arrayCSV[58],
				'MCS' : arrayCSV[59],
				'MCSTREND' : arrayCSV[60],
				'MCSMIN' : arrayCSV[61],
				'MCSMAX' : arrayCSV[62],
				'MCSDESCRIPTION' : arrayCSV[63],
				'MCSGROUP' : arrayCSV[64],
				'MCSRANK' : arrayCSV[65],
				'MCSSTANDARD' : arrayCSV[66],
				'MCSCOUNT' : arrayCSV[67],
				'ALERTEDMCS' : arrayCSV[68],
				'CHATMCS' : arrayCSV[69],
				'CHATDATAENRICHED' : arrayCSV[70],
				'ISPARTIAL' : arrayCSV[71],
				'ENDED' : arrayCSV[72],
				'INTERACTIVE' : arrayCSV[73],
				'ISPOSTCHATSURVEY' : arrayCSV[74],
				'ISAGENTSURVEY' : arrayCSV[75],
				'ISPRECHATSURVEY' : arrayCSV[76],
				'CHANNEL' : arrayCSV[77],
				'CAMPAIGNENGAGEMENTID' : arrayCSV[78],
				'CAMPAIGNENGAGEMENTNAME' : arrayCSV[79],
				'CAMPAIGNID' : arrayCSV[80],
				'CAMPAIGNNAME' : arrayCSV[81],
				'GOALID' : arrayCSV[82],
				'GOALNAME' : arrayCSV[83],
				'VISITORBEHAVIORID' : arrayCSV[84],
				'VISITORBEHAVIORNAME' : arrayCSV[85],
				'VISITORPROFILEID' : arrayCSV[86],
				'VISITORPROFILENAME' : arrayCSV[87],
				'LOBID' : arrayCSV[88],
				'LOBNAME' : arrayCSV[89],
				'COUNTRY' : arrayCSV[90],
				'COUNTRYCODE' : arrayCSV[91],
				'STATE' : arrayCSV[92],
				'CITY' : arrayCSV[93],
				'ISP' : arrayCSV[94],
				'ORG' : arrayCSV[95],
				'DEVICE' : arrayCSV[96],
				'IPADDRESS' : arrayCSV[97],
				'BROWSER' : arrayCSV[98],
				'OPERATINGSYSTEM' : arrayCSV[99],
				'MESSAGECOUNT' : arrayCSV[100],
				'MESSAGETIME' : arrayCSV[101],
				'RESPONSETIME' : arrayCSV[102],
				'RESPONSECOUNT' : arrayCSV[103],
				'WORDS' : arrayCSV[104],
				'QUESTIONS' : arrayCSV[105],
				'MESSAGECOUNTAGENT' : arrayCSV[106],
				'MESSAGECOUNTCONSUMER' : arrayCSV[107],
				'MESSAGETIMEAGENT' : arrayCSV[108],
				'MESSAGETIMECONSUMER' : arrayCSV[109],
				'AVERAGEMESSAGETIME' : arrayCSV[110],
				'AVERAGEMESSAGETIMEAGENT' : arrayCSV[111],
				'AVERAGEMESSAGETIMECONSUMER' : arrayCSV[112],
				'RESPONSECOUNTAGENT' : arrayCSV[113],
				'RESPONSECOUNTCONSUMER' : arrayCSV[114],
				'RESPONSETIMEAGENT' : arrayCSV[115],
				'RESPONSETIMECONSUMER' : arrayCSV[116],
				'AVERAGERESPONSETIME' : arrayCSV[117],
				'AVERAGERESPONSETIMEAGENT' : arrayCSV[118],
				'AVERAGERESPONSETIMECONSUMER' : arrayCSV[119],
				'FIRSTRESPONDENT' : arrayCSV[120],
				'FIRSTRESPONSETIMEAGENTFROMSTART' : arrayCSV[121],
				'FIRSTRESPONSETIMEAGENTFROMCONSUMER' : arrayCSV[122],
				'FIRSTRESPONSETIMECONSUMERFROMSTART' : arrayCSV[123],
				'FIRSTRESPONSETIMECONSUMERFROMAGENT' : arrayCSV[124],
				'WORDSAGENT' : arrayCSV[125],
				'WORDSCONSUMER' : arrayCSV[126],
				'QUESTIONSAGENT' : arrayCSV[127],
				'QUESTIONSCONSUMER' : arrayCSV[128],
				'COBROWSESESSIONCOUNT' : arrayCSV[129],
				'COBROWSESESSIONID' : arrayCSV[130],
				'COBROWSESTARTTIME' : arrayCSV[131],
				'COBROWSESTARTTIMEL' : arrayCSV[132],
				'COBROWSEENDTIME' : arrayCSV[133],
				'COBROWSEENDTIMEL' : arrayCSV[134],
				'COBROWSEENDREASON' : arrayCSV[135],
				'COBROWSEDURATION' : arrayCSV[136],
				'COBROWSEINTERACTIVE' : arrayCSV[137],
				'SDES' : arrayCSV[138],
				'PRECHAT_168637_5_CUESTIONARIO_PQRS_BIENVENIDO_A_AUTECO_CUENTANOS_COMO_PODEMOS_AYUDARTE_CON_TU_REQUERIMIENTO' : arrayCSV[139],
				'PRECHAT_162445_1_PRE_CHAT_SURVEY_16_11_18_CUL_ES_TU_NOMBRE' : arrayCSV[140],
				'PRECHAT_162445_5_PRE_CHAT_SURVEY_16_11_18_ELIGE_UNA_MARCA' : arrayCSV[141],
				'PRECHAT_162445_172_PRE_CHAT_SURVEY_16_11_18_CUNTANOS_COMO_PODEMOS_AYUDARTE' : arrayCSV[142],
				'POSTCHAT_165252_37_AUTECO_21_11_2018_QU_TAN_SATISFECHO_ESTS_CON_EL_AGENTE_DE_CHAT' : arrayCSV[143],
				'POSTCHAT_165252_4_AUTECO_21_11_2018_CUL_ES_TU_SATISFACCION_GENERAL_CON_LA_ATENCION_RECIBIDA' : arrayCSV[144],
				'POSTCHAT_165252_10_AUTECO_21_11_2018_TE_AYUDAMOS_A_RESOLVER_TUS_DUDAS' : arrayCSV[145],
				'POSTCHAT_165252_29_AUTECO_21_11_2018_NOS_RECOMENDARIAS' : arrayCSV[146],
				'POSTCHAT_165252_12_AUTECO_21_11_2018_AYUDANOS_A_MEJORAR_CON_TUS_COMENTARIOS_ADICIONALES' : arrayCSV[147],
				'POSTCHAT_162638_4_AUTECO__POST_09_10_18_QU_TAN_SATISFECHO_ESTS_CON_EL_AGENTE_DE_CHAT' : arrayCSV[148],
				'POSTCHAT_162638_9_AUTECO__POST_09_10_18_TE_AYUDAMOS_A_RESOLVER_TUS_DUDAS' : arrayCSV[149],
				'POSTCHAT_162638_11_AUTECO__POST_09_10_18_EN_UNA_ESCALA_DE_0_A_10' : arrayCSV[150],
				'POSTCHAT_162638_12_AUTECO__POST_09_10_18_AYUDANOS_A_MEJORAR_CON_TUS_COMENTARIOS_ADICIONALES' : arrayCSV[151],
				'POSTCHAT_166096_9_CUESTIONARIO_1_NUEVA_PREGUNTA' : arrayCSV[152],
				'POSTCHAT_162446_1_POST_CHAT_SURVEY_WOULD_YOU_LIKE_US_TO_EMAIL_YOU_A_TRANSCRIPT_OF_THIS_CHAT' : arrayCSV[153],
				'POSTCHAT_162446_2_POST_CHAT_SURVEY_IF_YES_PLEASE_PROVIDE_YOUR_EMAIL' : arrayCSV[154],
				'OPERATOR_162640_6_AGENT_AUTECO_09_10_18_ECOMMERCE_O_SAC' : arrayCSV[155],
				'OPERATOR_162640_7_AGENT_AUTECO_09_10_18_VEHICULOS' : arrayCSV[156],
				'OPERATOR_162640_8_AGENT_AUTECO_09_10_18_REPUESTOS' : arrayCSV[157],
				'OPERATOR_162640_9_AGENT_AUTECO_09_10_18_SERVICIOS_POSVENTA' : arrayCSV[158],
				'OPERATOR_162640_10_AGENT_AUTECO_09_10_18_CORPORATIVO' : arrayCSV[159],
				'OPERATOR_162640_11_AGENT_AUTECO_09_10_18_ACCESORIOS' : arrayCSV[160],
				'OPERATOR_162640_12_AGENT_AUTECO_09_10_18_CAMPANA_OUTBOUND' : arrayCSV[161],
				'OPERATOR_162640_13_AGENT_AUTECO_09_10_18_DERECHO_DEL_CONSUMIDOR' : arrayCSV[162],
				'OPERATOR_162640_14_AGENT_AUTECO_09_10_18_OTROS' : arrayCSV[163],
				'OPERATOR_162447_6_AGENT_SURVEY_CODIGO_1' : arrayCSV[164],
				'OPERATOR_162447_42_AGENT_SURVEY_CODIGO_2' : arrayCSV[165],
				'OPERATOR_162447_43_AGENT_SURVEY_SOLO_PARA_MOBILITY' : arrayCSV[166],
				'OPERATOR_166097_6_CUESTIONARIO_2_NUEVA_PREGUNTA' : arrayCSV[167],










				}
		
		return [tupla]



def run(archivo, mifecha):

	gcs_path = "gs://ct-pto" #Definicion de la raiz del bucket
	gcs_project = "contento-bi"

	mi_runer = ("DirectRunner", "DataflowRunner")[socket.gethostname()=="contentobi"]
	pipeline =  beam.Pipeline(runner=mi_runer, argv=[
        "--project", gcs_project,
        "--staging_location", ("%s/dataflow_files/staging_location" % gcs_path),
        "--temp_location", ("%s/dataflow_files/temp" % gcs_path),
        "--output", ("%s/dataflow_files/output" % gcs_path),
        "--setup_file", "./setup.py",
        "--max_num_workers", "10",
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

	transformed | 'Escritura a BigQuery Mobility' >> beam.io.WriteToBigQuery(
		gcs_project + ":Auteco_Mobility.chat", 
		schema=TABLE_SCHEMA, 
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
		)

	# transformed | 'Borrar Archivo' >> FileSystems.delete('gs://ct-avon/prejuridico/AVON_INF_PREJ_20181111.TXT')
	# 'Eliminar' >> FileSystems.delete (["archivos/Info_carga_avon.1.txt"])

	jobObject = pipeline.run()
	# jobID = jobObject.job_id()

	return ("Corrio Full HD")



