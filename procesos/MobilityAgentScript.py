from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.metlife.Metlife_BM_beam as Metlife_BM_beam
import dataflow_pipeline.metlife.metlife_seguimiento_beam as metlife_seguimiento_beam
import os
import socket
import procesos.descargas as descargas
import requests
from flask import request


fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

Mobility_Agent_Script = Blueprint('Mobility_Agent_Script', __name__)
@Mobility_Agent_Script.route("/descargar", methods=['POST','GET'])

def Descarga_Encuesta():
       
    dateini= request.args.get('desde')
    dateend= request.args.get('hasta')
   
  
    myRoute = '/BI_Archivos/GOOGLE/Mobility/Perfil_Cliente/'+dateini+'_'+dateend+'.csv'  
    myQuery ='SELECT * FROM `contento-bi.Auteco_Mobility.Informe_Perfil_Cliente_Agent_Script_Completo`  where Fecha_Encuesta between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'    
    myHeader = ["BASE" , "ESTADO" , "ANOCREACION" , "MESCREACION" , "FECHACREACION" , "TERCEROPROPIETARIO" , "NITPROPIETARIO" , "DIRECCIONPROPIETARIO" , "CIUDAD" , "DEPARTAMENTO" , "TELEFONOFIJO" , "CELULAR" , "OTROTELEFONO" , "EMAIL" , "AUTORIZAHABEASDATAPROPIETARIO" , "CHASISSERIE" , "MODELO" , "MARCA" , "LINEA" , "RAZONSOCIALESTABLECIMIENTO" , "MODELO_LINEA" , "UNIDAD_DE_NEGOCIO" , "CATEGORIA" , "SUBCATEGORIA" , "SEGMENTO" , "SUBSEGMENTO" , "MARCA2" , "Fecha_Encuesta" , "Hora_Exacta" , "Hora" , "Responde_Encuesta" , "Podriamos_Comunicarnos_Con_Usted_En_Otro_Horario" , "Motivo" , "Segun_Genero_Es_Femenino_O_Masculino" , "Es_Usted_El_Propietario_Del_Vehiculo" , "Cedula_Cliente" , "Es_Usted_El_Usuario_Permanente_Del_Vehiculo" , "De_Manera_General" , "Con_Que_Frecuencia_Conduce_El_Vehiculo" , "Cual_Es_El_Uso_Principal_Que_Le_Da_A_Su_Vehiculo" , "Herramienta_Trabajo" , "Otra_Herramienta_Trabajo" , "Cual_Es_La_Actividad_Economica_Principal_Que_Usted_Desempena" , "Por_Favor_Me_Indica_Su_Fecha_De_Nacimiento" , "Edad" , "Rango_Edad" , "Cual_Es_Su_Estado_Civil" , "Cual_Es_El_Maximo_Nivel_De_Estudios_Que_Ha_Alcanzado_Hasta_El_Momento" , "Cual_Es_El_Estrato_Del_Lugar_Donde_Vives" , "Tienes_Hijos" , "Cuantas_Personas_Dependen_Economicamente_De_Usted" , "Cual_Es_Su_Nivel_De_Ingresos" , "Realizo_Algun_Prestamo_Para_La_Compra" , "En_Que_Entidad_Solicito_El_Prestamo" , "Otra_Entidad_Categoria" , "Cual_Fue_La_Entidad_Con_La_Cual_Realizo_El_Prestamo" , "Otra_Entidad" , "Quien_Le_Ayudo_A_Tomar_La_Decision_De_La_Marca_Y_Modelo_Que_Deberia_Comprar" , "Otro_Ayuda" , "Tenia_Un_Vehiculo_Antes_De_Adquirir_Este_Cual" , "Categoria_Vehiculo" , "Cuantas_Ha_Tenido_Antes" , "Hace_Cuanto_Tiempo_La_Habia_Adquirido" , "Categoria_Marca" , "Cual_Era_La_Marca" , "Recuerda_La_Referencia" , "Era_Nuevo_O_Usado" , "En_Caso_De_Comprar__En_El_Futuro_Cual_Le_Gustaria_Comprar" , "Marca_Compra" , "Usa_Y_Conduce_Otro_Vehiculo_Adicional_Al_Que_Compro_Con_Nosotros" , "Cual_Usa" , "Cedula_Cliente3" , "Usa_Facebook" , "Usa_Instragram" , "Usa_Twitter" , "Usa_Youtube" , "Usa_Whatsapp" , "No_Usa_Ninguna" , "Calificacion_Calidad_" , "Calificacion_El_Diseno_" , "Calificacion_Garantia_Y_Respaldo_" , "Calificacion_Tecnologia__" , "Calificacion_Desempeno_" , "Calificacion_Asesoria__Servicio_" , "Calificacion_Ubicacion_O_Cercania_Del_Punto_De_Venta__" , "Calificacion_Opciones_De_Financiacion_" , "Calificacion_Precio_" , "Calificacion_Los_Descuentos_" , "Calificacion_Recomendacion" , "Id_Llamada" , "Asesor"
    ]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)
 


