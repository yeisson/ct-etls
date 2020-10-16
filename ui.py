from flask import Blueprint, jsonify, render_template, Flask, url_for
import procesos.pyg as pyg
import procesos.universidad_cooperativa_col as ucc
import procesos.metlife as met
import procesos.bancolombia_castigada as bancolombia_castigada
import os

my_resourses = os.path.join('static','images')


ui_api = Blueprint('ui_api', __name__)

myApp = Flask(__name__)
myApp.config['my_resourses'] = my_resourses



# Vistas Principales

# Vista del Informe financiero PyG
# ----------------------------------
@myApp.route("/")
@ui_api.route("/cargues_pyg")
def ui_cargues_pyg():
    return render_template('financiero/index.html')#user_image = full_filename)
# ----------------------------------

# Vista de las Operaciones Unificadas
# ----------------------------------
@myApp.route("/")
@ui_api.route("/cargues_unificadas")
def ui_cargues_unificadas():
    return render_template('unificadas/index.html')
# ----------------------------------

# Vista de Metlife
# ----------------------------------
@myApp.route("/")
@ui_api.route("/cargues_metlife")
def ui_cargues_metlife():
    return render_template('metlife/index.html')
# ----------------------------------

# Vista de Bancolombia Castigada
# ----------------------------------
@myApp.route("/")
@ui_api.route("/cargues_bancolombia_castigada")
def ui_cargues_bancolombia_castigada():
    return render_template('bancolombia_cast/index.html')
# ----------------------------------

# Invoca a cada ETL individualmente

# Informe Financiero PyG:
# ----------------------------------
@ui_api.route("/cargar_agentes")
def ui_cargar_agentes():      
    return pyg.pyg_agentes()

@ui_api.route("/cargar_ajustes")
def ui_cargar_ajustes():      
    return pyg.pyg_ajustes()

@ui_api.route("/cargar_ceco")
def ui_cargar_ceco():      
    return pyg.pyg_CeCo()

@ui_api.route("/cargar_ofima")
def ui_cargar_ofima():      
    return pyg.pyg_ofima()

@ui_api.route("/cargar_puc")
def ui_cargar_puc():      
    return pyg.pyg_PUC()
 
@ui_api.route("/cargar_puestos")
def ui_cargar_puestos():      
    return pyg.pyg_puestos()

# ----------------------------------    

# Operaciones Unificadas
# ----------------------------------    
@ui_api.route("/cargar_inscritos")
def ui_cargar_inscritos():      
    return ucc.archivos_inscritos()

@ui_api.route("/cargar_integracion")
def ui_cargar_integracion():      
    return ucc.archivos_integracion()

@ui_api.route("/cargar_sms")
def ui_cargar_sms():      
    return ucc.archivos_sms()

@ui_api.route("/cargar_agentv")
def ui_cargar_agentv():      
    return ucc.archivos_agentev()

@ui_api.route("/cargar_campanas")
def ui_cargar_campanas():      
    return ucc.archivos_campanas()

# ----------------------------------    

# Metlife
# ----------------------------------    
@ui_api.route("/cargar_metlife_seguimiento")
def ui_cargar_metlife_seguimiento():      
    return met.archivos_seguimiento_metlife()    

# ----------------------------------    

# Bancolombia Castigada
# ----------------------------------    
@ui_api.route("/cargar_bancolombia_castigada/<base>")
def ui_cargar_bancolombia_castigada(base):      
    return bancolombia_castigada.robotics(base)

# ----------------------------------    

# Prueba invocacion optimizada
@ui_api.route("/cargar")
def ui_cargar(base):  

    myProcess = [pyg.pyg_agentes(),pyg.pyg_ajustes(),pyg.pyg_CeCo()]

    return myProcess[base]