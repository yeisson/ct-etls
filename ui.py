from flask import Blueprint, jsonify, render_template, Flask, url_for
import procesos.pyg as pyg
import os

my_resourses = os.path.join('static','images')


ui_api = Blueprint('ui_api', __name__)

myApp = Flask(__name__)
myApp.config['my_resourses'] = my_resourses

@myApp.route("/")
@ui_api.route("/cargues_pyg")
def ui_cargues_pyg():
    return render_template('financiero/index.html')#user_image = full_filename)


# Invoca a cada ETL individualmente
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

# Prueba invocacion optimizada
@ui_api.route("/cargar")
def ui_cargar(base):  

    myProcess = [pyg.pyg_agentes(),pyg.pyg_ajustes(),pyg.pyg_CeCo()]

    return myProcess[base]