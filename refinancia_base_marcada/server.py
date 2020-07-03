from flask import Flask, session, render_template, request, redirect, g,  url_for 
from flask import Blueprint
from werkzeug.utils import secure_filename
import os                                                          #operating system
from google.cloud import bigquery
import socket
from flask import flash

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

"""El blueprint nos permite encapsular el proyecto, lo que quiere decir
que este va a ser desarrollado en un directorio independiente del resto 
de proyectos, por esto se deben declarar los folders static y template
para que sean encontrados durante todo el codigo por las invocaciones"""

refinancia_base_marcada_api = Blueprint('refinancia_base_marcada_api', __name__, static_folder='static',template_folder='templates') 
app = Flask(__name__)                             

"""A continuacion... Vemos la funcion que direcciona 
a la pagina principal una vez la invoquemos a traves de 
la url http://localhost:5000/refinancia_base_marcada/Inicio en ambiente de desarrollo"""

@refinancia_base_marcada_api.route('/inicio', methods = ['GET','POST'])          
def inicio():
    return render_template('main_page_refinancia.html')                       #A traves de este template el user digita usuario y contrasena y se invoca la siguiente funcion de /login

"""A continuacion... Esta funcion valida el password y el username. 
Como bajo esta metodologia se requiere de un key global, 
esta se debe de relacionar en el main.py, 
la linea de codigo es la siguiente: app.secret_key=os.urandom(24)"""

@refinancia_base_marcada_api.route('/login', methods = ['GET','POST'])            
def login():
    if request.method == 'POST':
        if request.form ['username'] != 'refinancia':
            error = 'Invalid username'
        elif request.form ['password'] != 'Julio2020':
            error = 'Invalid password'
        else:
            session['logged_in'] = True                              #Guarda la sesion porque de lo contrario el user debe estarse logueando en todo momento
            return render_template('Refinancia_Modulo.html')
    return render_template('main_page_refinancia.html')

"""A continuacion... Se muestra la funcion que permite el log_out
de manera autenticada"""

@refinancia_base_marcada_api.route('/logout')
def logout():
    session.pop('logged_in', None)
    return render_template('main_page_refinancia.html')

"""A continuacion... se hace una funcion que toma el archivo que submit entrega y 
se pega en la ruta definida"""

@refinancia_base_marcada_api.route('/redireccion_archivo_prejuridico', methods=['GET', 'POST'])
def upload_file_prejuridico():

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Base_Inicial'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
            return '.' in filename and \
                filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file'               #las variables llamadas file sin los apostrofes deben coincidir con el resto de files en cuanto a desc y los que estan dentro de los apostrofes deben coincidir con la variable name del input del html
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('refinancia_base_marcada_api.upload_file_prejuridico',
                                    filename=filename))
    return "Fichero movido exitosamente"

"""A continuacion... se hace una funcion que toma el archivo que submit entrega y 
se pega en la ruta definida"""

@refinancia_base_marcada_api.route('/redireccion_archivo_demograficos', methods=['GET', 'POST'])
def upload_file_demograficos(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Demograficos'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_demograficos'
        # check if the post request has the file part
        if 'file_demograficos' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_demograficos']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('refinancia_base_marcada_api.upload_file_demograficos',
                                    filename=filename))
    return "Fichero movido exitosamente"

"""A continuacion... se hace una funcion que toma el archivo que submit entrega y 
se pega en la ruta definida"""

@refinancia_base_marcada_api.route('/redireccion_archivo_gestion_diaria', methods=['GET', 'POST'])
def upload_file_gestion_diaria(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Gestion_Diaria'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_gestion_diaria'
        # check if the post request has the file part
        if 'file_gestion_diaria' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_gestion_diaria']
        # if user does not select file, browser also
        # submit an empty part without filename
        if  file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('refinancia_base_marcada_api.upload_file_gestion_diaria',
                                    filename=filename))
    return "Fichero movido exitosamente"

"""A continuacion... se hace una funcion que toma el archivo que submit entrega y 
se pega en la ruta definida"""

@refinancia_base_marcada_api.route('/redireccion_archivo_base_pagos', methods=['GET', 'POST'])
def upload_file_base_pagos(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Base_Pagos'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_base_pagos'
        # check if the post request has the file part
        if 'file_base_pagos' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_base_pagos']
        # if user does not select file, browser also
        # submit an empty part without filename
        if  file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('refinancia_base_marcada_api.upload_file_base_pagos',
                                    filename=filename))
    return "Fichero movido exitosamente"