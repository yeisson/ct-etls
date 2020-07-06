from flask import Flask, session, render_template, request, redirect, g,  url_for 
from flask import Blueprint
from werkzeug.utils import secure_filename
import os                                                          #operating system
from google.cloud import bigquery
import socket

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

"""El blueprint nos permite encapsular el proyecto, lo que quiere decir
que este va a ser desarrollado en un directorio independiente del resto 
de proyectos, por esto se deben declarar los folders static y template
para que sean encontrados durante todo el codigo por las invocaciones"""

metlife_base_marcada_api = Blueprint('metlife_base_marcada_api', __name__, static_folder='static',template_folder='templates') 
app = Flask(__name__)                             

"""A continuacion... Vemos la funcion que direcciona 
a la pagina principal una vez la invoquemos a traves de 
la url http://localhost:5000/metlife/Inicio en ambiente de desarrollo"""

@metlife_base_marcada_api.route('/Inicio', methods = ['GET','POST'])          
def inicio():
    return render_template('Main_Page.html')                       #A traves de este template el user digita usuario y contrasena y se invoca la siguiente funcion de /login

"""A continuacion... Esta funcion valida el password y el username. 
Como bajo esta metodologia se requiere de un key global, 
esta se debe de relacionar en el main.py, 
la linea de codigo es la siguiente: app.secret_key=os.urandom(24)"""

@metlife_base_marcada_api.route('/login', methods = ['GET','POST'])            
def login():
    if request.method == 'POST':
        if request.form ['username'] != 'metlife':
            error = 'Invalid username'
        elif request.form ['password'] != 'Junio2020':
            error = 'Invalid password'
        else:
            session['logged_in'] = True                              #Guarda la sesion porque de lo contrario el user debe estarse logueando en todo momento
            return render_template('Metlife_Modulo.html')
    return render_template('Main_Page.html')

"""A continuacion... Se muestra la funcion que permite el log_out
de manera autenticada"""

@metlife_base_marcada_api.route('/logout')
def logout():
    session.pop('logged_in', None)
    return render_template('Main_Page.html')

"""A continuacion... se hace el cargue del archivo base al storage, sin embargo
primero se deben de declarar la variable UPLOAD_FOLDER"""

UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Metlife/Base'
ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
app.config['ruta_completa'] = ruta_completa
ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

"""A continuacion... se hace una definicion de allowed_file para conservar
el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

"""A continuacion... se hace una funcion que toma el archivo que submit entrega y 
se pega en la ruta definida"""

@metlife_base_marcada_api.route('/redireccion_archivo_prejuridico', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = 'file'
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
            return redirect(url_for('metlife_base_marcada_api.upload_file',
                                    filename=filename))
    return "Fichero movido exitosamente"

    




