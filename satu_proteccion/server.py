from flask import Flask, session, render_template, request, redirect, g,  url_for 
from flask import Blueprint
from werkzeug.utils import secure_filename
import os                                                          #operating system
from google.cloud import bigquery
import socket
import time
from flask import flash
import sys
import datetime
from google.cloud import datastore
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

satu_proteccion_api = Blueprint('satu_proteccion_api', __name__, static_folder='static',template_folder='templates') 
app = Flask(__name__)       

client = bigquery.Client()
QUERY = (
         'SELECT string_field_0, string_field_1  FROM `contento-bi.unificadas.modulo_logueo` where string_field_2 = "Proteccion" ') #WHERE ipdial_code = "intcob-unisabaneta"
query_job = client.query(QUERY)
rows = query_job.result()
data = ""

for row in rows:

    username = row.string_field_0
    password = row.string_field_1

fecha = time.strftime('%Y%m%d')
GetDate1 = time.strftime('%Y%m%d')

@satu_proteccion_api.route('/inicio', methods = ['GET','POST'])          
def inicio():
    return render_template('main_page_satu.html')                       #OK



@satu_proteccion_api.route('/login', methods = ['GET','POST'])            
def login():
    if request.method == 'POST':
        if request.form ['username'] != username:
            error = 'Invalid username'
        elif request.form ['password'] != password:
            error = 'Invalid password'
        else:
            session['logged_in'] = True                             
            return render_template('Satu_Modulo.html')


    return render_template('main_page_satu.html')            


@satu_proteccion_api.route('/logincliente', methods = ['GET','POST'])            
def logincliente():
    if request.method == 'POST':
        if request.form ['username'] != username:
            error = 'Invalid username'
        elif request.form ['password'] != password:
            error = 'Invalid password'
        else:
            session['logged_in'] = True                             
            return render_template('Satu_Modulo.html')

            
    return render_template('main_page_satu.html') 


@satu_proteccion_api.route('/logout')
def logout():
    session.pop('logged_in', None)
    return render_template('main_page_satu.html')

@satu_proteccion_api.route('/actualizar')
def actualizar():    
    return render_template('Proteccion_Actualizador.html')


@satu_proteccion_api.route('/regresar')
def regresar():    
    return render_template('Satu_Modulo.html')

############################################################################################################

@satu_proteccion_api.route('/redireccion_archivo_wufoo', methods=['GET', 'POST'])
def upload_file_wufoo():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Satu/Wufoo'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}    
    
    

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
            return redirect(url_for('satu_proteccion_api.upload_file_wufoo',
                                    filename= '20200920'))

                                    
    return "Fichero movido exitosamente"
    

@satu_proteccion_api.route('/redireccion_archivo_wufooc', methods=['GET', 'POST'])
def upload_file_wufooc():     

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Satu/Wufoo'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}


    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_wufoo'
        # check if the post request has the file part
        if 'file_wufoo' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_wufoo']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200920' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('satu_proteccion_api.upload_file_wufooc',
                                    filename='20200920'))
    return "Fichero movido exitosamente"


#######################################################################################################

############################################################################################################

@satu_proteccion_api.route('/redireccion_archivo_sales', methods=['GET', 'POST'])
def upload_file_sales():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Satu/salesforce'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}    
    
    

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
            return redirect(url_for('satu_proteccion_api.upload_file_wufoo',
                                    filename= GetDate1))

                                    
    return "Fichero movido exitosamente"
    

@satu_proteccion_api.route('/redireccion_archivo_salesc', methods=['GET', 'POST'])
def upload_file_salesc():     

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Satu/salesforce'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}


    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_sales'
        # check if the post request has the file part
        if 'file_sales' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_sales']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = GetDate1 + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('satu_proteccion_api.upload_file_salesc',
                                    filename=GetDate1))
    return "Fichero movido exitosamente"


#######################################################################################################


############################################################################################################

@satu_proteccion_api.route('/redireccion_archivo_asesores', methods=['GET', 'POST'])
def upload_file_asesores():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Satu/Asesores'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}    
    
    

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
            return redirect(url_for('satu_proteccion_api.upload_file_wufoo',
                                    filename= '20200801'))

                                    
    return "Fichero movido exitosamente"
    

@satu_proteccion_api.route('/redireccion_archivo_asesoresc', methods=['GET', 'POST'])
def upload_file_asesoresc():     

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Satu/Asesores'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}


    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_asesores'
        # check if the post request has the file part
        if 'file_asesores' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_asesores']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200801' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('satu_proteccion_api.upload_file_asesoresc',
                                    filename='20200801'))
    return "Fichero movido exitosamente"


#######################################################################################################
############################################################################################################

@satu_proteccion_api.route('/redireccion_archivo_gestion', methods=['GET', 'POST'])
def upload_file_gestion():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Satu/Gestion'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}    
    
    

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
            return redirect(url_for('satu_proteccion_api.upload_file_wufoo',
                                    filename= GetDate1))

                                    
    return "Fichero movido exitosamente"
    

@satu_proteccion_api.route('/redireccion_archivo_gestionc', methods=['GET', 'POST'])
def upload_file_gestionc():     

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Satu/Gestion'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}


    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_gestion'
        # check if the post request has the file part
        if 'file_gestion' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_gestion']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = GetDate1 + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('satu_proteccion_api.upload_file_gestionc',
                                    filename=GetDate1))
    return "Fichero movido exitosamente"


#######################################################################################################
