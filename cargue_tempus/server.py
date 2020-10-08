from flask import Flask, session, render_template, request, redirect, g,  url_for 
from flask import Blueprint
from werkzeug.utils import secure_filename
import os                                                          #operating system
from google.cloud import bigquery
import socket
from flask import flash
import sys
import datetime
from google.cloud import datastore
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

cargue_tempus_api = Blueprint('cargue_tempus_api', __name__, static_folder='static',template_folder='templates') 
app = Flask(__name__)      


client = bigquery.Client()
QUERY = (
         'SELECT string_field_0, string_field_1  FROM `contento-bi.unificadas.modulo_logueo` where string_field_2 = "Unificadas" ') #WHERE ipdial_code = "intcob-unisabaneta"
query_job = client.query(QUERY)
rows = query_job.result()
data = ""

for row in rows:

    username = row.string_field_0
    password = row.string_field_1


@cargue_tempus_api.route('/inicio', methods = ['GET','POST'])          
def inicio():
    return render_template('main_page_tempus.html')                       #OK



@cargue_tempus_api.route('/login', methods = ['GET','POST'])            
def login():
    if request.method == 'POST':
        if request.form ['username'] != username:
            error = 'Invalid username'
        elif request.form ['password'] != password:
            error = 'Invalid password'
        else:
            session['logged_in'] = True                             
            return render_template('tempus_Modulo.html')
    return render_template('main_page_tempus.html')             #OK



@cargue_tempus_api.route('/logout')
def logout():
    session.pop('logged_in', None)
    return render_template('main_page_tempus.html')

############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_prejuridico', methods=['GET', 'POST'])
def upload_file_prejuridico():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Adeinco1'
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
            return redirect(url_for('cargue_tempus_api.upload_file_prejuridico',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


@cargue_tempus_api.route('/redireccion_archivo_demograficos', methods=['GET', 'POST'])
def upload_file_demograficos(): 

    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Adeinco1'
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
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_demograficos',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################


############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_asignacion', methods=['GET', 'POST'])
def upload_file_asignacion():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Adeinco2'
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
            return redirect(url_for('cargue_tempus_api.upload_file_asignacion',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_asignacion2', methods=['GET', 'POST'])
def upload_file_asignacion2(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Adeinco2'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_asignacion'
        # check if the post request has the file part
        if 'file_asignacion' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_asignacion']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_asignacion2',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################

############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_agaval', methods=['GET', 'POST'])
def upload_file_agaval():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Agaval'
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
            return redirect(url_for('cargue_tempus_api.upload_file_agaval',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_Agavalc', methods=['GET', 'POST'])
def upload_file_Agavalc(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Agaval'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_agaval'
        # check if the post request has the file part
        if 'file_agaval' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_agaval']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_Agavalc',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################


############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_agaval2', methods=['GET', 'POST'])
def upload_file_agaval2():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Agaval2'
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
            return redirect(url_for('cargue_tempus_api.upload_file_agaval2',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_Agavalc2', methods=['GET', 'POST'])
def upload_file_Agavalc2(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Agaval2'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_agaval2'
        # check if the post request has the file part
        if 'file_agaval2' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_agaval2']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_Agavalc2',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################


############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_aval1', methods=['GET', 'POST'])
def upload_file_aval1():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Aval1'
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
            return redirect(url_for('cargue_tempus_api.upload_file_aval1',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_Avalc', methods=['GET', 'POST'])
def upload_file_Avalc(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Aval1'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_aval'
        # check if the post request has the file part
        if 'file_aval' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_aval']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_Avalc',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################

############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_aval2', methods=['GET', 'POST'])
def upload_file_aval2():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Aval2'
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
            return redirect(url_for('cargue_tempus_api.upload_file_aval2',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_Avalc2', methods=['GET', 'POST'])
def upload_file_Avalc2(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Aval2'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_aval2'
        # check if the post request has the file part
        if 'file_aval2' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_aval2']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_Avalc2',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################

############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_codigos', methods=['GET', 'POST'])
def upload_file_codigos():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Cod'
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
            return redirect(url_for('cargue_tempus_api.upload_file_codigos',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_codigosc', methods=['GET', 'POST'])
def upload_file_codigosc(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Cod'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_cod'
        # check if the post request has the file part
        if 'file_cod' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_cod']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_codigosc',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################



############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_estrategia', methods=['GET', 'POST'])
def upload_file_estrategia():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Estrategia'
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
            return redirect(url_for('cargue_tempus_api.upload_file_estrategia',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_estrategiac', methods=['GET', 'POST'])
def upload_file_estrategiac(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Estrategia'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_estrategia'
        # check if the post request has the file part
        if 'file_estrategia' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_estrategia']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_estrategiac',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################

############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_historico', methods=['GET', 'POST'])
def upload_file_historico():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Historico'
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
            return redirect(url_for('cargue_tempus_api.upload_file_historico',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_historicoc', methods=['GET', 'POST'])
def upload_file_historicoc(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Historico'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_historico'
        # check if the post request has the file part
        if 'file_historico' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_historico']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_historicoc',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################

############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_telefonos', methods=['GET', 'POST'])
def upload_file_telefonos():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Lineatel'
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
            return redirect(url_for('cargue_tempus_api.upload_file_telefonos',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_telefonosc', methods=['GET', 'POST'])
def upload_file_telefonosc(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Lineatel'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_telefonos'
        # check if the post request has the file part
        if 'file_telefonos' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_telefonos']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_telefonosc',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################

############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_prejul', methods=['GET', 'POST'])
def upload_file_prejul():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Prejul'
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
            return redirect(url_for('cargue_tempus_api.upload_file_prejul',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_prejulc', methods=['GET', 'POST'])
def upload_file_prejulc(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Prejul'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_prejul'
        # check if the post request has the file part
        if 'file_prejul' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_prejul']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_prejulc',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################


############################################################################################################

@cargue_tempus_api.route('/redireccion_archivo_prejurleo', methods=['GET', 'POST'])
def upload_file_prejurleo():    

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Prejuridico'
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
            return redirect(url_for('cargue_tempus_api.upload_file_prejurleo',
                                    filename='20200924'))
    return "Fichero movido exitosamente"



@cargue_tempus_api.route('/redireccion_archivo_prejuleoc', methods=['GET', 'POST'])
def upload_file_prejuleoc(): 

    """A continuacion... se hace el cargue del archivo base al storage, sin embargo
    primero se deben de declarar la variable UPLOAD_FOLDER"""

    UPLOAD_FOLDER = '/BI_Archivos/GOOGLE/Tempus/Prejuridico'
    ruta_completa = fileserver_baseroute + UPLOAD_FOLDER
    app.config['ruta_completa'] = ruta_completa
    ALLOWED_EXTENSIONS = {'csv','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

    """A continuacion... se hace una definicion de allowed_file para conservar
    el nombre y la extension del archivo que se va a tomar y a pegar en la ruta definida"""

    def allowed_file(filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    if request.method == 'POST':
        file = 'file_prejuleo'
        # check if the post request has the file part
        if 'file_prejuleo' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file_prejuleo']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if  file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename = '20200924' + '.csv'
            file.save(os.path.join(app.config['ruta_completa'], filename))  #La ruta que se pega aca debe incluirse en un diccionario previamente: ver app.config['ruta_completa'] = ruta_completa
            return redirect(url_for('cargue_tempus_api.upload_file_prejuleoc',
                                    filename='20200924'))
    return "Fichero movido exitosamente"


#######################################################################################################
