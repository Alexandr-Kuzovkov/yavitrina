from flask import Flask
from flask import make_response
from flask import render_template
from flask import request, session, redirect, url_for
from flask import send_from_directory
from flask import send_file
from myconfig import config as config, config_file
import re
import json
import os
import pg
import run as cli_module
import cronjobs
import time
import uuid

app = Flask(__name__, static_url_path='/static')
# set the secret key.  keep this really secret:
app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'


def get_nodes():
    return cli_module.get_nodes()

def prepare_log(log):
    return log.replace("\n", '<br/>')

@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)

@app.route("/")
def index_page():
    if 'username' not in session:
        return redirect('login')
    nodes = get_nodes()
    masters = nodes['masters']
    slaves = nodes['slaves']
    fails = nodes['fails']
    return render_template('index.html', masters=masters, slaves=slaves, fails=fails)

@app.route("/stats")
def slaves_page():
    if 'username' not in session:
        return redirect('login')
    nodes = get_nodes()
    masters = None
    slaves = None
    fld_lst = 'pid,usename,application_name,client_addr,client_hostname,client_port,backend_start,state,sync_state'
    if len(nodes['masters']) > 0:
        masters = nodes['masters']
        for master in masters:
            dbname = config.get('database', 'dbname')
            dbconf = {'dbhost': master['ip'], 'dbport': int(master['dbport']), 'dbuser': master['dbuser'], 'dbpass': master['dbpass'], 'dbname': dbname}
            db = pg.PgSQLStore(dbconf)
            slaves = db._getraw('SELECT %s from pg_stat_replication' % fld_lst, fld_lst.split(','), None, True)
            master['slaves'] = slaves
    return render_template('stats.html', masters=masters, fields=fld_lst.split(','))

@app.route("/log")
def logs_page():
    if 'username' not in session:
        return redirect('login')
    return render_template('logs.html')

@app.route("/cronlog")
def logs_page_cron():
    if 'username' not in session:
        return redirect('login')
    return render_template('logs-cron.html')


@app.route("/help")
def help_page():
    if 'username' not in session:
        return redirect('login')
    content = open('/app/README.txt', 'r').read()
    return render_template('help.html', content=content)

@app.route("/get-log")
def log_page():
    command = "tail -100 /app/logs/log.txt"
    output = os.popen(command).read()
    return make_response(prepare_log(output))

@app.route("/get-log-cron")
def log_page_cron():
    command = "tail -100 /app/logs/cronlog.txt"
    output = os.popen(command).read()
    return make_response(prepare_log(output))

@app.route("/config", methods=['GET', 'POST'])
def config_page():
    if 'username' not in session:
        return redirect('login')
    if request.method == 'POST':
        content = request.form['conf_content']
        f = open('/app/config.ini', 'w')
        f.write(content)
        f.close()
    content = open('/app/config.ini', 'r').read()
    return render_template('config.html', content=content)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'username' in session:
        return redirect(url_for('index_page'))
    error = None
    config.read(config_file)
    username = config.get('auth', 'username')
    password = config.get('auth', 'password')
    if request.method == 'POST':
        if request.form['login'] == username and request.form['pass'] == password:
            session['username'] = request.form['login']
            return redirect(url_for('index_page'))
        else:
            error = 'Invalid login or password!'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    # remove the username from the session if it's there
    session.pop('username', None)
    return redirect(url_for('index_page'))

@app.route('/jobs')
def jobs():
    # show cron jobs list
    if 'username' not in session:
        return redirect('login')
    error = None
    job_list = cronjobs.get_job_list()
    return render_template('jobs.html', jobs=job_list, error=error)

@app.route('/jobsedit/<slug>', methods=['GET', 'POST'])
def jobedit(slug):
    # edit job
    if 'username' not in session:
        return redirect('login')
    error = None
    if request.method == 'POST':
        formdata = {'enabled': 0}
        fld_list = cronjobs.get_jobs_fld_list()
        for field in fld_list:
            if field in request.form:
                formdata[field] = request.form[field]
        cronjobs.update_job(slug, formdata)
        return redirect(url_for('jobs'))
    jobs = cronjobs.get_jobs(slug)
    if len(jobs) > 0:
        job = jobs[0]
        return render_template('jobedit.html', job=job, error=error)
    return redirect(url_for('jobs'))

@app.route('/jobsdelete/<slug>')
def jobdelete(slug):
    # delete job
    if 'username' not in session:
        return redirect('login')
    error = None
    jobs = cronjobs.get_jobs(slug)
    if len(jobs) > 0:
        job = jobs[0]
        cronjobs.delete_job(job['slug'])
    return redirect(url_for('jobs'))

@app.route('/newjob', methods=['GET', 'POST'])
def newjob():
    # create new job
    if 'username' not in session:
        return redirect('login')
    error = None
    if request.method == 'POST':
        formdata = {'enabled': 0}
        fld_list = cronjobs.get_jobs_fld_list()
        print(fld_list)
        for field in fld_list:
            if field in request.form:
                formdata[field] = request.form[field]
        print(formdata)
        cronjobs.create_job(formdata)
        return redirect(url_for('jobs'))
    job = {'slug': str(uuid.uuid1()), 'max_execute_time': 300, 'enabled': 1, 'description': 'Job description', 'm': '*', 'h': '*', 'dom': '*', 'mon': '*', 'dow': '*'}
    return render_template('newjob.html', job=job, error=error)

@app.route('/jobping/<slug>')
def jobping(slug):
    jobs = cronjobs.get_jobs(slug)
    if len(jobs) > 0:
        job = jobs[0]
        res = cronjobs.pingjob(job)
        if res:
            return make_response(json.dumps({'result': 'OK'}), 200)
        else:
            return make_response(json.dumps({'error': 'Server error'}), 500)
    else:
        response = make_response(json.dumps({'error': 'Not found'}), 404)
        return response

@app.route('/diedjobs')
def diedjobs():
    # show cron jobs list
    if 'username' not in session:
        return redirect('login')
    error = None
    job_list = cronjobs.get_died_jobs()
    return render_template('jobs.html', jobs=job_list, error=error)

@app.route('/joberror/<slug>', methods=['GET', 'POST'])
def joberror(slug):
    jobs = cronjobs.get_jobs(slug)
    if len(jobs) > 0:
        job = jobs[0]
        if request.method == 'POST':
            error_message = request.form['message']
        else:
            error_message = request.args.get('message')
        if error_message is None:
            return make_response(json.dumps({'error': 'Parameter "message" is required!'}), 400)
        res = cronjobs.new_job_error(job, error_message)
        if res:
            return make_response(json.dumps({'result': 'OK'}), 200)
        else:
            return make_response(json.dumps({'error': 'Server error'}), 500)
    else:
        response = make_response(json.dumps({'error': 'Not found'}), 404)
        return response

@app.route('/joberrors')
def joberrors():
    # show job erros list
    if 'username' not in session:
        return redirect('login')
    error = None
    errors = cronjobs.get_errors()
    return render_template('errors.html', errors=errors, error_msg=error)

@app.route('/errordelete/<id>')
def errordelete(id):
    # delete error
    if 'username' not in session:
        return redirect('login')
    error_msg = None
    cronjobs.delete_error(id)
    return redirect(url_for('joberrors'))

@app.route("/help2")
def help_page2():
    if 'username' not in session:
        return redirect('login')
    content = open('/app/help.txt', 'r').read()
    return render_template('help.html', content=content)

@app.route("/getdatabase")
def getdatabase():
    if 'username' not in session:
        return redirect('login')
    path = "/data/data.sqlite"
    return send_file(path, as_attachment=True)

@app.route('/putdatabase', methods=['GET', 'POST'])
def putdatabase():
    # upload database
    if 'username' not in session:
        return redirect('login')
    error = None
    if request.method == 'POST':
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            file.save(os.path.join('/data/data.sqlite'))
        return redirect(url_for('jobs'))
    return render_template('put-database.html', error=error)

def allowed_file(filename):
    ALLOWED_EXTENSIONS = set(['sqlite'])
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


if __name__ == '__main__':
    port = 8000
    app.run(host='0.0.0.0', port=int(port), debug=False)


