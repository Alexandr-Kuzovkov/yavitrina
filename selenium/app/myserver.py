from flask import Flask
from flask import make_response
from flask import render_template
from flask import request, session, redirect, url_for
from flask import send_from_directory
from flask import send_file
from flask import make_response
from selenium import webdriver
import uuid
import os
from flask import jsonify
import time

class InvalidImage(Exception):
    status_code = 500

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

class InvalidHtml(Exception):
    status_code = 500

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

app = Flask(__name__, static_url_path='/static')

@app.errorhandler(InvalidImage)
def handle_invalid_image(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.errorhandler(InvalidHtml)
def handle_invalid_html(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/screenshot', methods=['POST'])
def make_screenshot():
    MIN_SIZE = 15360
    if request.method == 'POST':
        url = request.form['url']
        try:
            if 'minsize' in request.form:
                MIN_SIZE = int(request.form['minsize'])
        except Exception as ex:
            pass
        try:
            if 'token' in request.form:
                if '?' in url:
                    url = '{url}&token={token}'.format(url=url, token=request.form['token'])
                else:
                    url = '{url}?token={token}'.format(url=url, token=request.form['token'])
        except Exception as ex:
            pass
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('headless')
        browser = webdriver.Chrome(options=options)
        attemps = 5
        imagesize = 0
        filename = '/home/apps/{uuid}.png'.format(uuid=str(uuid.uuid1()))
        #print(url)
        while attemps > 0 and imagesize < MIN_SIZE:
            browser.get(url)
            browser.implicitly_wait(10.0)
            #filename = '/data/{uuid}.png'.format(uuid=str(uuid.uuid1()))
            time.sleep(10)
            browser.save_screenshot(filename)
            attemps -= 1
            if os.path.isfile(filename):
                imagesize = os.stat(filename).st_size
        if imagesize < MIN_SIZE:
            raise InvalidImage('Image is invalid', status_code=500)
        else:
            try:
                return send_file(filename, attachment_filename=os.path.basename(filename))
            except Exception as e:
                return str(e)
            finally:
                os.unlink(filename)
                browser.close()


@app.route('/html', methods=['GET'])
def get_html():
    MIN_SIZE = 512
    IMPLICITLY_WAIT = 3.0
    if request.method == 'GET':
        url = request.args.get('url')
        try:
           MIN_SIZE = int(request.args.get('minsize', MIN_SIZE))
           IMPLICITLY_WAIT = float(request.args.get('wait', IMPLICITLY_WAIT))
        except Exception as ex:
            pass
        print('url: {url}'.format(url=url))
        print('MIN_SIZE: {min_size}'.format(min_size=MIN_SIZE))
        print('IMPLICITLY_WAIT: {implicitly_wait}'.format(implicitly_wait=IMPLICITLY_WAIT))
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('headless')
        browser = webdriver.Chrome(options=options)
        attemps = 5
        contentsize = 0
        content = ''
        while attemps > 0 and contentsize < MIN_SIZE:
            browser.get(url)
            browser.implicitly_wait(IMPLICITLY_WAIT)
            time.sleep(IMPLICITLY_WAIT)
            content = browser.page_source.encode('utf-8')
            attemps -= 1
            contentsize = len(content)
        if contentsize < MIN_SIZE:
            raise InvalidHtml('Content is invalid', status_code=500)
        else:
            try:
                response = make_response(content, 200)
                return response
            except Exception as e:
                return str(e)
            finally:
                browser.close()

#if __name__ == '__main__':
#    port = 8000
#    app.run(host='0.0.0.0', port=int(port), debug=False)


