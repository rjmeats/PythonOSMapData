# Basic flask plotting trial
# http://localhost:5000/plotpostcode/NG2%206AG
# http://localhost:5000/plotarea/DN
# http://localhost:5000/plotgridsquare/TQ
# http://localhost:5000/plotallGB

from flask import Flask, request

app = Flask(__name__)

@app.route('/')
def index():
    user_agent = request.headers.get('User-Agent')
    return f'<p>Your browser is {user_agent}</p>'

import io
import base64
import postcodes as pc
from cv2 import cv2
from flask import jsonify, make_response

@app.route('/plotpostcode/<postcode>')
def plotpostcode(postcode):

    df = pc.readCachedDataFrame()
    img = pc.plotPostcode(df, postcode, displayPlot=False)

    ret, jpeg = cv2.imencode('.jpg', img)
    response = make_response(jpeg.tobytes())
    response.headers['Content-Type'] = 'image/png'
    return response

@app.route('/plotarea/<postcodearea>')
def plotarea(postcodearea):

    df = pc.readCachedDataFrame()
    img = pc.plotPostcodeArea(df, postcodearea, displayPlot=False)

    ret, jpeg = cv2.imencode('.jpg', img)
    response = make_response(jpeg.tobytes())
    response.headers['Content-Type'] = 'image/png'
    return response

@app.route('/plotgridsquare/<gridsquare>')
def plotgridsquare(gridsquare):

    df = pc.readCachedDataFrame()
    img = pc.plotGridSquare(df, gridsquare, displayPlot=False)

    ret, jpeg = cv2.imencode('.jpg', img)
    response = make_response(jpeg.tobytes())
    response.headers['Content-Type'] = 'image/png'
    return response

@app.route('/plotallGB')
def plotallGB():

    df = pc.readCachedDataFrame()
    img = pc.plotAllGB(df, displayPlot=False)

    ret, jpeg = cv2.imencode('.jpg', img)
    response = make_response(jpeg.tobytes())
    response.headers['Content-Type'] = 'image/png'
    return response

@app.route('/request/<x>')
def req(x):
    s = ''
    s += '<h2>headers</h2>'
    s += f'{request.headers}'
    s += '<h2>args</h2>'
    s += f'{request.args}'
    s += '<h2>values</h2>'
    s += f'{request.values}'
    s += '<h2>cookies</h2>'
    s += f'{request.cookies}'
    s += '<h2>form</h2>'
    s += f'{request.form}'
    s += '<h2>endpoint</h2>'
    s += f'{request.endpoint}'
    s += '<h2>method</h2>'
    s += f'{request.method}'
    s += '<h2>scheme</h2>'
    s += f'{request.scheme}'
    s += '<h2>is_secure</h2>'
    s += f'{request.is_secure}'
    s += '<h2>host</h2>'
    s += f'{request.host}'
    s += '<h2>path</h2>'
    s += f'{request.path}'
    s += '<h2>query_string</h2>'
    s += f'{request.query_string}'
    s += '<h2>full_path</h2>'
    s += f'{request.full_path}'
    s += '<h2>url</h2>'
    s += f'{request.url}'
    s += '<h2>base_url</h2>'
    s += f'{request.base_url}'
    s += '<h2>remote_addr</h2>'
    s += f'{request.remote_addr}'
    s += '<h2>environ</h2>'
    s += f'{request.environ}'

    return s
    