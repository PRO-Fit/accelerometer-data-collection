from cassandra.query import BatchStatement
from flask import Flask
from flask import request
from flask import Response
from flask import json
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel, OperationTimedOut

import time
import sys
import logging

app = Flask(__name__)
actor = None
host = None
port = None
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

def db():
    cluster = Cluster()
    session = cluster.connect('accelerometer')
    return session


@app.route('/')
def hello_world():
    return 'Hello World!'


'''
@app.route('/api/test', methods=['POST'])
def receive_data():
    data = request.get_json()
    user_id = data['user_id']
    name = data['name']
    session.execute("""insert into users (user_id, name) values ( %(uid)s, %(name)s )""",
                    {'uid': data['user_id'], 'name': data['name']})
    resp = jsonify(data)
    # print resp
    resp.status_code = 200
    return resp
'''


@app.route('/api/insert/batch', methods=['POST'])
def receive_data():
    users_to_insert = request.get_json()

    insert_user = db().prepare("INSERT INTO user_accel_data_test (user_id, timestamp, x, y, z) VALUES (?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)

    for i in users_to_insert:
        batch.add(insert_user, (i['user_id'], i['timestamp'], i['x'], i['y'], i['z']))

    db().execute(batch)

    return Response(json.dumps(users_to_insert),  mimetype='application/json')


if __name__ == '__main__':
    app.debug = True
    host = sys.argv[1]
    port = int(sys.argv[2])
    app.run(host=host, port=port)