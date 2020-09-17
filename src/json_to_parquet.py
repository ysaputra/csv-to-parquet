import traceback
import uuid

import pandas as pd
import json

import webhdfspy
from confluent_kafka import Consumer
import logging
import os

module_name = "parquet-generator"
bootstrap_servers = "beam.tritronik.com:9092"
subscribed_topic = "dpiRaw"
group_id = "dpiRawConsumer"
logging.basicConfig(filename=module_name + '.log', format='%(asctime)s %(message)s', level=logging.INFO)
recordCount = os.getenv('recordCount', '100000')
endFlag = int(recordCount)


# convert json file to parquet
def convert_to_parquet(pJson):
    with open(pJson, 'r') as f:
        data = json.loads(f.read())

    f.close()
    df = pd.json_normalize(data)
    parqId = uuid.uuid1()
    parqHexVal = parqId.hex
    pathParquet = "../output/jsonParquet-" + parqHexVal + ".parquet"
    df.to_parquet(path=pathParquet, compression='GZIP')
    print(pd.read_parquet(pathParquet))
    try:
        webHDFS = webhdfspy.WebHDFSClient("mercury.tritronik.com", 50070, "hdfs")
        pathHdfs = '/dpi-aggregate/' + parqHexVal + ".parquet"
        webHDFS.copyfromlocal(pathParquet, pathHdfs)
        os.remove(pJson)
        os.remove(pathParquet)
    except:
        logging.error(traceback)


# Main Program
c = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
})

c.subscribe([subscribed_topic])
while True:
    jsonId = uuid.uuid1()
    jsonHexVal = jsonId.hex
    pathJson = "../data/json-" + jsonHexVal + ".json"
    file = open(pathJson, 'w+')
    counter = 0
    while counter < endFlag:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error("Consumer error: {}".format(msg.error()))
            continue
        try:
            logging.info('Received message: {}'.format(msg.value().decode('utf-8')))
            if counter < 1:
                file.write("[ \n" + msg.value().decode('utf-8') + ", \n")
            elif counter == endFlag - 1:
                file.write(msg.value().decode('utf-8') + "\n]")
            elif 0 < counter < endFlag and counter != endFlag - 1:
                file.write(msg.value().decode('utf-8') + ", \n")
        except:
            logging.error(traceback.print_exc())
        counter += 1
    file.close()
    convert_to_parquet(pathJson)
c.close()
