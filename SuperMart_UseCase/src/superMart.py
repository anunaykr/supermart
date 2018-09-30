
import requests
from flask import json
import datetime, time
import threading, logging
import multiprocessing
from flask import Flask,request
from flask_restful import Resource, Api
from kafka import KafkaConsumer, KafkaProducer

app = Flask(__name__)
api = Api(app)

class superMart(threading.Thread):

    def __init__(self, topic, data):
        threading.Thread.__init__(self)
        self.output   = "json"
        self.data     = data
        dt = datetime.datetime.now()
        self.unixtime = time.mktime(dt.timetuple())       
        self.stop_event = threading.Event()
        self.topicName = topic
        print("Current Unix Timestamp is {0}:".format(self.unixtime))


    def shoppingCart(self):
        print("Inside ShoppingCart Function")
        resp = {}
        try:
            data = self.data
            print(data)
            data["eventName"] = 'shoppingCart'
            resp["error"]     = 0
            resp              = json.dumps(data)         
       
        except ValueError as error:

            print("Invalid cartdetails Provided : ", error)
            resp["error"]     = 1
            resp["message"]   = 'Invalid cartdetails Provided'
            resp["eventName"] = 'shoppingCart'
        
        return resp

    def orderInfo(self):
        
        resp = {}
        try:
            data = json.dumps(self.data)
            data["eventName"] = 'orderDetails'
            resp              = data
            resp["error"]     = 0
       
        except ValueError as error:

            print("Invalid orderDetails Provided : ", error)
            resp["error"]     = 1
            resp["message"]   = 'Invalid orderDetails Provided'
            resp["eventName"] = 'orderDetails'
        
        return resp

    def stop(self):
        self.stop_event.set()

    def run(self):

        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        print("inside superMart. run()")
        while not self.stop_event.is_set():
            if self.topicName == "shoppingCart":
                data = self.shoppingCart()
                print(data)
                print(self.topicName )
                producer.send(self.topicName ,json.dumps(data).encode('utf-8'))
            if self.topicName == "orderDetails":
                producer.send(self.topicName ,self.orderInfo())
            time.sleep(1)
        producer.close()

@app.route('/messages', methods = ['POST'])
def api_message():
    
    if (request.headers['Content-Type'] == 'application/json'):
        content={}
        content=request.get_json()
    
        if "eventName" in content:
    
            if "shoppingCart" == content["eventName"]:
               
                data  = content["data"]
                topic = "topic_shoppingCart"
                threads = [superMart(topic,data)]
                for t in threads:
                    t.start()
                time.sleep(10)
            if "orderInfo" == content["eventName"]:
                data  = content["data"]
                topic = "topic_orderInfo"
                threads = [superMart(topic,data)]
                for t in threads:
                    t.start()
                time.sleep(10)
             
    resp = content
    resp["status"]   = "sucessful"
    return("JSON Message: " + json.dumps(resp))
    
    

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    app.run(debug=True)



