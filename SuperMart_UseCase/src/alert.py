import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from datetime import datetime, timedelta
import time
from flask import Flask,request
from flask_restful import Resource, Api
import multiprocessing

app = Flask(__name__)
api = Api(app)


class alert(threading.Thread):
    
    daemon = True

    def __init__(self, topic):
        threading.Thread.__init__(self)
        self.kafkaTopic   = topic
        self.db = self.get_db()
            
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='latest',                                 
                                 enable_auto_commit=False,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe([self.kafkaTopic])
        
        print("Displaying Request")
        
        resp = {}

        for message in consumer:
            msg = message.value
            
            if self.kafkaTopic == 'topic_shoppingCart':
                 
                if 'product' in msg:
                    products = msg['product']

                if 'userId' in msg:
                    userId = msg['userId']
                
                for product in products:
                    productId = product['productId']
                    duplicate = self.fetch_shoppingcard(self.db,userId,2,productId)
                    if duplicate :
                        print("Duplicate record found in shopping cart")
                        resp['dup_productId'].append(productId)

                if 'dup_productId' in resp:
                   resp['dup_msg'] = "Duplicate record found in shopping cart"

                self.add_shoppingCart(self.db,msg)
            
            if self.kafkaTopic== 'topic_orderInfo':
                
                if 'product' in msg:
                    products = msg['product']

                if 'userId' in msg:
                    userId = msg['userId']

                for product in products:
                    productId = product['productId']
                    duplicate = self.fetch_orderInfo(self.db,userId,productId)
                    if duplicate :
                        print("Duplicate record found in order Info")
                        resp['dup_productId'].append(productId)
                
                if 'dup_productId' in resp:
                   resp['dup_msg'] = "Duplicate record found in shopping cart"

                self.add_orderDetails(self.db,msg)     
           


    def get_db(self):
        client = MongoClient('localhost:27017')
        db = client.superMart
        return db
    
    def add_orderDetails(self,db,data):
        db.orderDetails.insert(data)
    
    def add_shoppingCart(self, db,data):
        db.shoppingCart.insert(data)

    # def fetch_shoppingcard(self,db,userId,interval,productid):
    #     dt = datetime.now() - timedelta(hours = interval)
    #     tms = time.mktime(dt.timetuple())    
    #     sql = {"userInfo.userId":  userId,"product.productId" : productid,
    #           "$or":[{"modifiedOn": {"$lte":tms}}]}
    #     print(sql)
    #     count = db.shoppingCart.find(sql).count()
    #     return count

    def fetch_shoppingcard(self,db,userId,interval,productid):
        dt = datetime.now() - timedelta(hours = interval)
        tms = time.mktime(dt.timetuple())    
        sql = {"userInfo.userId":  userId,
              "$or":[{"modifiedOn": {"$lte":tms}}]}
        duplicate = False
        rows = db.shoppingCart.find(sql).count()
        for row in rows:
            products = row["product"]
            for product in products:
                if product["productId"] == productid:
                    duplicate = True
        return duplicate

    def fetch_orderInfo(self,db,userId,productid):
        sql = {"userInfo.userId":  userId}
        print(sql)
        rows = db.orderDetails.find(sql).sort('$modifiedOn', -1).limit(5)
        duplicate = False
        for row in rows:
            products = row["product"]
            for product in products:
                if product["productId"] == productid:
                    duplicate = True
        return duplicate

@app.route('/alert', methods = ['POST'])

def api_message():
    
    if (request.headers['Content-Type'] == 'application/json'):
        content={}
        content=request.get_json()
        
        if "eventName" in content:
            
            if "shoppingCart" == content["eventName"]:
               
                topic = "topic_shoppingCart"
                threads = [alert(topic)]
                for t in threads:
                    t.start()
                time.sleep(10)
            if "orderInfo" == content["eventName"]:
                topic = "topic_orderInfo"
                threads = [alert(topic)]
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
