from django.shortcuts import render
from confluent_kafka import Producer,Consumer, KafkaError
from django.http import HttpResponse
import random
import json
# Create your views here.
ProducerList=[]
ConsumerList=[]

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def generate_data(length=6):
           return 'data'


def createProducer(request):
    resp=json.dumps([{}]) 
    if len(ProducerList)>=1:
        resp=json.dumps([{"Message":"1+producer"}])
    else:
        resp=json.dumps([{"Message":"Producer is created"}])
        ProducerList.append(Producer({'bootstrap.servers': '127.0.0.1:9092'}))
    return HttpResponse(resp, content_type='text/json')



def createConsumer(request):
    resp=json.dumps([{}])
    if len(ConsumerList)>=1:
        resp=json.dumps([{"Message":"1+Consumer"}])
    else:
        resp=json.dumps([{"Message":"Consumer is created"}])
        consumer=Consumer({'bootstrap.servers': '127.0.0.1:9092','group.id': 'foo'})
        consumer.subscribe(['mytopic1'])
        ConsumerList.append(consumer)
    return HttpResponse(resp, content_type='text/json')

def pushData(request):
    if len(ProducerList)==0:
        return HttpResponse(json.dumps([{"Message":"No Producer Found"}]), content_type='text/json')
    else:
        print("Producer Count"+str(len(ProducerList)))
    data=generate_data()
    ind=random.randint(0,len(ProducerList)-1)
    #print(ind)
    producer=ProducerList[ind]
    producer.poll(0)
    producer.produce("mytopic1",data.encode('utf-8'),callback=delivery_report)
    return HttpResponse(json.dumps([{"Message":"Data Pushed to KAFKA"}]), content_type='text/json')




def pullData(request):
    if len(ConsumerList)==0:
        return HttpResponse(json.dumps([{"Message":"No Consumer Found"}]), content_type='text/json')
    else:
        print("Consumer Count"+str(len(ConsumerList)))
    ind=random.randint(0,len(ConsumerList)-1)
    consumer=ConsumerList[ind]
    check=1
    while check:
        msg=consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print('Received message: {}'.format(msg.value().decode('utf-8')))
        check-=1
    return HttpResponse(json.dumps([{"Message":"Data Pulled from KAFKA"}]), content_type='text/json')

    
  
# # @app.route('/killAllProducerConsumer')
# # def killAllProcuctConsumer():
# #     for producer in ProducerList:
# #         producer.flush()
# #     for consumer in ConsumerList:
# #         consumer.close()
# #     return "Success"
  
  