from django.shortcuts import render
from confluent_kafka import Producer,Consumer, KafkaError
from django.http import HttpResponse
import json
import random
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
    response = json.dumps([{}])
    return HttpResponse(response, content_type='text/json')
    # print("Success reach")
    # response = json.dumps([{}])
    # if len(ProducerList)>=1:
    #      return HttpResponse(response,content_type='text/json')
    # ProducerList.append(Producer({'bootstrap.servers': '127.0.0.1:9092'}))
    # return HttpResponse(response,content_type='text/json')


# def createConsumer(request):
#     if len(ConsumerList)>=1:
#          return "1+ consumer"
#     consumer=Consumer({'bootstrap.servers': '127.0.0.1:9092','group.id': 'foo'})
#     consumer.subscribe(['mytopic'])
#     ConsumerList.append(consumer)
#     return "Success"

# def pushData(request):
#     if len(ProducerList)==0:
#         return "No Producer"
#     else:
#         print("Producer Count"+str(len(ProducerList)))
#     data=generate_data()
#     ind=random.randint(0,len(ProducerList)-1)
#     #print(ind)
#     producer=ProducerList[ind]
#     producer.poll(0)
#     producer.produce("mytopic",data.encode('utf-8'),callback=delivery_report)
#     return "Success"



# def pullData(request):
#     if len(ConsumerList)==0:
#         return "No Consumer"
#     else:
#         print("Consumer Count"+str(len(ConsumerList)))
#     ind=random.randint(0,len(ConsumerList)-1)
#     #print(ind)
#     consumer=ConsumerList[ind]
#     check=1
#     while check:
#             msg=consumer.poll(1.0)
#             if msg is None:
#                 continue
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     continue
#                 else:
#                     print(msg.error())
#                     break
#             print('Received message: {}'.format(msg.value().decode('utf-8')))
#             check-=1
#     return "Success"
  
# @app.route('/killAllProducerConsumer')
# def killAllProcuctConsumer():
#     for producer in ProducerList:
#         producer.flush()
#     for consumer in ConsumerList:
#         consumer.close()
#     return "Success"
  
  