from pika.adapters import BlockingConnection
import datetime
import pika
import pika.exceptions
import time
import os.path

class ConsumerServer:
    def __init__(self):
        self.usernme = 'guest'
        self.password = 'guest'
        self.host= 'localhost'
        self.port = 15672
        self.virtualhost ='/'
        self.ssl = True
        self.connection = None
        self.channel = None
        self.queue = None
        
    def connect(self):  
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        try:
            if self.connection.is_open:
                if os.path.exists("Rabbitmq_consumer_log.txt") == True:
                    file = open("Rabbitmq_consumer_log.txt", "a")
                    file.write('Connection established time %s.\n' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                else:
                    file = open("Rabbitmq_consumer_log.txt", "w")
                    file.write('Connection established time %s.\n' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                self.create_channel()
        except pika.exceptions.ConnectionClosed as Conn_msg:
            if conn_msg == conn_msg:
                if os.path.exists("Rabbitmq_consumer_log.txt") == True:
                    file = open("Rabbitmq_consumer_log.txt", "a")
                    file.write('Reason is %s.\n Connection lost time %s.\n' %(conn_msg, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                else:
                    file = open("Rabbitmq_consumer_log.txt", "w")
                    file.write('Reason is %s.\n Connection lost time %s.\n' %(conn_msg, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()

    def create_channel(self):     #queue_declare 207  queue_bind 224
        self.channel = self.connection.channel()
        self.create_queue()

    def create_queue(self):
        self.channel.queue_declare(queue='message_queue')
        try:
            self.queue == self.channel.queue_declare(queue='message_queue', passive=True)
        except pika.exceptions.ConsumerCancelled as cc_msg:
            print(cc_msg)
            if cc_msg == cc_msg:
                if os.path.exists("Rabbitmq_consumer_log.txt") == True:
                    file = open("Rabbitmq_consumer_log.txt", "a")
                    file.write('Reason is %s.\n Consumer lost time %s.\n' %(cc_msg, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                else:
                    file = open("Rabbitmq_consumer_log.txt", "w")
                    file.write('Reason is %s.\n Consumer lost time %s.\n' %(cc_msg, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
        else:
            self.callback(channel='', method='', properties='', body='')
            
    def callback(self, channel, method, properties, body):
        self.channel.queue_declare(queue = 'message_queue')
        print("[*] waiting for messages")
        print("[x] Received %r" % body)
        if os.path.exists("Rabbitmq_consumer_log.txt") == True:
            file = open("Rabbitmq_consumer_log.txt", "a")
            file.write('[x] Received:{}.\n'.format(body))
            file.close()
        else:
            file = open("Rabbitmq_consumer_log.txt", "w")
            file.write('[x] Received:{}.\n'.format(body))
            file.close()    
        self.channel.basic_ack(method.delivery_tag)
        #self.channel.basic_ack(delivery_tag = method.delivery_tag)
        self.consume_basic()
        
    def consume_basic(self, callback):
        self.channel.basic_consume(callback,queue='message_queue')
        self.consume_start()

    def consume_start(self):
        self.channel.start_consuming()

    def exit_connection(self):
        self.connection.close() 
                                        
def main():
    global cons_server
    cons_server = ConsumerServer()
    cons_server.connect()

if __name__ == '__main__':main()
