from pika.adapters import BlockingConnection
from pika import BasicProperties
import datetime 
import pika.exceptions
import pika
import time
import os.path

class PublisherClient:
    def __init__(self):
        self.username = 'guest'
        self.password = 'guest'
        self.host = 'localhost'
        self.port = 15672
        self.virtualhost='/'
        self.ssl = True
        self.connection = None
        self.channel = None
        
    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        try:
            if self.connection.is_open:
                if os.path.exists("Rabbitmq_publisher_log.txt") == True:
                    file = open("Rabbitmq_publisher_log.txt", "a")
                    file.write('Connection established time %s.\n' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                else:
                    file = open("Rabbitmq_publisher_log.txt", "w")
                    file.write('Connection established time %s.\n' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                self.publish()    
        except pika.exceptions.ConnectionClosed as conn_msg:
            pass
            print("in exception block")
            if conn_msg == conn_msg:
                if os.path.exists("Rabbitmq_publisher_log.txt") == True:
                    file = open("Rabbitmq_publisher_log.txt", "a")
                    file.write('Reason is %s.\n Connection lost time %s.\n' %(conn_msg, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                else:
                    file = open("Rabbitmq_publisher_log.txt", "w")
                    file.write('Reason is %s.\n Connection lost time %s.\n' %(conn_msg, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                #self.exit_connection()    
                time.sleep(10)
                if os.path.exists("Rabbitmq_publisher_log.txt") == True:
                    file = open("Rabbitmq_publisher_log.txt", "a")
                    file.write('Connection re-established time %s.\n' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                else:
                    file = open("Rabbitmq_publisher_log.txt", "w")
                    file.write('Connection re-established time %s.\n' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    file.close()
                pub_clnt.connect()    
        finally:
            file.close()        

    def publish(self):
        self.channel = self.connection.channel()
        self.queue = None                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
        self.channel.queue_declare(queue='message_queue')
        while True:
            try:
                self.queue == self.channel.queue_declare(queue='message_queue',passive=True)
            except pika.exceptions.ChannelClosed as chnl_clsd:
                if chnl_clsd == chnl_clsd:
                    if os.path.exists("Rabbitmq_publisher_log.txt") == True:
                        file = open("Rabbitmq_publisher_log.txt", "a")
                        file.write('No queue %s.\n Queue lost time %s.\n' %(chnl_clsd, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        file.close()
                    else:
                        file = open("Rabbitmq_publisher_log.txt", "w")
                        file.write('No queue %s.\n Queue lost time %s.\n' %(chnl_clsd, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        file.close()
                    time.sleep(10)
                    if os.path.exists("Rabbitmq_publisher_log.txt") == True:
                        file = open("Rabbitmq_publisher_log.txt", "a")
                        file.write('Queue re-established time %s.\n' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        file.close()
                    else:
                        file = open("Rabbitmq_publisher_log.txt", "w")
                        file.write('Queue re-established time %s.\n' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        file.close()
                    pub_clnt.publish()
            else:   
                message = "%s" % "Rabbitmq Program"
                message1 = "%s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                space = " "
                messages = message +  space +  message1
                self.channel.basic_publish(exchange='',routing_key='message_queue',body=messages,properties=pika.BasicProperties(delivery_mode=2))
                print(" [x] Sent message %r" % messages)
                if os.path.exists("Rabbitmq_publisher_log.txt") == True:
                    file = open("Rabbitmq_publisher_log.txt", "a")
                    file.write('[x] sent:{}.\n'.format(messages))
                    file.close()
                else:
                    file = open("Rabbitmq_publisher_log.txt", "w")
                    file.write('[x] sent:{}.\n'.format(messages))
                    file.close()
                time.sleep(60)
            finally:
                file.close()    

    def exit_connection(self):
        self.connection.close()

def main():
    global pub_clnt
    pub_clnt = PublisherClient()
    pub_clnt.connect()

if __name__ == '__main__':main()
    
