from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
import sys

conn_url = "amq-kafka-cluster-kafka-bootstrap.openshift-operators.svc.cluster.local:9092"
address  = "my-topic"

class SendHandler(MessagingHandler):
    def __init__(self, conn_url, address, message_body):
        super(SendHandler, self).__init__()

        self.conn_url = conn_url
        self.address = address
        self.message_body = message_body

    def on_start(self, event):
        conn = event.container.connect(self.conn_url)

        # To connect with a user and password:
        # conn = event.container.connect(self.conn_url, user="<user>", password="<password>")

        event.container.create_sender(conn, self.address)

    def on_link_opened(self, event):
        print("SEND: Opened sender for target address '{0}'".format
              (event.sender.target.address))

    def on_sendable(self, event):
        message = Message(self.message_body)
        event.sender.send(message)

        print("SEND: Sent message '{0}'".format(message.body))

        event.sender.close()
        event.connection.close()

def publishMsg(msg):
    print("=== in publish2kafka() publishMsg ===")
    message_body = msg
    handler = SendHandler(conn_url, address, message_body)
    print("=== in publish2kafka() handler created ===")
    container = Container(handler)
    container.run()
