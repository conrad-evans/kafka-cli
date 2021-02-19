from confluent_kafka import Producer, Consumer


class Kafka:
    """Kafka Message Broker Class"""

    PRODUCER_CONFIG = {}
    CONSUMER_CONFIG = {"group.id": "mygroup"}

    def __init__(self, kafka_server_port=None, start_time=None):
        """
        param1: (optional) -> string
        param2: (optional) -> string
        """
        self.producer = None
        self.consumer = None
        self.PRODUCER_CONFIG["bootstrap.servers"] = kafka_server_port
        self.CONSUMER_CONFIG["auto.offset.reset"] = start_time
        self.setDefaultConfig()
        self.CONSUMER_CONFIG = {**self.CONSUMER_CONFIG, **self.PRODUCER_CONFIG}

    def deliveryReport(self, err, msg):
        if err is not None:
            print("[ERROR] Message delivery failed: {}".format(err))
        else:
            print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    def produce(self, data, topic=None):
        self.producer = Producer(self.PRODUCER_CONFIG)

        self.producer.poll(0)
        self.producer.produce(topic, data.encode("utf-8"), callback=self.deliveryReport)

        self.producer.flush()

    def consume(self, topic=None, running=True):
        self.consumer = Consumer(self.CONSUMER_CONFIG)
        self.consumer.subscribe([self.topic])

        while running:
            data = self.consumer.poll(1.0)
            if data is None:
                continue
            if data.error():
                print("Consumer error: {}".format(data.error()))
                continue
            print("Received message: {}".format(data.value().decode("utf-8")))

    def setDefaultConfig(self):
        if self.PRODUCER_CONFIG["bootstrap.servers"] is None:
            self.PRODUCER_CONFIG["bootstrap.servers"] = "localhost:9092"
        if self.CONSUMER_CONFIG['auto.offset.reset'] is None:
            self.CONSUMER_CONFIG['auto.offset.reset'] = 'earliest'
