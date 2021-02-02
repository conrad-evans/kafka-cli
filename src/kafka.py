from confluent_kafka import Producer, Consumer


class Kafka:
    """Kafka Message Broker Class"""

    PRODUCER_CONFIG = {}
    CONSUMER_CONFIG = {"group.id": "mygroup"}

    def __init__(self, kafka_server_port="localhost:9092", start_time="earliest"):
        """
        param1: (optional) -> string
        param2: (optional) -> string
        """
        self.CONSUMER_CONFIG["auto.offset.reset"] = start_time
        self.PRODUCER_CONFIG["bootstrap.servers"] = kafka_server_port

    def deliveryReport(self, err, msg):
        if err is not None:
            print("[ERROR] Message delivery failed: {}".format(err))
        else:
            print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    def produce(self, data, topic="mytopic"):
        self.setDefaultConfig()
        if topic is None:
            topic = "mytopic"

        producer = Producer(self.PRODUCER_CONFIG)

        producer.poll(0)
        producer.produce(topic, data.encode("utf-8"), callback=self.deliveryReport)

        producer.flush()

    def consume(self, topic, running=True):
        self.setDefaultConfig()
        if topic is None:
            topic = "mytopic"
        if self.CONSUMER_CONFIG["auto.offset.reset"] is None:
            self.CONSUMER_CONFIG["auto.offset.reset"] = "earliest"
        self.CONSUMER_CONFIG = {**self.CONSUMER_CONFIG, **self.PRODUCER_CONFIG}

        consumer = Consumer(self.CONSUMER_CONFIG)
        consumer.subscribe([topic])

        while running:
            data = consumer.poll(1.0)
            if data is None:
                continue
            if data.error():
                print("Consumer error: {}".format(data.error()))
                continue
            print("Received message: {}".format(data.value().decode("utf-8")))

    def setDefaultConfig(self):
        if self.PRODUCER_CONFIG["bootstrap.servers"] is None:
            self.PRODUCER_CONFIG["bootstrap.servers"] = "localhost:9092"
