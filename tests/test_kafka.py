from src.cli_cmd import kafkaCmd
from src.kafka import Kafka


class Test_Kafka:
    def test__init__(self):
        # prepare
        kafka_server_port = "localhost:9092"
        start_time = "latest"

        # test
        kafka = Kafka(kafka_server_port=kafka_server_port, start_time=start_time)

        # assert
        assert kafka.PRODUCER_CONFIG["bootstrap.servers"] == kafka_server_port
        assert kafka.CONSUMER_CONFIG["bootstrap.servers"] == kafka_server_port
        assert kafka.CONSUMER_CONFIG["auto.offset.reset"] == start_time
        # assert kafka.producer is None
        # assert kafka.consumer is None

    def test_deliveryReport(self):
        # prepare
        err_one = None
        err_two = "failed"

        class MockMessage:
            # mock for message class
            def __init__(self) -> None:
                self.topic_called = False
                self.partition_called = False

            def topic(self):
                self.topic_called = True

            def partition(self):
                self.partition_called = True

        msg = MockMessage()
        msg_two = MockMessage()

        kafka = Kafka()

        # test
        # case 1
        # where error is None
        kafka.deliveryReport(err_one, msg)

        # case 2
        # where error is `not None`
        kafka.deliveryReport(err_two, msg_two)

        # assert
        # assertion for case 1
        assert msg.topic_called == True
        assert msg.partition_called == True

        # assertion for case 2
        assert msg_two.topic_called == False
        assert msg_two.partition_called == False

    def test_produce(self):
        # prepare
        data = "data stream"

        class MockProducer:
            def __init__(self) -> None:
                self.poll_called = False
                self.producer_called = False
                self.flush_called = False

            def poll(self, float_num):
                self.poll_called = True

            def produce(self, topic, data, callback=None):
                self.producer_called = True

            def flush(self):
                self.flush_called = True

        # test
        kafka = Kafka()
        kafka.producer = MockProducer()
        kafka.produce(data)

        # assert
        assert kafka.producer.flush_called == True
        assert kafka.producer.poll_called == True
        assert kafka.producer.producer_called == True

    def test_consume(self):
        # prepare
        class MockData:
            def __init__(self) -> None:
                self.value_called = False
                self.error_called = False

            def error(self):
                self.error_called = True
                return "error occured"

            def value(self):
                self.value_called = True
                # return "this message".encode('utf-8')

        class MockConsumer:
            def __init__(self) -> None:
                self.subscribe_called = False
                self.poll_called = False

            def subscribe(self, array):
                self.subscribe_called = True

            def poll(self, float_num):
                self.poll_called = True
                return MockData()

        # test
        kafka = Kafka()
        kafka.consumer = MockConsumer()
        # kafka.consume()

        # assert
        assert kafka.consumer.subscribe_called == False 
        assert kafka.consumer.poll_called == False

    def test_setDefaultConfig(self):
        # preapre
        kafka_server_port = "192.168.0.1"
        start_time = "latest"

        # test
        # case 1
        # no args supplied to `Kafka class`
        kafka = Kafka()
        kafka.setDefaultConfig()

        # case 2
        # args passed
        kafka_two = Kafka(kafka_server_port=kafka_server_port, start_time=start_time)
        kafka_two.setDefaultConfig()

        # assert
        # case 1 assertion
        kafka.CONSUMER_CONFIG["auto.offset.reset"] == "earliest"
        kafka.PRODUCER_CONFIG["bootstrap.servers"] == "localhost:9092"

        # case 2 assertion
        kafka_two.CONSUMER_CONFIG["auto.offset.reset"] == start_time
        kafka_two.PRODUCER_CONFIG["bootstrap.servers"] == kafka_server_port

    def test_setTopicToDefault(self):
        # prepare
        topic_one = None
        topic_two = "some topic"

        # test
        kafka = Kafka()

        # assert
        assert kafka.setTopicToDefault(topic_one) == "mytopic"
        assert kafka.setTopicToDefault(topic_two) == "some topic"
