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
        assert kafka.producer is None
        assert kafka.consumer is None

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

        kafka = Kafka(kafka_server_port="localhost:9092", start_time="lastest")

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
        pass

    def test_consume(self):
        pass

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
        kafka.CONSUMER_CONFIG['auto.offset.reset'] == 'earliest'
        kafka.PRODUCER_CONFIG['bootstrap.servers'] == 'localhost:9092'

        # case 2 assertion
        kafka_two.CONSUMER_CONFIG['auto.offset.reset'] == start_time
        kafka_two.PRODUCER_CONFIG['bootstrap.servers'] == kafka_server_port
        
