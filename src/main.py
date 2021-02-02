from docopt import docopt

from .kafka import Kafka
from .cli_cmd import kafkaCmd


def run():
    args = docopt(kafkaCmd(), version="1.0.0")

    if args["send"]:
        kafka = Kafka(kafka_server_port=args["<server-port>"])
        kafka.produce(args["<message>"], topic=args["<kafka-topic>"])

    if args["receive"]:
        kafka = Kafka(
            kafka_server_port=args["<server-port>"],
            start_time=args["<latest-or-earliest>"],
        )
        kafka.consume(topic=args["<kafka-topic>"])
