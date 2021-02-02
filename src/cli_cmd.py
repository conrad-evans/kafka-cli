from docopt import docopt


def kafkaCmd():
    docs = """Kafka Cli.

    Usage:
      kafka_cli.py send --message <message> [--channel <kafka-topic>] [--kafka [<server-port>]]
      kafka_cli.py receive [--channel <kafka-topic>] [--from <latest-or-earliest>] [--kafka <server-port>]
      kafka_cli.py --help
      kafka_cli.py --version

    Options:
      -h --help     Show this screen.
      --version     Show version.
      -r --receive  Recevive Message.
      -s --send     Send Message.
      -m --message  Message to be sent.
      -c --channel  Kafka topic where message is being sent.
      -k --kafka    Kafka server port. [default: localhost:9092]
      -f --from     Starting point of receiving the messages. [default: start]
    """
    return docs