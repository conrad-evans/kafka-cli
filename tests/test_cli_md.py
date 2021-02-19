from src.cli_cmd import kafkaCmd


def test_kafkaCmd():
    # test
    docs = kafkaCmd()

    # assert
    assert type(docs) == str
    assert "-h --help" in docs
    