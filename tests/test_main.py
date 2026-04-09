
from ifer_tool.main import hello


def test_hello() -> None:
    assert hello() == "Hello World!"