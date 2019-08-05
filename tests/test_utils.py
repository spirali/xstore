from orco.utils import format_time


def test_format_time():
    assert format_time(1) == "1.0s"
    assert format_time(1.2) == "1.2s"
    assert format_time(10/3) == "3.3s"

    assert format_time(0.1) == "100ms"
    assert format_time(1/3) == "333ms"
    assert format_time(0) == "0ms"

    assert format_time(60) == "1.0m"
    assert format_time(1000/3) == "5.6m"
    assert format_time(150.3) == "2.5m"

    assert format_time(3600) == "1.0h"
    assert format_time(100000/3) == "9.3h"
    assert format_time(1000000000) == "277777.8h"
