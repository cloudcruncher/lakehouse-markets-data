import pytest

from markets_data.scale import AVAILABLE_NOW, settings, trigger_kwargs


def test_laptop_is_a_catch_up_every_five_minutes():
    assert settings({}) == (AVAILABLE_NOW, 300)
    assert settings({"PLATFORM_SCALE": "laptop", "RUN_EVERY": "60"}) == (AVAILABLE_NOW, 60)


def test_full_scale_streams_continuously():
    assert settings({"PLATFORM_SCALE": "full"}) == ("30 seconds", 0)
    assert settings({"PLATFORM_SCALE": "full", "TRIGGER": "10 seconds"}) == ("10 seconds", 0)


def test_a_processing_time_trigger_never_loops():
    assert settings({"TRIGGER": "1 minute"}) == ("1 minute", 0)


def test_an_unknown_scale_stops_the_service():
    with pytest.raises(SystemExit, match="PLATFORM_SCALE"):
        settings({"PLATFORM_SCALE": "huge"})


def test_trigger_kwargs():
    assert trigger_kwargs(AVAILABLE_NOW) == {"availableNow": True}
    assert trigger_kwargs("30 seconds") == {"processingTime": "30 seconds"}
