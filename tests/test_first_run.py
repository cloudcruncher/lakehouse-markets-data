"""A new stack is queryable without anyone clicking: the first-run sensor fills it."""

import dagster as dg

from markets_data.definitions import first_run, fx_rates, gold, sanctions


def _tick(instance):
    ctx = dg.build_sensor_context(instance=instance)
    result = first_run(ctx)
    return [r.run_key for r in (result or [])]


def _materialize(instance, asset):
    for spec in asset.specs:
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=spec.key))


def test_empty_stack_requests_reference_data_but_not_gold_yet():
    with dg.DagsterInstance.ephemeral() as instance:
        assert sorted(_tick(instance)) == ["first-run-fx_rates", "first-run-sanctions"]


def test_gold_follows_once_fx_and_sanctions_have_landed():
    with dg.DagsterInstance.ephemeral() as instance:
        _materialize(instance, fx_rates)
        assert _tick(instance) == ["first-run-sanctions"]  # gold still needs sanctions
        _materialize(instance, sanctions)
        assert _tick(instance) == ["first-run-gold"]


def test_nothing_is_requested_once_everything_has_run():
    with dg.DagsterInstance.ephemeral() as instance:
        for asset in (fx_rates, sanctions, gold):
            _materialize(instance, asset)
        assert _tick(instance) == []
