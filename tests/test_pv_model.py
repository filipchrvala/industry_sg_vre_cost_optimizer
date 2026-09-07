"""Tests for the physical PV production model and the SHMU client."""

from __future__ import annotations

import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pieces.common_somes import pv_model, shmu_opendata


def _utc(dt_local: datetime, offset_seconds: int) -> datetime:
    return dt_local - timedelta(seconds=offset_seconds)


class TestSolarPosition:
    """Solar geometry is the foundation of the transposition, so it is pinned."""

    def test_summer_solstice_noon_faces_south_at_expected_elevation(self):
        # Bratislava, 13:00 CEST on the solstice is close to solar noon.
        elevation, azimuth = pv_model.solar_position(
            _utc(datetime(2024, 6, 21, 13, 0), 7200), 48.1486, 17.1077
        )
        # 90 - latitude + declination
        assert elevation == pytest.approx(90 - 48.1486 + 23.44, abs=1.0)
        assert azimuth == pytest.approx(180.0, abs=6.0)

    def test_winter_solstice_noon_elevation(self):
        elevation, azimuth = pv_model.solar_position(
            _utc(datetime(2024, 12, 21, 12, 0), 3600), 48.1486, 17.1077
        )
        assert elevation == pytest.approx(90 - 48.1486 - 23.44, abs=1.0)
        assert azimuth == pytest.approx(180.0, abs=6.0)

    def test_sun_rises_in_the_east_and_sets_in_the_west(self):
        morning, _ = pv_model.solar_position(
            _utc(datetime(2024, 6, 21, 6, 0), 7200), 48.1486, 17.1077
        )
        _, az_morning = pv_model.solar_position(
            _utc(datetime(2024, 6, 21, 8, 0), 7200), 48.1486, 17.1077
        )
        _, az_evening = pv_model.solar_position(
            _utc(datetime(2024, 6, 21, 18, 0), 7200), 48.1486, 17.1077
        )
        assert morning > 0
        assert 45 < az_morning < 135, "morning sun should be in the eastern half"
        assert 225 < az_evening < 315, "evening sun should be in the western half"

    def test_sun_is_below_horizon_at_local_midnight(self):
        elevation, _ = pv_model.solar_position(
            _utc(datetime(2024, 6, 21, 0, 0), 7200), 48.1486, 17.1077
        )
        assert elevation < 0

    def test_daylight_saving_is_not_ignored(self):
        """The same wall clock in summer and winter must not map to the same hour angle."""
        summer = pv_model.solar_position(
            _utc(datetime(2024, 7, 1, 13, 0), 7200), 48.1486, 17.1077
        )
        naive = pv_model.solar_position(datetime(2024, 7, 1, 13, 0), 48.1486, 17.1077)
        assert abs(summer[1] - naive[1]) > 10.0


class TestTransposition:
    def test_tilted_south_array_gains_over_horizontal_in_winter(self):
        common = dict(
            ghi=200.0, dni=600.0, dif=60.0,
            elevation_deg=18.0, azimuth_deg=180.0, dni_extra=1400.0,
            surface_azimuth_deg=180.0, albedo=0.2,
        )
        flat = pv_model.poa_irradiance(tilt_deg=0.0, **common)
        tilted = pv_model.poa_irradiance(tilt_deg=35.0, **common)
        assert tilted["poa_global"] > flat["poa_global"]

    def test_no_irradiance_when_sun_is_down(self):
        poa = pv_model.poa_irradiance(
            ghi=0.0, dni=0.0, dif=0.0, elevation_deg=-5.0, azimuth_deg=90.0,
            dni_extra=1400.0, tilt_deg=30.0, surface_azimuth_deg=180.0, albedo=0.2,
        )
        assert poa["poa_global"] == 0.0

    def test_north_facing_array_collects_less_than_south_facing(self):
        common = dict(
            ghi=500.0, dni=700.0, dif=150.0, elevation_deg=40.0, azimuth_deg=180.0,
            dni_extra=1400.0, tilt_deg=30.0, albedo=0.2,
        )
        south = pv_model.poa_irradiance(surface_azimuth_deg=180.0, **common)
        north = pv_model.poa_irradiance(surface_azimuth_deg=0.0, **common)
        assert south["poa_global"] > north["poa_global"]


class TestCellTemperature:
    def test_module_is_hotter_than_ambient_under_irradiance(self):
        assert pv_model.cell_temperature(800.0, 20.0, 1.0, 45.0) > 20.0

    def test_wind_cools_the_module(self):
        calm = pv_model.cell_temperature(800.0, 25.0, 0.5, 45.0)
        windy = pv_model.cell_temperature(800.0, 25.0, 8.0, 45.0)
        assert windy < calm

    def test_noct_definition_is_respected(self):
        """At 800 W/m2, 20 C ambient and 1 m/s the module should sit near NOCT."""
        assert pv_model.cell_temperature(800.0, 20.0, 1.0, 45.0) == pytest.approx(45.0, abs=1.5)


class TestAcPower:
    def _spec(self, **kw):
        return pv_model.array_spec_from_scenario({}, installed_kwp=kw.pop("kwp", 100.0),
                                                 tilt_deg=kw.pop("tilt", 30.0))

    def test_no_output_at_night(self):
        out = pv_model.ac_power_kw(
            dt=datetime(2024, 6, 21, 1, 0), ghi=0.0, dni=0.0, dif=0.0,
            temp_c=15.0, wind_ms=1.0, lat=48.1486, lon=17.1077,
            spec=self._spec(), utc_offset_seconds=7200,
        )
        assert out["pvout_kw"] == 0.0

    def test_zero_size_plant_produces_nothing(self):
        out = pv_model.ac_power_kw(
            dt=datetime(2024, 6, 21, 12, 0), ghi=900.0, dni=800.0, dif=120.0,
            temp_c=25.0, wind_ms=2.0, lat=48.1486, lon=17.1077,
            spec=self._spec(kwp=0.0), utc_offset_seconds=7200,
        )
        assert out["pvout_kw"] == 0.0

    def test_output_never_exceeds_the_inverter_limit(self):
        spec = self._spec(kwp=100.0)
        out = pv_model.ac_power_kw(
            dt=datetime(2024, 6, 21, 13, 0), ghi=1100.0, dni=1000.0, dif=100.0,
            temp_c=20.0, wind_ms=2.0, lat=48.1486, lon=17.1077,
            spec=spec, utc_offset_seconds=7200,
        )
        assert out["pvout_kw"] <= spec.ac_limit_kw + 1e-9
        assert out["clipped_kw"] >= 0.0

    def test_hot_weather_reduces_output(self):
        common = dict(
            dt=datetime(2024, 6, 21, 13, 0), ghi=800.0, dni=700.0, dif=150.0,
            wind_ms=1.0, lat=48.1486, lon=17.1077,
            spec=self._spec(kwp=1000.0), utc_offset_seconds=7200,
        )
        cool = pv_model.ac_power_kw(temp_c=5.0, **common)
        hot = pv_model.ac_power_kw(temp_c=35.0, **common)
        assert hot["pvout_kw"] < cool["pvout_kw"]

    def test_production_scales_with_plant_size(self):
        common = dict(
            dt=datetime(2024, 6, 21, 13, 0), ghi=800.0, dni=700.0, dif=150.0,
            temp_c=20.0, wind_ms=2.0, lat=48.1486, lon=17.1077, utc_offset_seconds=7200,
        )
        small = pv_model.ac_power_kw(spec=self._spec(kwp=100.0), **common)["pvout_kw"]
        large = pv_model.ac_power_kw(spec=self._spec(kwp=200.0), **common)["pvout_kw"]
        assert large == pytest.approx(2 * small, rel=1e-6)

    def test_pvout_is_not_a_fixed_multiple_of_ghi(self):
        """Regression guard for the target-leakage defect this model replaced.

        The previous generator used PVOUT = kWp * GHI / 1000 * 0.75, so PVOUT/GHI
        was a constant and any model trained on it just recovered that constant.
        """
        spec = self._spec(kwp=500.0)
        ratios = []
        for month, hour, ghi, temp, wind in [
            (1, 12, 200.0, -2.0, 1.0),
            (4, 10, 450.0, 12.0, 3.0),
            (6, 13, 900.0, 30.0, 1.0),
            (6, 17, 400.0, 28.0, 5.0),
            (9, 11, 600.0, 18.0, 2.0),
        ]:
            out = pv_model.ac_power_kw(
                dt=datetime(2024, month, 21, hour, 0), ghi=ghi, dni=ghi * 0.7,
                dif=ghi * 0.25, temp_c=temp, wind_ms=wind,
                lat=48.1486, lon=17.1077, spec=spec, utc_offset_seconds=7200,
            )
            ratios.append(out["pvout_kw"] / ghi)

        assert max(ratios) - min(ratios) > 0.05, (
            "PVOUT/GHI is nearly constant, so the target leaks into the features"
        )


class TestLossChain:
    def test_static_loss_factor_is_a_plausible_fraction(self):
        spec = pv_model.array_spec_from_scenario({}, installed_kwp=100.0, tilt_deg=30.0)
        assert 0.80 < spec.static_loss_factor < 0.95

    def test_scenario_overrides_losses_and_geometry(self):
        cfg = {
            "pv": {
                "azimuth_deg": 200.0,
                "dc_ac_ratio": 1.35,
                "losses": {"soiling_loss_pct": 6.0, "inverter_efficiency_pct": 96.0},
            }
        }
        spec = pv_model.array_spec_from_scenario(cfg, installed_kwp=250.0, tilt_deg=15.0)
        assert spec.azimuth_deg == 200.0
        assert spec.dc_ac_ratio == 1.35
        assert spec.soiling_loss_pct == 6.0
        assert spec.ac_limit_kw == pytest.approx(250.0 / 1.35)

    def test_installation_shading_level_maps_to_a_loss(self):
        low = pv_model.array_spec_from_scenario(
            {"equipment": {"constraints": {"installation": {"shading": "low"}}}},
            installed_kwp=100.0, tilt_deg=30.0,
        )
        high = pv_model.array_spec_from_scenario(
            {"equipment": {"constraints": {"installation": {"shading": "high"}}}},
            installed_kwp=100.0, tilt_deg=30.0,
        )
        assert high.shading_loss_pct > low.shading_loss_pct
        assert high.static_loss_factor < low.static_loss_factor

    def test_explicit_loss_wins_over_shading_level(self):
        spec = pv_model.array_spec_from_scenario(
            {
                "pv": {"losses": {"shading_loss_pct": 0.25}},
                "equipment": {"constraints": {"installation": {"shading": "high"}}},
            },
            installed_kwp=100.0, tilt_deg=30.0,
        )
        assert spec.shading_loss_pct == 0.25


class TestErbs:
    def test_overcast_sky_is_mostly_diffuse(self):
        assert pv_model.erbs_diffuse_fraction(50.0, 0.5, 1400.0) > 0.9

    def test_clear_sky_is_mostly_direct(self):
        assert pv_model.erbs_diffuse_fraction(900.0, 0.95, 1400.0) < 0.3


class TestShmuStationLookup:
    def test_nearest_station_is_really_the_closest_one(self):
        for lat, lon in [
            (48.1486, 17.1077),   # Bratislava
            (48.7200, 21.2600),   # Kosice
            (49.0600, 20.3000),   # Poprad basin
            (48.3060, 18.0760),   # Nitra
        ]:
            station = shmu_opendata.nearest_station(lat, lon)
            assert station is not None
            closest = min(s.distance_km(lat, lon) for s in shmu_opendata.SHMU_STATIONS)
            assert station.distance_km(lat, lon) == pytest.approx(closest)

    def test_major_cities_have_a_station_within_reach(self):
        for lat, lon, limit_km in [
            (48.1486, 17.1077, 15.0),
            (48.7200, 21.2600, 15.0),
            (49.2231, 18.7394, 15.0),
        ]:
            station = shmu_opendata.nearest_station(lat, lon)
            assert station is not None
            assert station.distance_km(lat, lon) < limit_km

    def test_distant_site_has_no_usable_station(self):
        # Madrid is far outside the network.
        assert shmu_opendata.nearest_station(40.4168, -3.7038) is None

    def test_slovakia_bounding_box(self):
        assert shmu_opendata.is_in_slovakia(48.1486, 17.1077)
        assert not shmu_opendata.is_in_slovakia(52.52, 13.40)

    def test_distance_is_symmetric_and_zero_at_the_station(self):
        station = shmu_opendata.SHMU_STATIONS[0]
        assert station.distance_km(station.latitude, station.longitude) == pytest.approx(0.0, abs=1e-6)

    def test_every_station_is_inside_slovakia(self):
        for station in shmu_opendata.SHMU_STATIONS:
            assert shmu_opendata.is_in_slovakia(station.latitude, station.longitude), station.name


class TestBiasReport:
    def test_reports_insufficient_overlap_on_short_series(self):
        pd = pytest.importorskip("pandas")
        idx = pd.date_range("2026-01-01", periods=5, freq="15min")
        out = shmu_opendata.bias_report(
            pd.Series([100.0] * 5, index=idx), pd.Series([100.0] * 5, index=idx)
        )
        assert out["status"] == "insufficient_overlap"

    def test_detects_a_known_multiplicative_bias(self):
        pd = pytest.importorskip("pandas")
        idx = pd.date_range("2026-06-01 06:00", periods=60, freq="15min")
        measured = pd.Series([400.0 + 3 * i for i in range(60)], index=idx)
        modelled = measured * 0.9
        out = shmu_opendata.bias_report(modelled, measured)
        assert out["status"] == "ok"
        assert out["bias_pct"] == pytest.approx(-10.0, abs=0.1)
        assert out["recommended_scale_factor"] == pytest.approx(1.0 / 0.9, rel=1e-3)

    def test_night_steps_do_not_flatter_the_metrics(self):
        pd = pytest.importorskip("pandas")
        idx = pd.date_range("2026-06-01 00:00", periods=200, freq="15min")
        measured = pd.Series(
            [0.0 if i % 2 else 500.0 for i in range(200)], index=idx
        )
        modelled = measured * 0.8
        out = shmu_opendata.bias_report(modelled, measured)
        assert out["daylight_steps"] == 100
        assert out["bias_pct"] == pytest.approx(-20.0, abs=0.1)
