from app.main import app


def test_geo_debug_suhail_srid_route_registered() -> None:
    routes = {getattr(route, "path", "") for route in app.router.routes}

    assert "/v1/geo/debug/suhail_srid" in routes


def test_geo_debug_layers_route_registered() -> None:
    routes = {getattr(route, "path", "") for route in app.router.routes}

    assert "/v1/geo/debug/layers" in routes
