from app.main import app


def test_tiles_routes_aliases() -> None:
    routes = {getattr(route, "path", "") for route in app.router.routes}

    assert "/v1/tiles/parcels/{z}/{x}/{y}.pbf" in routes
    assert "/tiles/parcels/{z}/{x}/{y}.pbf" in routes
