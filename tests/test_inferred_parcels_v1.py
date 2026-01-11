from app.ingest import inferred_parcels_v1


def test_inferred_parcels_v1_importable() -> None:
    assert inferred_parcels_v1 is not None


def test_inferred_parcels_v1_cli_parses_bbox() -> None:
    parser = inferred_parcels_v1._build_arg_parser()
    args = parser.parse_args(["--bbox", "46.20,24.20,47.30,25.10"])
    assert inferred_parcels_v1._parse_bbox(args.bbox) == (46.20, 24.20, 47.30, 25.10)
