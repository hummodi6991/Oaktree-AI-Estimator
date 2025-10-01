import os
import pathlib

import httpx

BASE = os.environ.get("API_BASE", "http://127.0.0.1:8000")
HERE = pathlib.Path(__file__).resolve().parents[1] / "data" / "samples"


def _post_file(path: pathlib.Path, url: str, extra: dict | None = None) -> None:
    with path.open("rb") as handle:
        response = httpx.post(
            url,
            files={"file": (path.name, handle, "text/csv")},
            params=extra or {},
            timeout=httpx.Timeout(60.0),
        )
        response.raise_for_status()
        print("OK:", url, "→", response.json())


def main() -> None:
    _post_file(HERE / "cci_sample.csv", f"{BASE}/v1/ingest/cci")
    _post_file(HERE / "rates_sample.csv", f"{BASE}/v1/ingest/rates")
    _post_file(HERE / "indicators_sample.csv", f"{BASE}/v1/ingest/indicators")
    _post_file(
        HERE / "comps_sale_sample.csv",
        f"{BASE}/v1/ingest/comps",
        {"comp_type": "sale"},
    )
    print("Sample ingest complete.")


if __name__ == "__main__":
    main()
