### Utility functions for defining API queries corresponding to the CMRBackend tests
import pandas as pd
from urllib.parse import urlencode
import helpers.dataframe as dataframe_helpers
import helpers.eodc_hub_role as eodc_hub_role


def gen_datetime_query(temporal):
    return pd.to_datetime(temporal[0]).isoformat("T") + "Z"


def get_tile_path(
    concept_id,
    variable,
    temporal,
    tile,
    *,
    tms: str = "WebMercatorQuad",
    backend: str = "xarray",
    scale: int = 1,
    return_mask: str = "true",
    **kwargs,
):
    temporal = gen_datetime_query(temporal)
    x, y, z = tile
    query = urlencode(
        {
            "concept_id": concept_id,
            "variable": variable,
            "datetime": temporal,
            "backend": backend,
            "scale": scale,
            "return_mask": return_mask,
            **kwargs,
        }
    )
    return f"/tiles/{tms}/{z}/{x}/{y}?{query}"


def generate_locust_urls(uri, output_file, **kwargs):
    credentials = eodc_hub_role.fetch_and_set_credentials()
    df = dataframe_helpers.load_all_into_dataframe(credentials, [uri], use_boto3=False)
    df = dataframe_helpers.expand_timings(df).reset_index()
    df["temporal"] = df.apply(lambda x: x["cmr_query"]["temporal"], axis=1)
    df["query"] = df.apply(
        lambda x: get_tile_path(
            x["dataset_id"], x["variable"], x["temporal"], x["tile"], **kwargs
        ),
        axis=1,
    )
    df["query"].to_csv(output_file, index=False, header=False)
    return df
