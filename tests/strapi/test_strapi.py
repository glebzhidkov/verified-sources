from pipelines.strapi import strapi_source

import dlt
import pytest
from tests.utils import ALL_DESTINATIONS, assert_load_info



@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(pipeline_name='strapi', destination=destination_name, dataset_name='strapi', full_refresh=True)
    load_info = pipeline.run(strapi_source())
    print(load_info)
    assert_load_info(load_info)

def test_strapi_source():
    endpoints = ['athletes']
    resources = strapi_source(endpoints=endpoints).resources.keys()

    assert resources == endpoints