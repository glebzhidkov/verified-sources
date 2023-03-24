from pipelines.strapi import strapi_source

def test_strapi_source():
    endpoints = ['athletes']
    resources = strapi_source(endpoints=endpoints).resources.keys()

    assert resources == endpoints