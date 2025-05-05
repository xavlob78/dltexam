import dlt
import os
from dlt.sources.rest_api import rest_api_source
from itertools import islice
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator



@dlt.source
def jaffleshop_source(page_size,chunk_size):
    client = RESTClient(
        base_url= "https://jaffle-shop.scalevector.ai/api/v1",
        paginator= PageNumberPaginator (
                base_page= 1,
                page_param= "page",
                total_path= None,
        )
    )

    @dlt.resource(table_name="orders", write_disposition="replace", parallelized=True)
    def jaffleshop_orders():
      pages= client.paginate(
          "/orders",
          params={
              "page_size":page_size,
              "start_date":"2017-08-01"
          })
      while page_slice := list(islice(pages, chunk_size)):
        yield page_slice

    @dlt.resource(table_name="customers", write_disposition="replace",parallelized=True)
    def jaffleshop_customers():
      pages= client.paginate("/customers")
      while page_slice := list(islice(pages, chunk_size)):
        yield page_slice

    @dlt.resource(table_name="products", write_disposition="replace", parallelized=True)
    def jaffleshop_products():
      pages= client.paginate("/products")
      while page_slice := list(islice(pages, chunk_size)):
        yield page_slice

    return jaffleshop_orders,jaffleshop_customers,jaffleshop_products

pipeline = dlt.pipeline(
    pipeline_name="dltoptim_v1",
    destination="duckdb",
    dataset_name="dltexo",
)


extract_info = pipeline.extract(jaffleshop_source(100,100))
print(extract_info)

normalize_info = pipeline.normalize()
print(normalize_info)

load_info= pipeline.load()
print(load_info)