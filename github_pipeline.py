import dlt
import os
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator



@dlt.source
def jaffleshop_source():
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
              "page_size":500,
              "start_date":"2017-08-01"
          })
        for resource in pages:
            yield resource

    @dlt.resource(table_name="customers", write_disposition="replace",parallelized=True)
    def jaffleshop_customers():
      pages= client.paginate("/customers")
      
      for resource in pages:
            yield resource

    @dlt.resource(table_name="products", write_disposition="replace", parallelized=True)
    def jaffleshop_products():
        pages= client.paginate("/products")

        for resource in pages:
            yield resource

    return jaffleshop_orders,jaffleshop_customers,jaffleshop_products

pipeline = dlt.pipeline(
    pipeline_name="dltoptim_v1",
    destination="duckdb",
    dataset_name="dltexo",
)



if __name__ == "__main__":
    load_info= pipeline.run(jaffleshop_source())
    print(load_info)