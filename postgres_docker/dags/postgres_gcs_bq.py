from airflow.decorators import dag
from datetime import datetime, timedelta 
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.baseoperator import chain


default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 8, 8),
    'retries': 1,  
    'retry_delay': timedelta(minutes=5)  
}


PG_CONN = "postgresdb" 
GCS_CONN = "gcs_project_connection" 
BQ_PROJECT = "final-project-431620" 
BQ_DATASET = "raw" 
SCHEMA = "ecommerce" 


@dag(
    dag_id='ecommerce_dag',
    default_args=default_args,
    description='This dag import data into bigquery', 
    schedule_interval=None,  
    catchup=False, 
)

def load_to_bq(): 

    ### Task to export customers table to gcs
    export_customers_to_gcs = PostgresToGCSOperator(
        task_id='export_customers_to_gcs',  
        sql=f"SELECT * FROM {SCHEMA}.customers", 
        bucket="capstone-bucket-07", 
        filename='customers.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )

    
    ### Task to export geolocation table to gcs
    export_geolocation_to_gcs = PostgresToGCSOperator(
        task_id='export_geolocation_to_gcs', 
        sql=f"SELECT * FROM {SCHEMA}.geolocation",  
        bucket="capstone-bucket-07", 
        filename='geolocation.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )

    ### Task to export order_items table to gcs
    export_order_items_to_gcs = PostgresToGCSOperator(
        task_id='export_order_items_to_gcs', 
        sql=f"SELECT * FROM {SCHEMA}.order_items", 
        bucket="capstone-bucket-07", 
        filename='order_items.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )


    ### Task to export order_payments table to gcs
    export_order_payments_to_gcs = PostgresToGCSOperator(
        task_id='export_order_payments_to_gcs',  
        sql=f"SELECT * FROM {SCHEMA}.order_payments",  
        bucket="capstone-bucket-07", 
        filename='order_payments.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )

    ### Task to export order_reviews table to gcs
    export_order_reviews_to_gcs = PostgresToGCSOperator(
        task_id='export_order_reviews_to_gcs',  
        sql=f"SELECT * FROM {SCHEMA}.order_reviews",  
        bucket="capstone-bucket-07", 
        filename='order_reviews.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )

    ### Task to export orders table to gcs
    export_orders_to_gcs = PostgresToGCSOperator(
        task_id='export_orders_to_gcs',  
        sql=f"SELECT * FROM {SCHEMA}.orders",  
        bucket="capstone-bucket-07", 
        filename='orders.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )

    ### Task to export products table to gcs
    export_products_to_gcs = PostgresToGCSOperator(
        task_id='export_products_to_gcs',  
        sql=f"SELECT * FROM {SCHEMA}.products",  
        bucket="capstone-bucket-07", 
        filename='products.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )

    ### Task to export sellers table to gcs
    export_sellers_to_gcs = PostgresToGCSOperator(
        task_id='export_sellers_to_gcs',  
        sql=f"SELECT * FROM {SCHEMA}.sellers",  
        bucket="capstone-bucket-07", 
        filename='sellers.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )

    ### Task to export product_name_translation table to gcs
    export_product_name_translation_to_gcs = PostgresToGCSOperator(
        task_id='export_product_name_translation_to_gcs',  
        sql=f"SELECT * FROM {SCHEMA}.product_name_translation",  
        bucket="capstone-bucket-07", 
        filename='product_translation.csv', 
        export_format='csv',
        gcp_conn_id=GCS_CONN,
        postgres_conn_id=PG_CONN,
        use_server_side_cursor=False,
    )

    ## Task to load customers from GCS Bucket to BigQuery                          
    export_customers_from_gcs_to_bq = GCSToBigQueryOperator( 
        task_id="export_customers_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'customers.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.customers", 
        schema_fields=
        [
            {
                "name": "customer_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "customer_unique_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "customer_zip_code_prefix",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "customer_city",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "customer_state",
                "type": "STRING",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )
    
   
    ## Task to load geolocation from GCS Bucket to BigQuery 
    export_geolocation_from_gcs_to_bq = GCSToBigQueryOperator( 
        task_id="export_geolocation_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'geolocation.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.geolocation", 
        schema_fields=
        [
            {
                "name": "geolocation_zip_code_prefix",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "geolocation_lat",
                "type": "FLOAT",
                "mode": "NULLABLE"
            },
            {
                "name": "geolocation_lng",
                "type": "FLOAT",
                "mode": "NULLABLE"
            },
            {
                "name": "geolocation_city",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "geolocation_state",
                "type": "STRING",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    ## Task to load order_items from GCS Bucket to BigQuery 
    export_order_items_from_gcs_to_bq = GCSToBigQueryOperator(
        task_id="export_order_items_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'order_items.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.order_items", 
        schema_fields=
        [
            {
                "name": "order_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "order_item_id",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "product_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "seller_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "shipping_limit_date",
                "type": "TIMESTAMP",
                "mode": "NULLABLE"
            },
            {
                "name": "price",
                "type": "FLOAT",
                "mode": "NULLABLE"
            },
            {
                "name": "freight_value",
                "type": "FLOAT",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )


    ## Task to load order_payments from GCS Bucket to BigQuery 
    export_order_payments_from_gcs_to_bq = GCSToBigQueryOperator( 
        task_id="export_order_payments_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'order_payments.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.order_payments", 
        schema_fields=
        [
            {
                "name": "order_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "payment_sequential",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "payment_type",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "payment_installments",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "payment_value",
                "type": "FLOAT",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )


    ## Task to load order_reviews from GCS Bucket to BigQuery 
    export_order_reviews_from_gcs_to_bq = GCSToBigQueryOperator( 
        task_id="export_order_reviews_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'order_reviews.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.order_reviews", 
        schema_fields=
        [
            {
                "name": "review_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "order_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "review_score",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "review_comment_title",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "review_comment_message",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "review_creation_date",
                "type": "TIMESTAMP",
                "mode": "NULLABLE"
            },
            {
                "name": "review_answer_timestamp",
                "type": "TIMESTAMP",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )


    ## Task to load orders from GCS Bucket to BigQuery
    export_orders_from_gcs_to_bq = GCSToBigQueryOperator( 
        task_id="export_orders_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'orders.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.orders", 
        schema_fields=
        [
            {
                "name": "order_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "customer_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "order_status",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "order_purchase_timestamp",
                "type": "TIMESTAMP",
                "mode": "NULLABLE"
            },
            {
                "name": "order_approved_at",
                "type": "TIMESTAMP",
                "mode": "NULLABLE"
            },
            {
                "name": "order_delivered_carrier_date",
                "type": "TIMESTAMP",
                "mode": "NULLABLE"
            },
            {
                "name": "order_delivered_customer_date",
                "type": "TIMESTAMP",
                "mode": "NULLABLE"
            },
            {
                "name": "order_estimated_delivery_date",
                "type": "TIMESTAMP",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )


    ## Task to load products from GCS Bucket to BigQuery
    export_products_from_gcs_to_bq = GCSToBigQueryOperator( 
        task_id="export_products_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'products.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.products", 
        schema_fields=
        [
            {
                "name": "product_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "product_category_name",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "product_name_lenght",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "product_description_lenght",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "product_photos_qty",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "product_weight_g",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "product_length_cm",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "product_height_cm",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "product_width_cm",
                "type": "INTEGER",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )


    ## Task to load sellers from GCS Bucket to BigQuery
    export_sellers_from_gcs_to_bq = GCSToBigQueryOperator(
        task_id="export_sellers_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'sellers.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.sellers", 
        schema_fields=
        [
            {
                "name": "seller_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "seller_zip_code_prefix",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "seller_city",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "seller_state",
                "type": "STRING",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )


    ## Task to load product_translation from GCS Bucket to BigQuery
    export_product_name_translation_from_gcs_to_bq = GCSToBigQueryOperator(
        task_id="export_product_translation_to_bigquery",  
        bucket="capstone-bucket-07", 
        source_objects=[f'product_translation.csv'], 
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.product_translation", 
        schema_fields=
        [
            {
                "name": "product_category_name",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "product_category_name_english",
                "type": "STRING",
                "mode": "NULLABLE"
            }
        ], 
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id = GCS_CONN,
    )

    #Task dependencies
    chain(
        export_customers_to_gcs, export_customers_from_gcs_to_bq,
        export_geolocation_to_gcs, export_geolocation_from_gcs_to_bq,
        export_order_items_to_gcs, export_order_items_from_gcs_to_bq,
        export_order_payments_to_gcs, export_order_payments_from_gcs_to_bq,
        export_order_reviews_to_gcs, export_order_reviews_from_gcs_to_bq,
        export_orders_to_gcs, export_orders_from_gcs_to_bq,
        export_products_to_gcs, export_products_from_gcs_to_bq,
        export_sellers_to_gcs, export_sellers_from_gcs_to_bq,
        export_product_name_translation_to_gcs, export_product_name_translation_from_gcs_to_bq,
    )

load_to_bq()

    