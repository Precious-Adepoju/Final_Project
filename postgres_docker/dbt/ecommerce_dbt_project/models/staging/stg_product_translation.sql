select *
from {{ source('raw', 'product_translation') }}