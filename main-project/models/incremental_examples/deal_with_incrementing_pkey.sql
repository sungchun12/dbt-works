{{
    config(
        materialized='incremental',
        unique_key='emailid',
        incremental_strategy='merge',
    )
}}

-- primary_key_of_destination_table: this needs to be replaced by the actual name of the primary key
-- If you do NOT replace this, this model will NOT successfully run as primary_key_of_destination_table does NOT exist in either table
-- I'll assume emailid in this example is the unique key as this reproduces the exact scenario illustrated in the original email

select * from {{ ref('first_table') }}