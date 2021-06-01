{{
    config(
        materialized='incremental',
        unique_key='primary_key_of_destination_table',
        incremental_strategy='merge',
    )
}}

select * from {{ ref('first_table') }}