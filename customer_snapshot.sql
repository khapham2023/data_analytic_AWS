{% snapshot customer_snapshot %}

    {{ config( 
        target_schema = 'intermediate',
        strategy = 'timestamp',
        unique_key = 'customer_sk',
        updated_at = 'loaded_at'
    )}}

select * from {{ ref('stg_customer')}}

{% endsnapshot %}
