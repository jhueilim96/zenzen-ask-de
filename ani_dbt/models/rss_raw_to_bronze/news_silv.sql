with union_table as (
    select * from {{ ref('chrunchyroll_feed_bronze') }}
    union all
    select * from {{ ref('animenewsnetwork_feed_bronze') }}
    union all
    select * from {{ ref('otakukart_feed_bronze') }}
)
select * from union_table