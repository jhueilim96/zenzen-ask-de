with raw_ as (
    select * from {{ source('main', 'rss_chruncyroll_raw')}}
), dedupe as (
    select
    any_value(content) as content
    , any_value(site) as site
    , retrieved_at
    from raw_
    group by (content_hash, retrieved_at)
), tb_extract as (
    select
    json_extract_string(content, [
        '$.feed'
        ,'$.entries'
    ]) content_extract_list
    from dedupe
), feeds as (
    select
    json_extract_string(unnest(content_extract_list[2]::JSON[]), [
        '$.title'
        , '$.link'
        , '$.id'
        , '$.summary'
        , '$.published'
        , '$.published_parsed'
        , '$.tags'
    ]) feeds_extract
    from tb_extract
), parsed as (
    select
        feeds_extract[1] title
        , feeds_extract[2] link
        , feeds_extract[3] id
        , feeds_extract[4] summary
        , feeds_extract[6]::int[] ts
        , [doc->>'term' for doc in (feeds_extract[7]::JSON[])] tags
    from feeds
)
    select
    * exclude(ts)
    , make_timestamptz(ts[1], ts[2], ts[3], ts[4], ts[5], ts[6], 'GMT' ) published
    from parsed