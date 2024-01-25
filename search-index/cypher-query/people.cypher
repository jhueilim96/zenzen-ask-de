MATCH (n:People)
RETURN
'People/' + toString(n.id) AS id
, labels(n) AS type
, coalesce(n.name_full, n.name_first + ' ' + n.name_last )AS name
, n.description AS description
, n.siteUrl AS url
, n.image_medium AS image_url
SKIP $skip_count
LIMIT $BATCH_SIZE