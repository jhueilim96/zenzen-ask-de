MATCH (n:People)
RETURN
'people/' + toString(n.id) AS id
, coalesce(n.name_full, n.name_first + ' ' + n.name_last ) AS title
, [ val in ([n.name_first,n.name_last,n.name_native,n.name_middle] + n.name_alternative)  WHERE val IS NOT NULL] AS alternative
, labels(n) AS type
, n.description AS description
, n.image_medium AS image_url
SKIP $skip_count
LIMIT $BATCH_SIZE