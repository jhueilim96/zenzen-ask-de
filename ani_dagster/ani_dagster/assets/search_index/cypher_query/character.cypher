MATCH (n:Character)
RETURN
'character/' + toString(n.id) AS id
, n.name_full AS title
, [ val in ([n.name_first,n.name_last,n.name_native,n.name_middle] + n.name_alternative)  WHERE val IS NOT NULL] AS alternative
, labels(n) AS type
, n.description AS description
, n.image as image_url
SKIP $skip_count
LIMIT $BATCH_SIZE