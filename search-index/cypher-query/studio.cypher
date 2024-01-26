MATCH (n:Studio)
RETURN
'studio/' + toString(n.id) AS id
, labels(n) AS type
, n.name AS name  
SKIP $skip_count
LIMIT $BATCH_SIZE