MATCH (n:Anime)
RETURN
'anime/' + toString(n.id) AS id
, labels(n) AS type
, n.title AS name  
, n.description AS description  
SKIP $skip_count
LIMIT $BATCH_SIZE