MATCH (n:Character)
RETURN
'character/' + toString(n.id) AS id
, labels(n) AS type
, n.name_full AS name  
, n.description AS description  
, n.site_url as url
, n.image as image_url
SKIP $skip_count
LIMIT $BATCH_SIZE