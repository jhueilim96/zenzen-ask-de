MATCH (n:Studio)
RETURN
'studio/' + toString(n.id) AS id
, labels(n) AS type
, n.name AS name
, n.is_animation_studio AS is_animation_studio
SKIP $skip_count
LIMIT $BATCH_SIZE