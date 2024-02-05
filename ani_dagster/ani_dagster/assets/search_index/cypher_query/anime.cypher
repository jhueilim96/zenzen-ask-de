MATCH (n:Anime)
RETURN
'anime/' + toString(n.id) AS id
, n.title AS title
, n.synonyms AS alternative
, labels(n) AS type
, n.description AS description
SKIP $skip_count
LIMIT $BATCH_SIZE
