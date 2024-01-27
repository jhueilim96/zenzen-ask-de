MATCH (n:Character)
RETURN
'character/' + toString(n.id) AS id
, labels(n) AS type
, n.name_full AS name
, n.description AS description
, n.site_url as url
, n.image as image_url
, toString(n.last_updated) AS last_updated
, n.gender AS gender
, toString(n.date_of_birth) AS date_of_birth
, [ val in ([n.name_first,n.name_last,n.name_native,n.name_middle] + n.name_alternative)  WHERE val IS NOT NULL] AS synonyms
, n.blood_type AS blood_type
, n.age AS age
SKIP $skip_count
LIMIT $BATCH_SIZE