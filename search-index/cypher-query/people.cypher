MATCH (n:People)
RETURN
'People/' + toString(n.id) AS id
, labels(n) AS type
, coalesce(n.name_full, n.name_first + ' ' + n.name_last )AS name
, n.description AS description
, n.siteUrl AS url
, n.image_medium AS image_url
, n.name_first AS name_first
, n.gender AS gender
, toString(n.dateOfBirth) AS dateOfBirth
, n.language AS language
, n.bloodType AS bloodType
, [ val in ([n.name_first,n.name_last,n.name_native,n.name_middle] + n.name_alternative)  WHERE val IS NOT NULL] AS synonyms
, n.homeTown AS homeTown
, n.primaryOccupations AS primaryOccupations
, n.yearsActive AS yearsActive
, n.age AS age
SKIP $skip_count
LIMIT $BATCH_SIZE
