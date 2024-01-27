MATCH (n:Anime)
RETURN
'anime/' + toString(n.id) AS id
, labels(n) AS type
, n.title AS name
, n.description AS description
, n.trending AS trending
, n.favourites AS favourites
, toString(n.endDate) AS endDate
, n.meanScore AS meanScore
, n.synonyms AS synonyms
, n.format AS format
, n.source AS source
, n.averageScore AS averageScore
, n.duration AS duration
, n.seasonInt AS seasonInt
, n.genres AS genres
, n.popularity AS popularity
, n.season AS season
, n.countryOfOrigin AS countryOfOrigin
, n.isAdult AS isAdult
, toString(n.startDate) AS startDate
, n.seasonYear AS seasonYear
, n.episodes AS episodes
SKIP $skip_count
LIMIT $BATCH_SIZE
