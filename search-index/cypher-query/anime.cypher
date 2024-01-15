
MATCH (n:Anime)
RETURN
toString(n.id) AS id
, labels(n) AS type

, n.title AS name  
, n.description AS description  