CALL {
    MATCH (l:Link)<--(n:Anime)
    WHERE n.title IS NOT NULL
    RETURN
    n.title AS title
    , l.url AS url
    , n.synonyms AS aka
    , l.site AS site
    , l.site_type AS site_type
    , labels(n) AS label

    UNION

    MATCH (l:Link)<--(n:Character)
    WHERE n.name_full IS NOT NULL
    RETURN
    n.name_full AS title
    , l.url AS url
    , [val in [n.name_first, n.name_last, n.name_native] WHERE val IS NOT NULL] AS aka
    , "Anilist" AS site
    , "Info" AS site_type
    , labels(n) AS label
}
RETURN
count(url) as count_
