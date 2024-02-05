MATCH (n:Anime)
CALL {
    WITH n
    MATCH (n)<-[r:PARTICIPATE]-(c:Character)
    WHERE r.role = "MAIN"
    WITH c LIMIT $ADJACENT_SIZE
    RETURN {
        id:c.id,
        name:c.name_full,
        url:c.site_url,
        image:c.image
    } AS relate
}
RETURN 'anime/' + toString(n.id) AS id
, {
    type: 'Main Character',
    item: collect(relate)
} AS relates

SKIP $skip_count
LIMIT $BATCH_SIZE
