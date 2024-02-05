from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue
)

from pathlib import Path
import time
import httpx
import queue

from dagster_duckdb import DuckDBResource

URL = 'https://graphql.anilist.co'
TEMPLATE_PATH = Path().cwd() / 'ani_dagster/assets/anilist_api/graphql-template'

def get_graphql_template(template_path)->str:
    with open(str(template_path), 'r') as fp:
        return fp.read()

def make_api_call(url:str, query:str, variables:dict, retry:int=2):
    while retry > 0:
        resp = httpx.post(url, json={'query': query, 'variables': variables})
        if resp.status_code == 200:
            result =  resp.json()
            print(f'Remaing API call per min: {resp.headers.get("x-ratelimit-remaining")}')
            return result
        elif resp.status_code == 429:
            print(f'Too many request. Retry for 30 seconds')
            time.sleep(30)
            retry = retry - 1
        else:
            print(f"Failed to fetch data. Status code: {resp.status_code}")
            raise resp.raise_for_status()

    return False



@asset(compute_kind='python', io_manager_key='q_json_fs')
def get_anime(
    context: AssetExecutionContext,
) -> queue.Queue :

    log = get_dagster_logger()
    graphql_filename = 'get-anime.graphql'
    template = get_graphql_template(TEMPLATE_PATH / graphql_filename)

    MAX_PER_PAGE = 5
    TOP = 20
    total_api_call_count = int(TOP / MAX_PER_PAGE)
    q = queue.Queue()

    for page_num in range(1, total_api_call_count+1):
        variables = {
            'page': page_num,
            'perPage': MAX_PER_PAGE
        }
        result = make_api_call(URL, template, variables)
        data = result.get('data')
        current_page = result.get('data').get('Page').get('pageInfo').get('currentPage')
        q.put((data, f"anime/page-{current_page}"))
        log.info(f"Compeleted request page {current_page} at {URL}")

    context.add_output_metadata({"Size": q.qsize() })
    return q

@asset(compute_kind='python', deps=[get_anime])
def make_anime_seed(duck:DuckDBResource)->MaterializeResult:

    storage_path = Path().cwd().parent / 'raw/anilist/anime/*.json'

    with duck.get_connection() as conn:

        df_page = conn.read_json(str(storage_path), format='auto')

        df_media = conn.sql("""
            WITH df_page_media AS (
                SELECT
                    UNNEST(
                    (json_extract(df_page, '$.Page.media'))::JSON[]
                    ) media
                FROM df_page
            )
            SELECT
                d->'media'->>'id' media_id
                ,d->'media'->'title'->>'romaji' title_romaji
                ,d->'media'->'title'->>'romaji' title_english
                ,d->'media'->'title'->>'romaji' title_native
                ,d->'media'->>'type' "type"
                ,d->'media'->>'format' format
                ,d->'media'->>'description' "description"
                ,d->'media'->'startDate'->>'year' "start_date_year"
                ,d->'media'->'startDate'->>'month' "start_date_month"
                ,d->'media'->'startDate'->>'day' "start_date_day"
                ,d->'media'->'endDate'->>'year' "end_date_year"
                ,d->'media'->'endDate'->>'month' "end_date_month"
                ,d->'media'->'endDate'->>'day' "end_date_day"
                ,d->'media'->>'season' "season"
                ,d->'media'->>'seasonYear' "season_year"
                ,d->'media'->>'seasonInt' "season_int"
                ,d->'media'->>'episodes' "episodes"
                ,d->'media'->>'duration' "duration"
                ,d->'media'->>'volumes' "volumes"
                ,d->'media'->>'countryOfOrigin' "country_of_origin"
                ,d->'media'->>'source' "source"
                ,(d->'media'->>'averageScore')::INT "averageScore"
                ,(d->'media'->>'meanScore')::INT "meanScore"
                ,(d->'media'->>'popularity')::INT64 "popularity"
                ,(d->'media'->>'trending')::INT "trending"
                ,(d->'media'->>'favourites')::INT "favourites"
                ,(d->'media'->>'isAdult')::BOOLEAN "isAdult"
                ,(d->'media'->'genres')::VARCHAR[] "genres"
                ,(d->'media'->'synonyms')::VARCHAR[] "synonyms"
                ,(d->'media'->'tags')::JSON[] "tags"
                ,(d->'media'->'externalLinks')::JSON[] "external_links"
            FROM df_page_media d
        """)

        conn.sql("""
        DROP TABLE IF EXISTS anime_seed;
        CREATE TABLE anime_seed AS
        SELECT DISTINCT media_id, title_english
        FROM df_media
        """)

        seed_count = conn.sql('select count(*) from anime_seed').fetchone()[0]
        seed_preview = conn.sql('select * from anime_seed limit 3').df()

    return MaterializeResult(
        metadata={
            "Row Count": seed_count,
            "Preview": MetadataValue.md(seed_preview.to_markdown()),
        }
    )

@asset(compute_kind='python', io_manager_key='q_json_fs', deps=[make_anime_seed])
def get_anime_character(
    context: AssetExecutionContext,
    duck: DuckDBResource
)->queue.Queue:

    MAX_PER_PAGE = 25
    remaining_request = 90
    q = queue.Queue()
    log = get_dagster_logger()

    graphql_filename = 'get-anime-character.graphql'
    template = get_graphql_template(TEMPLATE_PATH / graphql_filename)

    with duck.get_connection() as conn:
        rows = conn.sql("SELECT media_id FROM anime_seed").fetchall()

    for row in rows:
        id = row[0]
        has_next_page = True
        current_page = 1

        while has_next_page:

            variables = {
                'page': current_page,
                'perPage': MAX_PER_PAGE,
                'media_id': id
            }

            if remaining_request < 10:
                log.info("Pause to recover limit")
                time.sleep(30)

            log.info(f"Requesting page {current_page} for {id}")
            result = make_api_call(URL, template, variables)
            q.put((result, f"character/{id}-{current_page}"))

            has_next_page = result.get('data').get('Media').get('characters').get('pageInfo').get('hasNextPage')
            if has_next_page:
                current_page += 1
            else:
                log.info(f'{id} has completed all pagination')
                break

    context.add_output_metadata(metadata={
        "Character JSON Count": q.qsize(),
    })

    return q

@asset(compute_kind='python', io_manager_key='q_json_fs', deps=[make_anime_seed])
def get_anime_staff(
    context: AssetExecutionContext,
    duck: DuckDBResource
)->queue.Queue:

    MAX_PER_PAGE = 25
    remaining_request = 90
    q = queue.Queue()
    log = get_dagster_logger()

    graphql_filename = 'get-anime-staff.graphql'
    template = get_graphql_template(TEMPLATE_PATH / graphql_filename)

    with duck.get_connection() as conn:
        rows = conn.sql("SELECT media_id FROM anime_seed").fetchall()

    for row in rows:
        id = row[0]
        has_next_page = True
        current_page = 1

        while has_next_page:

            variables = {
                'page': current_page,
                'perPage': MAX_PER_PAGE,
                'media_id': id
            }

            if remaining_request < 20:
                log.info("Pause to recover limit")
                time.sleep(30)

            log.info(f"Requesting page {current_page} for {id}")
            result = make_api_call(URL, template, variables)

            if not result:
                context.add_output_metadata(metadata={
                    "Staff JSON Count": q.qsize(),
                })
                return q

            q.put((result, f"staff/{id}-{current_page}"))
            has_next_page = result.get('data').get('Media').get('staff').get('pageInfo').get('hasNextPage')
            if has_next_page:
                current_page += 1

        log.info(f'{id} has completed all pagination')

    context.add_output_metadata(metadata={
        "Staff JSON Count": q.qsize(),
    })
    return q

@asset(compute_kind='python', io_manager_key='q_json_fs', deps=[make_anime_seed])
def get_anime_studio(
    context: AssetExecutionContext,
    duck: DuckDBResource
)->queue.Queue:

    MAX_API_PER_MIN = 30
    DURATION_PER_API_CALL = round(60 / MAX_API_PER_MIN, 1)
    q = queue.Queue()
    log = get_dagster_logger()

    graphql_filename = 'get-anime-studio.graphql'
    template = get_graphql_template(TEMPLATE_PATH / graphql_filename)

    with duck.get_connection() as conn:
        rows = conn.sql("SELECT media_id FROM anime_seed").fetchall()

    for row in rows:
        id = row[0]
        start_time = time.time()
        variables = {
            'media_id': id
        }
        resp = httpx.post(URL, json={'query': template, 'variables': variables})
        if resp.status_code != 200:
            print(f"Request fails at {id}")
            raise resp.raise_for_status()

        result = resp.json()
        log.info(f"Completed for staff {id}")
        q.put((result, f"studio/{id}"))

        remaining_request = int(resp.headers.get('x-ratelimit-remaining'))
        print(f'Remaining API call per min: {remaining_request}')
        duration_ops = time.time() - start_time
        duration_to_waiting = DURATION_PER_API_CALL - duration_ops

        if duration_to_waiting > 0:
            time.sleep(duration_to_waiting)

    context.add_output_metadata(metadata={
        "Studio JSON Count": q.qsize(),
    })
    return q
