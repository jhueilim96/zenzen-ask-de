import feedparser
import httpx
import json
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from dagster_duckdb import DuckDBResource

from ani_dagster.resources.rss import RssRegistry


@asset(compute_kind="python")
def rss_chruncyroll(
    context: AssetExecutionContext, rss_registry: RssRegistry
) -> feedparser.util.FeedParserDict:
    site_detail = rss_registry.retrive_sites("chruncyroll")
    main_rss_url = site_detail.get("url")
    res = httpx.get(main_rss_url)

    if res.status_code != 200:
        res.raise_for_status()

    xml_content = res.content.decode("utf-8")
    post = feedparser.parse(xml_content)
    last_updated = post.feed.published
    context.add_output_metadata(metadata={"last_update": last_updated})
    return post


@asset(compute_kind="python")
def load_chruncyroll(
    rss_chruncyroll: feedparser.util.FeedParserDict,
    duck: DuckDBResource,
    rss_registry: RssRegistry,
) -> None:
    site_name = rss_registry.retrive_sites("chruncyroll").get("name")
    with duck.get_connection() as conn:
        conn.sql(
            """create table if not exists rss_bronze
            (content varchar, site varchar, retrieved_at timestamp, content_hash UBIGINT)
            """
        )
        json_d = json.dumps(rss_chruncyroll)
        conn.execute(
            """with prep as (select ? as content, ? as site, current_timestamp)
            insert into rss_bronze from ( select * , hash(content) from prep)
            """,
            parameters=[json_d, site_name],
        )


@asset(compute_kind="python")
def rss_animenewsnetwork(
    context: AssetExecutionContext, rss_registry: RssRegistry
) -> feedparser.util.FeedParserDict:
    site_detail = rss_registry.retrive_sites("animenewsnetwork")
    main_rss_url = site_detail.get("url")
    res = httpx.get(main_rss_url)

    if res.status_code != 200:
        res.raise_for_status()

    xml_content = res.content.decode("utf-8")
    post = feedparser.parse(xml_content)
    last_updated = post.feed.updated
    context.add_output_metadata(metadata={"last_update": last_updated})
    return post


@asset(compute_kind="python")
def load_animenewsnetwork(
    rss_animenewsnetwork: feedparser.util.FeedParserDict,
    duck: DuckDBResource,
    rss_registry: RssRegistry,
) -> None:
    site_name = rss_registry.retrive_sites("animenewsnetwork").get("name")
    with duck.get_connection() as conn:
        conn.sql(
            """create table if not exists rss_bronze
            (content varchar, site varchar, retrieved_at timestamp, content_hash UBIGINT)
            """
        )
        json_d = json.dumps(rss_animenewsnetwork)
        conn.execute(
            """with prep as (select ? as content, ? as site, current_timestamp)
            insert into rss_bronze from ( select * , hash(content) from prep)
            """,
            parameters=[json_d, site_name],
        )


@asset(compute_kind="python")
def rss_otakukart(
    context: AssetExecutionContext, rss_registry: RssRegistry
) -> feedparser.util.FeedParserDict:
    site_detail = rss_registry.retrive_sites("otakukart")
    main_rss_url = site_detail.get("url")
    res = httpx.get(main_rss_url)

    if res.status_code != 200:
        res.raise_for_status()

    xml_content = res.content.decode("utf-8")
    post = feedparser.parse(xml_content)
    last_updated = post.feed.updated
    context.add_output_metadata(metadata={"last_update": last_updated})
    return post


@asset(compute_kind="python")
def load_otakukart(
    rss_otakukart: feedparser.util.FeedParserDict,
    duck: DuckDBResource,
    rss_registry: RssRegistry,
) -> None:
    site_name = rss_registry.retrive_sites("otakukart").get("name")
    with duck.get_connection() as conn:
        conn.sql(
            """create table if not exists rss_bronze
            (content varchar, site varchar, retrieved_at timestamp, content_hash UBIGINT)
            """
        )
        json_d = json.dumps(rss_otakukart)
        conn.execute(
            """with prep as (select ? as content, ? as site, current_timestamp)
            insert into rss_bronze from ( select * , hash(content) from prep)
            """,
            parameters=[json_d, site_name],
        )
