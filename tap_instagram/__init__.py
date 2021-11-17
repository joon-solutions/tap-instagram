#!/usr/bin/env python3
import json
import os
import sys
from typing import Iterator

import singer
from singer import Transformer, metrics, utils
from singer import metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from tap_instagram.api import InstagramAPI
from tap_instagram.common import InstagramTapException
from tap_instagram import streams as insta_streams

REQUIRED_CONFIG_KEYS = ["access_token"]
LOGGER = singer.get_logger()
STREAM_CLS = {
    "users": insta_streams.Users,
    "user_lifetime_insights": insta_streams.UserLifetimeInsights,
    "user_insights": insta_streams.UserInsights,
    "media": insta_streams.Media,
    "media_insights": insta_streams.MediaInsights,
    "stories": insta_streams.Stories,
    "story_insights": insta_streams.StoryInsights,
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """Load schemas from schemas folder"""
    schemas = {}
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path, "r", encoding="utf-8") as file:
            schemas[file_raw] = json.load(file)
    return schemas


def load_schema_by_stream(stream):
    path = get_abs_path("schemas/{}.json".format(stream.name))
    schema = utils.load_json(path)
    return schema


def init_stream(api, catalog_entry, state) -> insta_streams.Stream:
    name = catalog_entry.stream
    stream_alias = catalog_entry.stream_alias

    if name in STREAM_CLS:
        stream_cls = STREAM_CLS[name]
        if issubclass(stream_cls, insta_streams.IncrementalStream):
            return stream_cls(
                state, name=name, api=api, stream_alias=stream_alias, catalog_entry=catalog_entry
            )
        return stream_cls(name, api, stream_alias, catalog_entry)
    raise InstagramTapException("Stream {} not available".format(name))


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_cls = STREAM_CLS[stream_id]
        stream_metadata = metadata.to_list(metadata.to_map(metadata.get_standard_metadata(schema)))
        # Auto select all fields
        for item in stream_metadata:
            item["metadata"]["selected"] = True
        streams.append(
            {
                "stream": stream_id,
                "tap_stream_id": stream_id,
                "stream_alias": stream_id,
                "schema": schema,
                "metadata": stream_metadata,
                "key_properties": stream_cls.key_properties or [],
                "replication-key": stream_cls.bookmark_key or None,
            }
        )
    return {"streams": streams}


def get_selected_streams(api, catalog, state) -> Iterator[insta_streams.Stream]:
    for avail_stream in STREAM_CLS:
        catalog_entry = next((s for s in catalog.streams if s.tap_stream_id == avail_stream), None)
        if catalog_entry:
            yield init_stream(api, catalog_entry, state)


def sync(config, state, catalog: singer.Catalog):
    """Sync data from tap source"""
    LOGGER.info("Start to sync")
    LOGGER.info("State: %s", state)
    api = InstagramAPI(config["access_token"])

    for stream in get_selected_streams(api, catalog, state):
        LOGGER.info("Syncing stream:%s", stream.name)
        schema = load_schema_by_stream(stream)
        bookmark_properties = [stream.bookmark_key] if stream.bookmark_key else []

        singer.write_schema(
            stream_name=stream.name,
            schema=schema,
            key_properties=stream.key_properties,
            bookmark_properties=bookmark_properties,
            stream_alias=stream.stream_alias,
        )

        with Transformer() as transformer:
            with metrics.record_counter(stream.name) as counter:
                for message in stream:
                    # place type conversions or transformations here
                    if "record" in message:
                        counter.increment()
                        time_extracted = utils.now()
                        record = transformer.transform(message["record"], schema)
                        singer.write_record(
                            stream.name, record, stream.stream_alias, time_extracted
                        )
                    elif "state" in message:
                        singer.write_state(message["state"])
                    else:
                        raise InstagramTapException("Message invalid: {}".format(message))


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        json.dump(catalog, sys.stdout, indent=4)
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
