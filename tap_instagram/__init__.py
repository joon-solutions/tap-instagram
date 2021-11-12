#!/usr/bin/env python3
import os
import json
from typing import Iterator
import singer
from singer import utils, Transformer, metrics
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_instagram.streams import User, Stream, UserLifetimeInsights, UserInsights
from tap_instagram.common import InstagramTapException
from tap_instagram.api import InstagramAPI


REQUIRED_CONFIG_KEYS = ["access_token"]
LOGGER = singer.get_logger()
STREAMS = ["user", "user_lifetime_insights", "user_insights"]

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def load_schema_by_stream(stream):
    path = get_abs_path("schemas/{}.json".format(stream.name))
    schema = utils.load_json(path)
    return schema


def init_stream(api, catalog_entry, state):
    name = catalog_entry.stream
    stream_alias = catalog_entry.stream_alias

    if name == "user":
        return User(name, api, stream_alias, catalog_entry)
    if name == "user_lifetime_insights":
        return UserLifetimeInsights(name, api, stream_alias, catalog_entry)
    if name =="user_insights":
        return UserInsights(state, name=name, api=api, stream_alias=stream_alias, catalog_entry=catalog_entry)
    else:
        raise InstagramTapException("Stream {} not available".format(name))


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def get_selected_streams(api, catalog, state) -> Iterator[Stream]:
    for avail_stream in STREAMS:
        catalog_entry = next((s for s in catalog.streams if s.tap_stream_id == avail_stream), None)
        if catalog_entry:
            yield init_stream(api, catalog_entry, state)
    

def sync(config, state, catalog: singer.Catalog):
    """ Sync data from tap source """
    LOGGER.info("Start to sync")
    LOGGER.info("State: %s", state)
    api = InstagramAPI(config["access_token"])

    # Loop over selected streams in catalog
    for stream in get_selected_streams(api, catalog, state):
        LOGGER.info("Syncing stream:" + stream.name)
        schema = load_schema_by_stream(stream)

        singer.write_schema(
            stream_name=stream.name,
            schema=schema,
            key_properties=stream.key_properties,
            bookmark_properties=None,
            stream_alias=stream.stream_alias
        )

        # max_bookmark = None
        with Transformer() as transformer:
            with metrics.record_counter(stream.name) as counter:
                for message in stream:
                    # place type conversions or transformations here
                    if "record" in message:
                        counter.increment()
                        time_extracted = utils.now()
                        record = transformer.transform(message["record"], schema)
                        singer.write_record(stream.name, record, stream.stream_alias, time_extracted)
                    elif "state" in message:
                        singer.write_state(message["state"])
                    else:
                        raise InstagramTapException("Message invalid: {}".format(message))
    return


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
