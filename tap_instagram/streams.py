import copy
from datetime import datetime

import pendulum
import singer
from cached_property import cached_property
from facebook_business.adobjects.igmedia import IGMedia
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.instagraminsightsresult import InstagramInsightsResult
from tap_instagram.api import InstagramAPI

LOGGER = singer.get_logger()


class Stream:
    key_properties = []

    # Request params
    page_size = 100

    def __init__(self, name: str, api: InstagramAPI, stream_alias: str, catalog_entry: singer.catalog.CatalogEntry):
        self.name = name
        self._api = api
        self.stream_alias = stream_alias
        self._catalog_entry = catalog_entry

    def automatic_fields(self):
        fields = set()
        if self._catalog_entry:
            props = singer.metadata.to_map(self._catalog_entry.metadata)
            for breadcrumb, data in props.items():
                if len(breadcrumb) != 2:
                    continue # Skip root and nested metadata

                if data.get('inclusion') == 'automatic':
                    fields.add(breadcrumb[1])
        return fields

    def fields(self):
        fields = set()
        if self._catalog_entry:
            props = singer.metadata.to_map(self._catalog_entry.metadata)
            for breadcrumb, data in props.items():
                if len(breadcrumb) != 2:
                    continue # Skip root and nested metadata

                if data.get('selected') or data.get('inclusion') == 'automatic':
                    fields.add(breadcrumb[1])
        return fields

    def request_params(self):
        return {"limit": self.page_size}


class IncrementalStream(Stream):
    bookmark_key = None

    def __init__(self, state, **kwargs):
        super().__init__(**kwargs)
        self._state = state or {}

    def _get_bookmark(self):
        if self.bookmark_key is None:
            return None

        current_bookmark = singer.get_bookmark(self._state, self.name, self.bookmark_key)
        if current_bookmark is None:
            return None

        LOGGER.info("Found bookmark for stream %s: %s", self.name, current_bookmark)
        return pendulum.parse(current_bookmark)


class User(Stream):
    key_properties = ["id"]
    def __iter__(self):
        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]
            record = ig_account.api_get(fields=self.fields()).export_all_data()
            record["page_id"] = account["page_id"]
            # TODO: remove params from urls
            yield {"record": record}


class UserLifetimeInsights(Stream):
    key_properties =["business_account_id", "metric"]
    period = [InstagramInsightsResult.Period.lifetime]
    metrics = ["audience_city", "audience_country", "audience_gender_age", "audience_locale"]

    def __iter__(self):
        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]
            for metric in ig_account.get_insights(params=self.request_params()):
                yield {
                    "record": {
                    "page_id": account["page_id"],
                    "business_account_id": ig_account.get("id"),
                    "metric": metric["name"],
                    "date": metric["values"][0]["end_time"],
                    "value": metric["values"][0]["value"],
                }}

    def request_params(self):
        params = super().request_params()
        params.update({
            "metric": self.metrics,
            "period": self.period
        })
        return params


class UserInsights(IncrementalStream):
    key_properties = ["business_account_id", "date"]
    bookmark_key = "date"
    buffer_days = 30

    period_to_metrics = {
        InstagramInsightsResult.Period.day: [
            "email_contacts",
            "follower_count",
            "get_directions_clicks",
            "impressions",
            "phone_call_clicks",
            "profile_views",
            "reach",
            "text_message_clicks",
            "website_clicks",
        ],
        InstagramInsightsResult.Period.week: [
            "impressions", "reach"
        ],
        InstagramInsightsResult.Period.days_28: [
            "impressions", "reach"
        ],
        InstagramInsightsResult.Period.lifetime: ["online_followers"]
    }

    def build_range(self):
        start_date = self._get_bookmark()
        min_start_date = pendulum.today().subtract(days=self.buffer_days)
        if start_date is not None:
            if start_date < min_start_date:
                LOGGER.warning("Start date is earlier than %s days from today, force using %s", self.buffer_days, min_start_date)
                start_date = min_start_date
        else:
            LOGGER.info("Get insight data since %s days ago until now", self.buffer_days)
            start_date = min_start_date
        end_date = pendulum.today()
        return start_date, end_date

    def request_params(self):
        params = super().request_params()
        since, until = self.build_range()
        LOGGER.info("Query range: (%s, %s)", since, until)
        params.update({
            "since": since.to_datetime_string(),
            "until": until.to_datetime_string()
        })
        return params

    def __iter__(self):
        base_params = self.request_params()

        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]

            metrics_by_day = {}
            for period, metrics in self.period_to_metrics.items():
                params = {
                    **base_params,
                    "period": [period],
                    "metric": metrics
                }
                insights = ig_account.get_insights(params=params)
                for metric in insights:
                    key = metric["name"]
                    if period in ["week", "days_28"]:
                        key += '_{}'.format(period)
                    for value in metric["values"]:
                        end_time = value["end_time"]

                        if end_time not in metrics_by_day:
                            metrics_by_day[end_time] = {}
                        metrics_by_day[end_time][key] = value["value"]
            
            for end_time in sorted(metrics_by_day):
                record = {
                    **metrics_by_day[end_time],
                    "page_id": account["page_id"],
                    "business_account_id": ig_account.get("id"),
                }
                record[self.bookmark_key] = end_time
                yield {"record": record}

        yield {"state": singer.write_bookmark(self._state, self.name, self.bookmark_key, base_params["until"])}
