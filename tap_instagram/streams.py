import pendulum
import singer
from facebook_business.adobjects.igmedia import IGMedia
from facebook_business.adobjects.instagraminsightsresult import InstagramInsightsResult
from facebook_business.exceptions import FacebookRequestError

from tap_instagram.api import InstagramAPI
from tap_instagram.common import remove_params_from_url

LOGGER = singer.get_logger()


class Stream:
    base_properties = set()
    key_properties = []
    bookmark_key = None

    # Request params
    page_size = 100

    def __init__(
        self,
        name: str,
        api: InstagramAPI,
        stream_alias: str,
        catalog_entry: singer.catalog.CatalogEntry,
    ):
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
                    continue  # Skip root and nested metadata

                if data.get("inclusion") == "automatic":
                    fields.add(breadcrumb[1])
        return fields.difference(self.base_properties)

    def fields(self):
        fields = set()
        if self._catalog_entry:
            props = singer.metadata.to_map(self._catalog_entry.metadata)
            for breadcrumb, data in props.items():
                if len(breadcrumb) != 2:
                    continue  # Skip root and nested metadata

                if data.get("selected") or data.get("inclusion") == "automatic":
                    fields.add(breadcrumb[1])
        return fields.difference(self.base_properties)

    def request_params(self):
        return {"limit": self.page_size}

    @classmethod
    def make_record(cls, record):
        record = cls.clean_url(record)
        return {"record": record}

    @classmethod
    def make_state(cls, state):
        return {"state": state}

    @staticmethod
    def clean_url(record):
        if record.get("media_url"):
            record["media_url"] = remove_params_from_url(
                record["media_url"], params=["_nc_sid", "_nc_cat", "ccb"]
            )
        if record.get("profile_picture_url"):
            record["profile_picture_url"] = remove_params_from_url(
                record["profile_picture_url"], params=[["_nc_sid", "_nc_cat", "ccb"]]
            )
        return record


class IncrementalStream(Stream):
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


class Users(Stream):
    """
    Stream for basic Instagram User data. This stream has no primary key to allow
    track history.
    """

    base_properties = {"page_id"}

    def __iter__(self):
        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]
            record = ig_account.api_get(fields=self.fields()).export_all_data()
            record["page_id"] = account["page_id"]
            yield self.make_record(record)


class UserLifetimeInsights(Stream):
    base_properties = {"page_id", "business_account_id", "metric", "date", "value"}
    key_properties = ["business_account_id", "metric", "date"]
    period = [InstagramInsightsResult.Period.lifetime]
    metrics = ["audience_city", "audience_country", "audience_gender_age", "audience_locale"]

    def __iter__(self):
        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]
            for metric in ig_account.get_insights(params=self.request_params()):
                yield self.make_record(
                    {
                        "page_id": account["page_id"],
                        "business_account_id": ig_account.get("id"),
                        "metric": metric["name"],
                        "date": metric["values"][0]["end_time"],
                        "value": metric["values"][0]["value"],
                    }
                )

    def request_params(self):
        params = super().request_params()
        params.update({"metric": self.metrics, "period": self.period})
        return params


class UserInsights(IncrementalStream):
    base_properties = {"page_id", "business_account_id"}
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
        InstagramInsightsResult.Period.week: ["impressions", "reach"],
        InstagramInsightsResult.Period.days_28: ["impressions", "reach"],
        InstagramInsightsResult.Period.lifetime: ["online_followers"],
    }

    def build_range(self):
        start_date = self._get_bookmark()
        min_start_date = pendulum.today().subtract(days=self.buffer_days)
        if start_date is not None:
            if start_date < min_start_date:
                LOGGER.warning(
                    "Start date is earlier than %s days from today, force using %s",
                    self.buffer_days,
                    min_start_date,
                )
                start_date = min_start_date
        else:
            LOGGER.info("Get insight data since %s days ago until now", self.buffer_days)
            start_date = min_start_date
        # Instagram data can be delayed up to 48 hours and is calculated at 7:00 or 8:00 AM daily
        end_date = pendulum.today(-1)
        return min(start_date, end_date), end_date

    def request_params(self):
        params = super().request_params()
        since, until = self.build_range()
        LOGGER.info("Query range: (%s, %s)", since, until)
        params.update({"since": since.to_datetime_string(), "until": until.to_datetime_string()})
        return params

    def __iter__(self):
        base_params = self.request_params()

        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]

            metrics_by_day = {}
            for period, metrics in self.period_to_metrics.items():
                params = {**base_params, "period": [period], "metric": metrics}
                insights = ig_account.get_insights(params=params)
                for metric in insights:
                    key = metric["name"]
                    if period in ["week", "days_28"]:
                        key += "_{}".format(period)
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
                yield self.make_record(record)

        yield self.make_state(
            singer.write_bookmark(self._state, self.name, self.bookmark_key, base_params["until"])
        )


class Media(Stream):
    """Children objects can only be of the media_type == "CAROUSEL_ALBUM".
    And children object does not support invalid_children_fields fields,
    so they are excluded when trying to get child objects to avoid the error.
    No primary key for this stream to allow tracking history.
    """

    base_properties = {"page_id", "business_account_id"}
    invalid_children_fields = [
        "caption",
        "comments_count",
        "is_comment_enabled",
        "like_count",
        "children",
    ]

    def __iter__(self):
        params = self.request_params()

        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]
            LOGGER.info(self.fields())
            media = ig_account.get_media(params=params, fields=self.fields())

            for row in media:
                record = row.export_all_data()
                if record.get("children"):
                    record["children"] = [
                        self.get_child(child["id"]) for child in record["children"]["data"]
                    ]

                record.update(
                    {"page_id": account["page_id"], "business_account_id": ig_account.get("id")}
                )
                yield self.make_record(record)

    def get_child(self, child_id):
        fields = list(set(self.fields()).difference(set(self.invalid_children_fields)))
        media_obj = IGMedia(child_id)
        record = media_obj.api_get(fields=fields).export_all_data()
        return record


class MediaInsights(Stream):
    base_properties = {"id", "page_id", "business_account_id"}
    metrics = ["engagement", "impressions", "reach", "saved"]
    carousel_album_metrics = [
        "carousel_album_engagement",
        "carousel_album_impressions",
        "carousel_album_reach",
        "carousel_album_saved",
        "carousel_album_video_views",
    ]

    def __iter__(self):
        params = self.request_params()

        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]
            account_id = ig_account.get("id")
            media = ig_account.get_media(params=params, fields=["media_type"])
            for ig_media in media:
                insights = self.get_insights(ig_media, account_id)
                if insights is None:
                    break

                insights.update(
                    {
                        "id": ig_media["id"],
                        "page_id": account["page_id"],
                        "business_account_id": account_id,
                    }
                )
                yield self.make_record(insights)

    def get_insights(self, ig_media: IGMedia, account_id):
        media_type = ig_media.get("media_type")
        if media_type == "VIDEO":
            metrics = self.metrics + ["video_views"]
        elif media_type == "CAROUSEL_ALBUM":
            metrics = self.carousel_album_metrics
        else:
            metrics = self.metrics

        try:
            insights = ig_media.get_insights(params={"metric": metrics})
            return {record.get("name"): record.get("values")[0]["value"] for record in insights}
        except FacebookRequestError as error:
            # An error might occur if the media was posted before the most recent time that
            # the user's account was converted to a business account from a personal account
            if error.api_error_subcode() == 2108006:
                details = (
                    error.body().get("error", {}).get("error_user_title")
                    or error.api_error_message()
                )
                LOGGER.error("Insights error for business_account_id %s: %s", account_id, details)
                # We receive all Media starting from the last one, and if on the next Media
                # we get an Insight error, then no reason to make inquiries for each Media further,
                #  since they were published even earlier.
                return None
            raise error


class Stories(Stream):
    base_properties = {"page_id", "business_account_id"}

    def __iter__(self):
        params = self.request_params()

        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]
            stories = ig_account.get_stories(params=params, fields=self.fields())
            LOGGER.info("Num stories: %s", len(stories))
            for story in stories:
                record = story.export_all_data()
                record.update(
                    {"page_id": account["page_id"], "business_account_id": ig_account.get("id")}
                )
                yield self.make_record(record)


class StoryInsights(Stream):
    base_properties = {"page_id", "business_account_id"}
    metrics = ["exits", "impressions", "reach", "replies", "taps_forward", "taps_back"]

    def __iter__(self):
        params = self.request_params()

        for account in self._api.accounts:
            ig_account = account["instagram_business_account"]
            stories = ig_account.get_stories(params=params, fields=[])
            for story in stories:
                insights = self.get_insights(story)
                if not insights:
                    continue

                insights.update(
                    {
                        "id": story["id"],
                        "page_id": account["page_id"],
                        "business_account_id": ig_account.get("id"),
                    }
                )
                yield self.make_record(insights)

    def get_insights(self, story: IGMedia) -> dict:
        # Story IG Media object metrics with values less than 5 will return an error code 10
        # with the message (#10) Not enough viewers for the media to show insights.
        try:
            insights = story.get_insights(params={"metric": self.metrics})
            return {record["name"]: record["values"][0]["value"] for record in insights}
        except FacebookRequestError as error:
            if error.api_error_code() == 10:
                LOGGER.error("Insights error: %s", error.api_error_message())
                return {}
            raise error
