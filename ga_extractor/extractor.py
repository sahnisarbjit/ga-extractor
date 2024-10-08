import json
import uuid

import typer
import validators
from googleapiclient.discovery import build
from google.oauth2 import service_account
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
from typing import Optional, NamedTuple
from urllib.parse import urlparse

extractor = typer.Typer()
APP_NAME = "ga-extractor"
APP_CONFIG_FILE = "config.yaml"

class SamplingLevel(str, Enum):
    SAMPLING_UNSPECIFIED = "SAMPLING_UNSPECIFIED"
    DEFAULT = "DEFAULT"
    SMALL = "SMALL"
    LARGE = "LARGE"


class OutputFormat(str, Enum):
    JSON = "JSON"
    CSV = "CSV"
    UMAMI = "UMAMI"

    @staticmethod
    def file_suffix(f):
        format_mapping = {
            OutputFormat.JSON: "json",
            OutputFormat.CSV: "csv",
            OutputFormat.UMAMI: "sql",
        }
        return format_mapping[f]


class Preset(str, Enum):
    NONE = "NONE"
    FULL = "FULL"
    BASIC = "BASIC"

    @staticmethod
    def metrics(p):
        metrics_mapping = {
            Preset.NONE: [],
            Preset.FULL: ["ga:pageviews", "ga:sessions"],
            Preset.BASIC: ["ga:pageviews"],
        }
        return metrics_mapping[p]

    @staticmethod
    def dims(p):
        dims_mapping = {
            Preset.NONE: [],
            Preset.FULL: ["ga:pagePath", "ga:pageTitle", "ga:browser", "ga:operatingSystem", "ga:deviceCategory", "ga:browserSize",
                          "ga:dateHourMinute", "ga:countryIsoCode", "ga:fullReferrer"],
            Preset.BASIC: ["ga:pagePath"],
        }
        return dims_mapping[p]


@extractor.command()
def setup(metrics: str = typer.Option(None, "--metrics"),
          dimensions: str = typer.Option(None, "--dimensions"),
          sa_key_path: str = typer.Option(..., "--sa-key-path"),
          table_id: int = typer.Option(..., "--table-id"),
          sampling_level: SamplingLevel = typer.Option(SamplingLevel.DEFAULT, "--sampling-level"),
          preset: Preset = typer.Option(Preset.NONE, "--preset",
                                        help="Use metrics and dimension preset (can't be specified with '--dimensions' or '--metrics')"),
          start_date: datetime = typer.Option(..., formats=["%Y-%m-%d"]),
          end_date: datetime = typer.Option(..., formats=["%Y-%m-%d"]),
          dry_run: bool = typer.Option(False, "--dry-run", help="Outputs config to terminal instead of config file")):
    """
    Generate configuration file from arguments
    """

    if (
            (preset is Preset.NONE and dimensions is None and metrics is None) or
            (dimensions is None and metrics is not None) or (dimensions is not None and metrics is None)
    ):
        typer.echo("Dimensions and Metrics or Preset must be specified.")
        raise typer.Exit(2)

    config = {
        "serviceAccountKeyPath": sa_key_path,
        "table": table_id,
        "metrics": "" if not metrics else metrics.split(","),
        "dimensions": "" if not dimensions else dimensions.split(","),
        "samplingLevel": sampling_level.value,
        "startDate": f"{start_date:%Y-%m-%d}",
        "endDate": f"{end_date:%Y-%m-%d}",
    }

    if preset is not Preset.NONE:
        config["metrics"] = Preset.metrics(preset)
        config["dimensions"] = Preset.dims(preset)

    output = yaml.dump(config)
    if dry_run:
        typer.echo(output)
    else:
        app_dir = typer.get_app_dir(APP_NAME)
        config_path: Path = Path(app_dir) / APP_CONFIG_FILE
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, 'w') as outfile:
            outfile.write(output)


@extractor.command()
def auth():
    """
    Test authentication using generated configuration
    """
    app_dir = typer.get_app_dir(APP_NAME)
    config_path: Path = Path(app_dir) / APP_CONFIG_FILE
    if not config_path.is_file():
        typer.echo("Config file doesn't exist yet. Please run 'setup' command first.")
        return
    try:
        with config_path.open() as config:
            credentials = service_account.Credentials.from_service_account_file(yaml.safe_load(config)["serviceAccountKeyPath"])
            scoped_credentials = credentials.with_scopes(['openid'])
        with build('oauth2', 'v2', credentials=scoped_credentials) as service:
            user_info = service.userinfo().v2().me().get().execute()
            typer.echo(f"Successfully authenticated with user: {user_info['id']}")
    except BaseException as e:
        typer.echo(f"Authenticated failed with error: '{e}'")


@extractor.command()
def extract(report: Optional[Path] = typer.Option("report.json", dir_okay=True)):
    """
    Extracts data based on the config
    """
    # https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet

    app_dir = typer.get_app_dir(APP_NAME)
    config_path: Path = Path(app_dir) / APP_CONFIG_FILE
    output_path: Path = Path(app_dir) / report
    if not config_path.is_file():
        typer.echo("Config file doesn't exist yet. Please run 'setup' command first.")
        raise typer.Exit(2)
    with config_path.open() as file:
        config = yaml.safe_load(file)
        credentials = service_account.Credentials.from_service_account_file(config["serviceAccountKeyPath"])
        scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/analytics.readonly'])

        dimensions = [{"name": d} for d in config['dimensions']]
        metrics = [{"expression": m} for m in config['metrics']]
        body = {"reportRequests": [
                    {
                        # "pageSize": 2,  # Use this to test paging
                        "viewId": f"{config['table']}",
                        "dateRanges": [
                            {
                                "startDate": f"{config['startDate']}",
                                "endDate": f"{config['endDate']}"
                            }],
                        "dimensions": [dimensions],
                        "metrics": [metrics],
                        "samplingLevel": config['samplingLevel']
                    }]}
        rows = []
        with build('analyticsreporting', 'v4', credentials=scoped_credentials) as service:
            response = service.reports().batchGet(body=body).execute()
            if not "rows" in response.values():
                raise Exception("There were no rows in the response.")
            rows.extend(response["reports"][0]["data"]["rows"])

            while "nextPageToken" in response["reports"][0]:  # Paging...
                body["reportRequests"][0]["pageToken"] = response["reports"][0]["nextPageToken"]
                response = service.reports().batchGet(body=body).execute()
                rows.extend(response["reports"][0]["data"]["rows"])

            output_path.write_text(json.dumps(rows))
        typer.echo(f"Report written to {output_path.absolute()}")


@extractor.command()
def migrate(output_format: OutputFormat = typer.Option(OutputFormat.UMAMI, "--format"),
            umami_website_id: uuid.UUID = typer.Argument(uuid.uuid4(), help="Website UUID, used if migrating data for Umami Analytics"),
            umami_hostname: str = typer.Argument("localhost", help="Hostname website being migrated, used if migrating data for Umami Analytics")):
    """
    Export necessary data and transform it to format for target environment (Umami, ...)

    Old sessions won't be preserved because session can span multiple days, but extraction is done on daily level.

    Bounce rate and session duration won't be accurate.
    Views and visitors on day-level granularity will be accurate.
    Exact visit time is (hour and minute) is not preserved.
    """

    app_dir = typer.get_app_dir(APP_NAME)
    config_path: Path = Path(app_dir) / APP_CONFIG_FILE
    output_path: Path = Path(app_dir) / f"{umami_website_id}_extract.{OutputFormat.file_suffix(output_format)}"
    if not config_path.is_file():
        typer.echo("Config file doesn't exist yet. Please run 'setup' command first.")
        raise typer.Exit(2)
    with config_path.open() as file:
        config = yaml.safe_load(file)
        credentials = service_account.Credentials.from_service_account_file(config["serviceAccountKeyPath"])
        scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/analytics.readonly'])

        date_ranges = __migrate_date_ranges(config['startDate'], config['endDate'])
        rows = __migrate_extract(scoped_credentials, config['table'], date_ranges)

        if output_format == OutputFormat.UMAMI:
            data = __migrate_transform_umami(rows, umami_website_id, umami_hostname)
            with output_path.open(mode="w") as f:
                for insert in data:
                    f.write(f"{insert}\n")
                f.write(
                    f"WITH r AS (SELECT TO_CHAR(created_at, 'yyyy-mm-ddT00:00:00')::date as reset_time, website_id FROM public.website_event WHERE website_id = '{umami_website_id}' ORDER BY event_id DESC LIMIT 1) UPDATE public.website SET reset_at = r.reset_time, created_at = r.reset_time FROM r WHERE public.website.website_id = r.website_id;\n"
                )
        elif output_format == OutputFormat.JSON:
            output_path.write_text(json.dumps(rows))
        elif output_format == OutputFormat.CSV:
            data = __migrate_transform_csv(rows)
            with output_path.open(mode="w") as f:
                for row in data:
                    f.write(f"{row}\n")

        typer.echo(f"Report written to {output_path.absolute()}")


def __migrate_date_ranges(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    date_ranges = [{"startDate": f"{start_date + timedelta(days=d):%Y-%m-%d}",
                    "endDate": f"{start_date + timedelta(days=d):%Y-%m-%d}"} for d in
                   range(((end_date.date() - start_date.date()).days + 1))]
    return date_ranges


def __migrate_extract(credentials, table_id, date_ranges):
    dimensions = ["ga:pagePath", "ga:pageTitle", "ga:browser", "ga:operatingSystem", "ga:deviceCategory", "ga:browserSize", "ga:dateHourMinute", "ga:countryIsoCode", "ga:fullReferrer"]
    metrics = ["ga:pageviews", "ga:sessions"]

    body = {"reportRequests": [
        {
            "viewId": f"{table_id}",
            "dimensions": [{"name": d} for d in dimensions],
            "metrics": [{"expression": m} for m in metrics]
        }]}

    rows = {}
    for r in date_ranges:
        with build('analyticsreporting', 'v4', credentials=credentials) as service:
            body["reportRequests"][0]["dateRanges"] = [r]
            response = service.reports().batchGet(body=body).execute()
            num_rows = response["reports"][0]["data"]["totals"][0]["values"]
            if len(list(filter(lambda x: x != '0', num_rows))):
                rows[r["startDate"]] = response["reports"][0]["data"]["rows"]

    return rows


class Session(NamedTuple):
    session_id: uuid.UUID
    website_id: uuid.UUID
    created_at: str
    hostname: str
    browser: str
    os: str
    device: str
    screen: str
    country: str

    def sql(self):
        session_insert = (
            f"INSERT INTO public.session (session_id, website_id, created_at, hostname, browser, os, device, screen, country) "
            f"VALUES ('{self.session_id}', '{self.website_id}', '{self.created_at}', '{self.hostname[:100]}', '{_safe_db_value(self.browser, 20)}', '{_safe_db_value(self.os, 20)}', '{_safe_db_value(self.device, 20)}', '{_safe_db_value(self.screen, 10)}', '{self.country}');"
        )
        return session_insert


class WebsiteEvent(NamedTuple):
    id: uuid.UUID
    website_id: uuid.UUID
    session_id: uuid.UUID
    created_at: str
    url: str
    title: str
    referrer: str

    def sql(self):
        url_data = urlparse(self.url)
        referrer_data = urlparse(self.referrer)

        session_insert = (
            f"INSERT INTO public.website_event (event_id, website_id, session_id, created_at, url_path, url_query, referrer_path, referrer_query, referrer_domain, page_title, event_name, visit_id) "
            f"VALUES ('{self.id}', '{self.website_id}', '{self.session_id}', '{self.created_at}', '{_safe_db_value(url_data.path, 500)}', '{_safe_db_value(url_data.query, 500)}', '{_safe_db_value(referrer_data.path, 500)}', '{_safe_db_value(referrer_data.query, 500)}', '{_safe_db_value(referrer_data.hostname, 500)}', '{_safe_db_value(self.title, 500)}', 'pageview', '{uuid.uuid4()}');"
        )

        return session_insert


def __migrate_transform_umami(rows, website_id, hostname):

    # Sample row:
    # {'dimensions': ['/', 'Chrome', 'Windows', 'desktop', '1350x610', 'en-us', 'IN', '(direct)'], 'metrics': [{'values': ['1', '1']}]}
    #
    # Notes: there can be 0 sessions in the record; there's always more or equal number of views
    #        - treat zero sessions as one
    #        - if sessions is non-zero and page views are > 1, then divide, e.g.:
    #           - 5, 5 - 5 sessions, 1 view each
    #           - 4, 2 - 2 sessions, 2 views each
    #           - 5, 3 - 3 sessions, 2x1 view, 1x3 views

    sql_inserts = []
    for day, value in rows.items():
        for row in value:
            referrer = f"https://{row['dimensions'][8]}"
            if not validators.url(referrer):
                referrer = ""
            elif referrer == "google":
                referrer = "https://google.com"

            timestamp = _convert_ua_datetime(row["dimensions"][6])
            country = row["dimensions"][7]
            page_views, sessions = map(int, row["metrics"][0]["values"])
            sessions = max(sessions, 1)  # in case it's zero
            if page_views == sessions:  # One page view for each session
                for i in range(sessions):
                    session_id = uuid.uuid4()
                    s = Session(
                        session_id=session_id, website_id=website_id, created_at=timestamp, hostname=hostname,
                        browser=row["dimensions"][2], os=row["dimensions"][3], device=row["dimensions"][4],
                        screen=row["dimensions"][5], country=country
                    )
                    p = WebsiteEvent(
                        id=uuid.uuid4(), website_id=website_id, session_id=session_id, created_at=timestamp,
                        url=row["dimensions"][0], title=row["dimensions"][1], referrer=referrer
                    )
                    sql_inserts.extend([s.sql(), p.sql()])

            elif page_views % sessions == 0:  # Split equally
                for i in range(sessions):
                    session_id = uuid.uuid4()
                    s = Session(
                        session_id=session_id, website_id=website_id, created_at=timestamp, hostname=hostname,
                        browser=row["dimensions"][2], os=row["dimensions"][3], device=row["dimensions"][4],
                        screen=row["dimensions"][5], country=country
                    )
                    sql_inserts.append(s.sql())
                    for j in range(page_views // sessions):
                        p = WebsiteEvent(
                            id=uuid.uuid4(), website_id=website_id, session_id=session_id, created_at=timestamp,
                            url=row["dimensions"][0], title=row["dimensions"][1], referrer=referrer
                        )
                        sql_inserts.append(p.sql())
            else:  # One page view for each, rest for the last session
                last_session_id = None
                for i in range(sessions):
                    session_id = uuid.uuid4()
                    s = Session(
                        session_id=session_id, website_id=website_id, created_at=timestamp, hostname=hostname,
                        browser=row["dimensions"][2], os=row["dimensions"][3], device=row["dimensions"][4],
                        screen=row["dimensions"][5], country=country
                    )
                    p = WebsiteEvent(
                        id=uuid.uuid4(), website_id=website_id, session_id=session_id, created_at=timestamp,
                        url=row["dimensions"][0], title=row["dimensions"][1], referrer=referrer
                    )
                    sql_inserts.extend([s.sql(), p.sql()])
                    last_session_id = session_id
                for i in range(page_views - sessions):
                    p = WebsiteEvent(
                        id=uuid.uuid4(), website_id=website_id, session_id=last_session_id, created_at=timestamp,
                        url=row["dimensions"][0], title=row["dimensions"][1], referrer=referrer
                    )
                    sql_inserts.append(p.sql())

    return sql_inserts


class CSVRow(NamedTuple):
    path: str
    title: str
    browser: str
    os: str
    device: str
    screen: str
    datetime: str
    country_id: str
    referral_path: str
    count: str
    date: datetime.date

    @staticmethod
    def header():
        return f"path,title,browser,os,device,screen,datetime,country_id,referral_path,count"

    def csv(self):
        return f"{self.path},{self.title},{self.browser},{self.os},{self.device},{self.screen},{self.datetime},{self.country_id},{self.referral_path},{self.count}"


def __migrate_transform_csv(rows):
    csv_rows = [CSVRow.header()]
    for day, value in rows.items():
        for row in value:
            page_views, _ = map(int, row["metrics"][0]["values"])
            row = CSVRow(
                path=row["dimensions"][0],
                title=row["dimensions"][1],
                browser=row["dimensions"][2],
                os=row["dimensions"][3],
                device=row["dimensions"][4],
                screen=row["dimensions"][5],
                date=_convert_ua_datetime(row["dimensions"][6]),
                country_id=row["dimensions"][7],
                referral_path=row["dimensions"][8],
                count=page_views
            )
            csv_rows.append(row.csv())
    return csv_rows

def _convert_ua_datetime(dt):
    return datetime.strptime(dt, '%Y%m%d%H%M').strftime("%Y-%m-%d %H:%M:00.000+00")

def _safe_db_value(str, length):
    return str.replace("'", "''")[:length] if str else ''
