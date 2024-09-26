import json
import uuid

import typer
from typing_extensions import Annotated
import validators
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    Row,
    RunReportRequest,
)
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlparse

extractor = typer.Typer()

DIMENSIONS = (
    'pagePathPlusQueryString',
    'pageTitle',
    'browser',
    'operatingSystem',
    'deviceCategory',
    'screenResolution',
    'dateHourMinute',
    'countryId',
    'pageReferrer',
)

METRICS = (
    'screenPageViews',
    'sessions',
)

class Site(NamedTuple):
    table: str
    website_id: uuid.UUID
    host: str

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
        insert = (
            f"INSERT INTO public.session (session_id, website_id, created_at, hostname, browser, os, device, screen, country) "
            f"VALUES ('{self.session_id}', '{self.website_id}', '{self.created_at}', '{self.hostname[:100]}', '{_safe_db_value(self.browser, 20)}', '{_safe_db_value(self.os, 20)}', '{_safe_db_value(self.device, 20)}', '{_safe_db_value(self.screen, 10)}', '{self.country}');"
        )
        return insert


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

        insert = (
            f"INSERT INTO public.website_event (event_id, website_id, session_id, created_at, url_path, url_query, referrer_path, referrer_query, referrer_domain, page_title, event_name, visit_id) "
            f"VALUES ('{self.id}', '{self.website_id}', '{self.session_id}', '{self.created_at}', '{_safe_db_value(url_data.path, 500)}', '{_safe_db_value(url_data.query, 500)}', '{_safe_db_value(referrer_data.path, 500)}', '{_safe_db_value(referrer_data.query, 500)}', '{_safe_db_value(referrer_data.hostname, 500)}', '{_safe_db_value(self.title, 500)}', 'pageview', '{uuid.uuid4()}');"
        )

        return insert

sites = {
    'BYM': Site(host='www.bebesymas.com', table='298125183', website_id='d90f4a57-31d4-4aa0-8ac8-238bc61d2003'),
    'CYM': Site(host='www.cocoymaya.com', table='298125183', website_id='a0a25bf2-bfe7-4b7f-abd0-aa6aaf4ec5ac'),
    'DDV': Site(host='www.diariodelviajero.com', table='298125183', website_id='21aaa234-a195-45b3-88a5-3e7720781248'),
    'EBS': Site(host='www.elblogsalmon.com', table='298125183', website_id='95845666-a411-482e-b652-7631e15b9743'),
    'M22': Site(host='www.motorpasionmoto.com', table='298197205', website_id='d2c2f659-01d4-4b14-b05e-28efa81d4600'),
    'MOP': Site(host='www.motorpasion.com', table='298125183', website_id='1b258930-34ec-4eb6-a659-c7df374943aa'),
    'MPX': Site(host='www.motorpasion.com.mx', table='298125183', website_id='eb820f0e-73db-45f1-9f02-ddf585cfc708'),
    'PYM': Site(host='www.pymesyautonomos.com', table='298125183', website_id='64ae7857-00c3-46cc-9e09-adcc45465bed'),
    'HEP': Site(host='hyundaielectricpower.motorpasion.com', table='298125183', website_id='f153dcd4-5057-4d1d-a885-a115f48b53a0'),
    'SBYM': Site(host='guiaservicios.bebesymas.com', table='298125183', website_id='9d047a80-a5fb-4b8e-8bed-8fc9513914c3'),
}

def _convert_ua_datetime(dt):
    return datetime.strptime(dt, '%Y%m%d%H%M').strftime("%Y-%m-%d %H:%M:00.000+0200")

def _safe_db_value(str, length):
    return str.replace("'", "''")[:length] if str else ''

def _dates(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    return [f"{start_date + timedelta(days=d):%Y-%m-%d}" for d in range(((end_date.date() - start_date.date()).days + 1))]

def _sql(rows, website_id, hostname):
    sql_inserts = []
    for row in rows:
        referrer = row.dimension_values[8].value
        if not validators.url(referrer):
            referrer = ""

        timestamp = _convert_ua_datetime(row.dimension_values[6].value)
        country = row.dimension_values[7].value
        page_views = int(row.metric_values[0].value)
        sessions = int(row.metric_values[1].value)
        sessions = max(sessions, 1)  # in case it's zero
        if page_views == sessions:  # One page view for each session
            for i in range(sessions):
                session_id = uuid.uuid4()
                s = Session(
                    session_id=session_id, website_id=website_id, created_at=timestamp, hostname=hostname,
                    browser=row.dimension_values[2].value, os=row.dimension_values[3].value, device=row.dimension_values[4].value,
                    screen=row.dimension_values[5].value, country=country
                )
                p = WebsiteEvent(
                    id=uuid.uuid4(), website_id=website_id, session_id=session_id, created_at=timestamp,
                    url=row.dimension_values[0].value, title=row.dimension_values[1].value, referrer=referrer
                )
                sql_inserts.extend([s.sql(), p.sql()])

        elif page_views % sessions == 0:  # Split equally
            group_size = page_views // sessions
            for i in range(sessions):
                session_id = uuid.uuid4()
                s = Session(
                    session_id=session_id, website_id=website_id, created_at=timestamp, hostname=hostname,
                    browser=row.dimension_values[2].value, os=row.dimension_values[3].value, device=row.dimension_values[4].value,
                    screen=row.dimension_values[5].value, country=country
                )
                sql_inserts.append(s.sql())
                for j in range(group_size):
                    p = WebsiteEvent(
                        id=uuid.uuid4(), website_id=website_id, session_id=session_id, created_at=timestamp,
                        url=row.dimension_values[0].value, title=row.dimension_values[1].value, referrer=referrer
                    )
                    sql_inserts.append(p.sql())
        else:  # One page view for each, rest for the last session
            last_session_id = None
            for i in range(sessions):
                session_id = uuid.uuid4()
                s = Session(
                    session_id=session_id, website_id=website_id, created_at=timestamp, hostname=hostname,
                    browser=row.dimension_values[2].value, os=row.dimension_values[3].value, device=row.dimension_values[4].value,
                    screen=row.dimension_values[5].value, country=country
                )
                p = WebsiteEvent(
                    id=uuid.uuid4(), website_id=website_id, session_id=session_id, created_at=timestamp,
                    url=row.dimension_values[0].value, title=row.dimension_values[1].value, referrer=referrer
                )
                sql_inserts.extend([s.sql(), p.sql()])
                last_session_id = session_id
            for i in range(page_views - sessions):
                p = WebsiteEvent(
                    id=uuid.uuid4(), website_id=website_id, session_id=last_session_id, created_at=timestamp,
                    url=row.dimension_values[0].value, title=row.dimension_values[1].value, referrer=referrer
                )
                sql_inserts.append(p.sql())

    return sql_inserts

@extractor.command()
def migrate(
    mnemonic: Annotated[str, typer.Argument(help="Mnemonic of website being migrated")],
    startdate: Annotated[str, typer.Argument(help="Backup start date")],
    enddate: Annotated[str, typer.Argument(help="Backup end date")],
):
    if mnemonic not in sites:
        typer.echo("Site doesn't exist.")
        raise typer.Exit(2)

    site = sites[mnemonic]
    client = BetaAnalyticsDataClient.from_service_account_file(Path('google.json'))
    output_path: Path = Path(f"{mnemonic}-{startdate}-to-{enddate}.sql")

    # Creates a new file
    with output_path.open(mode="w") as outputFile:
        pass

    dates = _dates(startdate, enddate)
    property = f"properties/{site.table}"
    dimensions = [Dimension(name=dimension) for dimension in DIMENSIONS]
    metrics = [Metric(name=metric) for metric in METRICS]

    for date in dates:
        offset = 0
        limit = 100000

        with output_path.open(mode="a") as outputFile:
            while True:
                request_api = RunReportRequest(
                    property=property,
                    dimensions=dimensions,
                    metrics=metrics,
                    date_ranges=[DateRange(start_date=date, end_date=date)],
                    offset=offset,
                    limit=limit,
                )

                response =  client.run_report(request_api)

                if (len(response.rows)):
                    for insert in _sql(response.rows, site.website_id, site.host):
                        outputFile.write(f"{insert}\n")

                offset += limit
                if offset >= response.row_count:
                    break

    with output_path.open(mode="a") as outputFile:
        outputFile.write(
            f"WITH r AS (SELECT TO_CHAR(created_at, 'yyyy-mm-ddT00:00:00')::date as reset_time, website_id FROM public.website_event WHERE website_id = '{site.website_id}' ORDER BY event_id DESC LIMIT 1) UPDATE public.website SET reset_at = r.reset_time, created_at = r.reset_time FROM r WHERE public.website.website_id = r.website_id;\n"
        )

    typer.echo(f"Report written to {output_path.absolute()}")
