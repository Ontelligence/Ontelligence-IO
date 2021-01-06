import requests
from typing import List, Optional

from pprint import PrettyPrinter
p = PrettyPrinter(indent=4)

from ontelligence.providers.appsflyer.base import BaseAppsFlyerProvider


class PullAPI(BaseAppsFlyerProvider):

    version = 'v5'
    report_type = None
    additional_fields = None
    retargeting = False

    def __init__(self, conn_id: str, app_id: str, **kwargs):
        super().__init__(conn_id=conn_id, **kwargs)
        if self.report_type is None:
            raise Exception('Please specify the report type')
        self.app_id = app_id
        self.base_url = f'https://hq.appsflyer.com/export/{app_id}/{self.report_type}/{self.version}?api_token={self.get_conn()}'

    def get_report(self, start_date: str, end_date: str, file_path: str, additional_fields: Optional[List[str]] = None):
        url = f'{self.base_url}&from={start_date}&to={end_date}'
        if self.additional_fields or additional_fields:
            self.additional_fields = self.additional_fields if self.additional_fields else []
            additional_fields = additional_fields if additional_fields else []
            url = f'{url}&additional_fields={",".join(self.additional_fields + additional_fields)}'
        if self.retargeting:
            url = f'{url}&reattr=true'

        res = requests.get(url=url)
        if res.status_code == 200:
            with open(file_path, 'w') as f:
                f.write(res.text)
        else:
            print('ERROR ON URL:', url)
            raise Exception(f'StatusCode={res.status_code}: {res.text}')

        return file_path


########################################################################################################################
# Performance.
#   ref: https://support.appsflyer.com/hc/en-us/articles/207034346
########################################################################################################################


class DailyReport(PullAPI):
    report_type = 'daily_report'


class PartnersReport(PullAPI):
    report_type = 'partners_by_date_report'


class GeoReport(PullAPI):
    report_type = 'geo_by_date_report'


########################################################################################################################
# User acquisition.
#   ref: https://support.appsflyer.com/hc/en-us/articles/360007530258
########################################################################################################################


class InstallsReport(PullAPI):
    report_type = 'installs_report'


class InAppEventsReport(PullAPI):
    report_type = 'in_app_events_report'


# class UninstallsReport(_BasePullAPIConnection):
#     report_type = 'uninstall_events_report'


# class OrganicInstallsReport(_BasePullAPIConnection):
#     report_type = 'organic_installs_report'


class OrganicInAppEventsReport(PullAPI):
    report_type = 'organic_in_app_events_report'


########################################################################################################################
# Retargeting.
########################################################################################################################


# class RetargetingConversionsReport(_BasePullAPIConnection):
#     report_type = 'installs_report'
#     retargeting = True


# class RetargetingInAppEventsReport(_BasePullAPIConnection):
#     report_type = 'in_app_events_report'
#     retargeting = True


########################################################################################################################
# Ad revenue.
#   note: Data freshness rate is "Ad revenue"
########################################################################################################################


# class AttributedAdRevenueReport(_BasePullAPIConnection):
#     report_type = 'ad_revenue_raw'


# class RetargetingAdRevenueReport(_BasePullAPIConnection):
#     report_type = 'ad_revenue_raw'
#     retargeting = True


# class OrganicAdRevenueReport(_BasePullAPIConnection):
#     report_type = 'ad_revenue_organic_raw'


########################################################################################################################
# Validation rules.
########################################################################################################################


# class InvalidInstallsReport(_BasePullAPIConnection):
#     report_type = 'invalid_installs_report'


# class InvalidInAppEventsReport(_BasePullAPIConnection):
#     report_type = 'invalid_in_app_events_report'
