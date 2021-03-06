from typing import List, Optional
from ontelligence.utils.log import LoggingMixin

class RFM(LoggingMixin):

    def __init__(
            self,
            query: str,
            id_field: Optional[str] = None,
            recency_field: Optional[str] = None,  # How recently did a customer purchase?
            frequency_field: Optional[str] = None,  # How often does a customer purchase?
            value_field: Optional[str] = None,  # How much does a customer spend?
            additional_dimensions: Optional[List[str]] = None,
            rfm_table: Optional[str] = None
    ):
        self.input_query = query.strip(' ').strip('\n').rstrip(';')
        self.id_field = id_field or 'customer_id'
        self.recency_field = recency_field or 'recency'
        self.frequency_field = frequency_field or 'frequency'
        self.value_field = value_field or 'value'
        self.additional_dimensions = additional_dimensions or []
        self.rfm_table = rfm_table.upper() if rfm_table else 'RFM_SCORES_ALL'

    def _get_dimensions_for_select(self):
        if self.additional_dimensions:
            return f'''"{'", "'.join(self.additional_dimensions)}",'''
        return ''

    def generate_rfm_query(self):
        partition_by = f'''PARTITION BY {self._get_dimensions_for_select().rstrip(',')} ''' if self.additional_dimensions else ''
        query = f'''SELECT "{self.id_field}" AS "CUSTOMER_ID",
                           {self._get_dimensions_for_select()}
                           "{self.recency_field}" AS "RECENCY",
                           "{self.frequency_field}" AS "FREQUENCY",
                           "{self.value_field}" AS "VALUE",
                           NTILE(5) OVER ({partition_by}ORDER BY "{self.recency_field}") AS "R_Score",
                           NTILE(5) OVER ({partition_by}ORDER BY "{self.frequency_field}") AS "F_Score",
                           NTILE(5) OVER ({partition_by}ORDER BY "{self.value_field}") AS "M_Score"
                    FROM ({self.input_query})'''
        return query

    def generate_rfm_query_using_rank(self):
        partition_by = f'''PARTITION BY {self._get_dimensions_for_select().rstrip(',')} ''' if self.additional_dimensions else ''
        query = f'''SELECT "{self.id_field}" AS "CUSTOMER_ID",
                           {self._get_dimensions_for_select()}
                           "{self.recency_field}" AS "RECENCY",
                           "{self.frequency_field}" AS "FREQUENCY",
                           "{self.value_field}" AS "VALUE",
                           RANK() OVER ({partition_by}ORDER BY "{self.recency_field}") AS "R_RANK",
                           RANK() OVER ({partition_by}ORDER BY "{self.frequency_field}") AS "F_RANK",
                           RANK() OVER ({partition_by}ORDER BY "{self.value_field}") AS "M_RANK"
                    FROM ({self.input_query})'''
        return query

    def generate_rfm_scores_using_average(self):
        query = f'''SELECT "CUSTOMER_ID",
                           {self._get_dimensions_for_select()}
                           ("R_Score" + "F_Score" + "M_Score") / 3 AS RFM_Score,
                           CASE WHEN ROUND(RFM_SCORE,2) >= 3.34 and ROUND(RFM_SCORE,2) <= 5 THEN 1
                                WHEN ROUND(RFM_SCORE,2) >= 1.67 and ROUND(RFM_SCORE,2) < 3.34 THEN 2
                                WHEN ROUND(RFM_SCORE,2) >= 0 and ROUND(RFM_SCORE,2) < 1.67 THEN 3
                                END AS "TIER"
                    FROM ({self.generate_rfm_query()})'''
        return query

    def generate_rfm_scores_using_ntile_over_average(self):
        partition_by = f'''PARTITION BY {self._get_dimensions_for_select().rstrip(',')} ''' if self.additional_dimensions else ''
        query = f'''SELECT "CUSTOMER_ID",
                           {self._get_dimensions_for_select()}
                           NTILE(5) OVER ({partition_by}ORDER BY ("R_RANK" + "F_RANK" + "M_RANK") / 3) AS RFM_SCORE,
                           CASE WHEN RFM_SCORE IN (5)    THEN 1
                                WHEN RFM_SCORE IN (3, 4) THEN 2
                                WHEN RFM_SCORE IN (1, 2) THEN 3
                                END AS "TIER"
                    FROM ({self.generate_rfm_query_using_rank()})'''
        return query
