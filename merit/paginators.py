from datetime import datetime, timedelta
from typing import Any, List, Optional

from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.requests import Response, Request


class MeritDatePaginator(BasePaginator):
    """A paginator for Merit API that handles date-based pagination.
    
    This paginator uses PeriodStart and PeriodEnd parameters to paginate through data
    in configurable intervals (default 7 days). The interval cannot exceed 3 months
    as per Merit API requirements.
    
    Args:
        start_date (datetime): The overall start date for data retrieval
        end_date (datetime): The overall end date for data retrieval
        interval_days (int, optional): Number of days per page. Defaults to 7.
            Cannot exceed 90 days (3 months).
        date_type (int, optional): Merit API DateType parameter. 
            0 for DocumentDate, 1 for ChangedDate. Defaults to 0.
    """
    MAX_INTERVAL_DAYS = 90  # 3 months maximum interval

    def __init__(
        self,
        start_date: datetime,
        end_date: datetime,
        interval_days: int = 7,
        date_type: int = 0,
    ) -> None:
        super().__init__()
        
        # Validate and set the interval
        if interval_days > self.MAX_INTERVAL_DAYS:
            raise ValueError(
                f"Interval cannot exceed {self.MAX_INTERVAL_DAYS} days (3 months) "
                f"as per Merit API requirements. Got {interval_days} days."
            )
        if interval_days < 1:
            raise ValueError("Interval must be at least 1 day.")
        
        self.interval_days = interval_days
        self.date_type = date_type
        
        # Set the overall period
        self.start_date = start_date
        self.end_date = end_date
        
        # Initialize the current period
        self.current_start = start_date
        self.current_end = min(
            start_date + timedelta(days=interval_days - 1),
            end_date
        )

    def _format_date(self, date: datetime) -> str:
        """Format date as required by Merit API (yyyymmdd)."""
        return date.strftime("%Y%m%d")

    def init_request(self, request: Request) -> None:
        """Initialize the first request with the first period."""
        self.update_request(request)

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        """Update paginator state after each response.
        
        Determines if there are more pages by checking if the current end date
        has reached the overall end date.
        """
        # Calculate the next period
        next_start = self.current_end + timedelta(days=1)
        next_end = min(
            next_start + timedelta(days=self.interval_days - 1),
            self.end_date
        )
        
        # Check if we've reached the end
        if next_start > self.end_date:
            self._has_next_page = False
        else:
            self._has_next_page = True
            self.current_start = next_start
            self.current_end = next_end

    def update_request(self, request: Request) -> None:
        """Update request parameters for the next page."""
        # Initialize params if not present
        if request.params is None:
            request.params = {}
            
        # Update the period parameters in request params
        request.params.update({
            "PeriodStart": self._format_date(self.current_start),
            "PeriodEnd": self._format_date(self.current_end),
            "DateType": self.date_type,
        })