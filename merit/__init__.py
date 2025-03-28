import dlt
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.config_setup import register_auth
from datetime import datetime
from requests.models import Response
from typing import Any

from .auth import MeritAuth
from .paginators import MeritDatePaginator
from .dates import get_default_dates, convert_date_format, format_date

def clean_response(response: Response, *args: Any, **kwargs: Any) -> Response:
    """Clean null bytes from Merit API responses.
    
    Args:
        response: The response to clean
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
        
    Returns:
        Response with cleaned content
    """
    if response.content:
        response._content = response.content.replace(b"\x00", b"").replace(b"\\u0000", b"")
    return response

# Register the custom auth class
register_auth("merit", MeritAuth)

@dlt.source(section="merit")
def aktiva( # type: ignore
    api_id: str = dlt.secrets.value, 
    api_key: str = dlt.secrets.value,
    initial_start_date: datetime | None = None,
    period_end_date: datetime | None = None,
):
    """Merit Aktiva API source.
    
    Args:
        api_id: Merit API ID
        api_key: Merit API key
        initial_start_date: Initial start date for incremental loading as datetime.
                          Defaults to 12 months ago if not provided.
        period_end_date: End date for the period as datetime.
                       Defaults to today if not provided.
                       Note: Period length should be max 3 months from start date.
    """
    # Get default dates if not provided
    default_start, default_end = get_default_dates()
    start_date = initial_start_date or default_start
    end_date = period_end_date or default_end

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://aktiva.merit.ee/api/",  # No v1/v2 in base URL
            "auth": MeritAuth(api_id=api_id, api_key=api_key),
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "method": "POST",  # All Merit API endpoints use POST
                "response_actions": [clean_response],  # Clean all responses
                "paginator": {
                    "type": "single_page",
                },
            },
        },
        "resources": [
            # Master data resources (no incremental loading)
            {
                "name": "accounts",
                "primary_key": ["account_id"],
                "endpoint": {
                    "path": "v1/getaccounts",
                },
            },
            {
                "name": "departments",
                "primary_key": ["code"],
                "endpoint": {
                    "path": "v1/getdepartments",
                },
            },
            {
                "name": "items",
                "primary_key": ["item_id"],
                "endpoint": {
                    "path": "v1/getitems",
                },
            },
            {
                "name": "item_groups",
                "endpoint": {
                    "path": "v2/getitemgroups",
                },
            },
            {
                "name": "banks",
                "primary_key": ["bank_id"],
                "endpoint": {
                    "path": "v1/getbanks",
                },
            },
            {
                "name": "units",
                "primary_key": ["code"],
                "endpoint": {
                    "path": "v1/getunits",
                },
            },
            {
                "name": "dimensions",
                "primary_key": ["code"],
                "endpoint": {
                    "path": "v2/getdimensions",
                },
            },
            {
                "name": "costcenters",
                "primary_key": ["code"],
                "endpoint": {
                    "path": "v1/getcostcenters",
                },
            },
            {
                "name": "projects",
                "primary_key": ["code"],
                "endpoint": {
                    "path": "v1/getprojects",
                },
            },
            {
                "name": "vendors",
                "primary_key": ["vendor_id"],
                "endpoint": {
                    "path": "v1/getvendors",
                },
            },
            {
                "name": "fixed_assets",
                "primary_key": ["fa_id"],
                "endpoint": {
                    "path": "v2/getfixassets",
                },
            },
            {
                "name": "fixed_asset_locations",
                "endpoint": {
                    "path": "v2/getfalocations",
                },
            },
            {
                "name": "locations",
                "primary_key": ["location_id"],
                "endpoint": {
                    "path": "v2/getlocations",
                },
            },
            {
                "name": "customers",
                "primary_key": ["customer_id"],
                "endpoint": {
                    "path": "v1/getcustomers",
                },
            },
            {
                "name": "payment_types",
                "endpoint": {
                    "path": "v2/getpaymenttypes",
                    "params": {
                        "param": "", # This endpoint gives HTTP 500 error when no params provided
                    },
                },
            },
            {
                "name": "taxes",
                "endpoint": {
                    "path": "v1/gettaxes",
                },
            },
            # Transactional resources with incremental loading
            {
                "name": "purchase_invoices",
                "primary_key": ["PIHId"],
                "endpoint": {
                    "path": "v2/getpurchorders",
                    "paginator": MeritDatePaginator(start_date=start_date, end_date=end_date, interval_days=30, date_type=1),
                    "incremental": {
                        "start_param": "PeriodStart",
                        "cursor_path": "ChangedDate",
                        "initial_value": format_date(start_date),
                        "convert": convert_date_format,
                    }
                },
            },
            {
                "name": "sales_invoices",
                "primary_key": ["SIHId"],
                "endpoint": {
                    "path": "v2/getinvoices",
                    "paginator": MeritDatePaginator(start_date=start_date, end_date=end_date, interval_days=30, date_type=1),
                    "incremental": {
                        "start_param": "PeriodStart",
                        "cursor_path": "ChangedDate",
                        "initial_value": format_date(start_date),
                        "convert": convert_date_format,
                    }
                },
            },
            {
                "name": "gl_batches",
                "primary_key": ["GLBId"],
                "endpoint": {
                    "path": "v1/GetGLBatchesFull",
                    "paginator": MeritDatePaginator(start_date=start_date, end_date=end_date, interval_days=30, date_type=1),
                    "incremental": {
                        "start_param": "PeriodStart",
                        "cursor_path": "ChangedDate",
                        "initial_value": format_date(start_date),
                        "convert": convert_date_format,
                    },
                    "params": {
                        "WithLines": 1,
                    }
                },
            },
            {
                "name": "payments",
                "primary_key": ["PHId"],
                "endpoint": {
                    "path": "v2/getpayments",
                    "paginator": MeritDatePaginator(start_date=start_date, end_date=end_date, interval_days=30, date_type=1),
                    "incremental": {
                        "start_param": "PeriodStart",
                        "cursor_path": "ChangedDate",
                        "initial_value": format_date(start_date),
                        "convert": convert_date_format,
                    }
                },
            },
        ],
    }

    yield from rest_api_resources(config)
