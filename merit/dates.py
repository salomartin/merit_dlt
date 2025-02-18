"""Date handling utilities for Merit API.

All Merit API date handling is centralized here to ensure consistent formatting
and timezone handling across the codebase.
"""

from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Tuple


def format_date(date: datetime) -> str:
    """Format date as required by Merit API date parameters (YYYYMMDD).
    
    Args:
        date: The datetime to format
        
    Returns:
        Date string in YYYYMMDD format
    """
    return date.strftime("%Y%m%d")


def format_auth_timestamp(date: Optional[datetime] = None) -> str:
    """Format datetime as required by Merit API authentication (YYYYMMDDHHMMSS in UTC).
    
    Args:
        date: The datetime to format, defaults to current UTC time if None
        
    Returns:
        Timestamp string in YYYYMMDDHHMMSS format
    """
    if date is None:
        date = datetime.now(timezone.utc)
    elif date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    return date.strftime("%Y%m%d%H%M%S")


def parse_date(date_str: str) -> datetime:
    """Parse a Merit API date string (YYYYMMDD) into a datetime object.
    
    Args:
        date_str: Date string in YYYYMMDD format
        
    Returns:
        datetime object
    """
    return datetime.strptime(date_str, "%Y%m%d")


def get_default_dates() -> Tuple[datetime, datetime]:
    """Get default start and end dates for Merit API.
    
    Returns:
        tuple: (start_date, end_date) as datetime objects
        - start_date: 12 months ago from today
        - end_date: today
    """
    today = datetime.now(timezone.utc)
    start_date = today - timedelta(days=365)  # 12 months ago
    return start_date, today


def convert_date_format(date_str: Optional[str]) -> Optional[str]:
    """Convert date string between Merit API formats.
    Used for incremental loading to ensure consistent date format.
    
    Handles both Merit API date format (YYYYMMDD) and ISO format (YYYY-MM-DDTHH:MM:SS.ff).
    
    Args:
        date_str: Date string to convert, can be None
        
    Returns:
        Converted date string in YYYYMMDD format or None if input is None
    """
    if date_str is None:
        return None

    try:
        # First try Merit API format (YYYYMMDD)
        dt = parse_date(date_str)
    except ValueError:
        try:
            # Then try ISO format
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError as e:
            raise ValueError(
                f"Date string '{date_str}' does not match either Merit API format (YYYYMMDD) "
                f"or ISO format (YYYY-MM-DDTHH:MM:SS.ff)"
            ) from e
    
    return format_date(dt)


def serialize_date(obj: Any) -> str:
    """Serialize datetime objects to Merit's date format.
    Used as a default serializer for orjson.dumps.
    
    Args:
        obj: Object to convert, expected to be a datetime
        
    Returns:
        Date string in YYYYMMDD format
        
    Raises:
        TypeError: If obj is not a datetime
    """
    if isinstance(obj, datetime):
        return format_date(obj)
    raise TypeError("Object is not of type datetime") 