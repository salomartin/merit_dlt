import hmac
import hashlib
import base64
from typing import Any
from dlt.sources.helpers.rest_client.auth import AuthConfigBase
import orjson
from requests.models import PreparedRequest

from .dates import serialize_date, format_auth_timestamp

def merit_dumps(obj: Any) -> bytes:
    """Convert request data to Merit's expected format.
    
    Args:
        obj: Request data to convert
        
    Returns:
        JSON bytes in Merit's format
    """
    return orjson.dumps(
        obj,
        option=orjson.OPT_PASSTHROUGH_DATETIME,
        default=serialize_date,
    )

class MeritAuth(AuthConfigBase):
    """Merit Aktiva API authentication.
    
    Implements Merit's custom authentication scheme which requires:
    - API ID and API Key credentials
    - A timestamp in YYYYMMDDHHMMSS format in UTC
    - A HMAC-SHA256 signature generated from the API ID, timestamp, and request data
    """
    
    def __init__(self, api_id: str, api_key: str):
        """Initialize Merit authentication.
        
        Args:
            api_id: The Merit API ID
            api_key: The Merit API Key
        """
        self.api_id = api_id
        self.api_key = api_key

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Implement Merit's authentication by modifying the request URL with auth parameters.
        
        This method is called by the requests library to authenticate the request.
        
        Args:
            request: The prepared request to authenticate
            
        Returns:
            Modified request with Merit authentication parameters
        """
        if request.url is None:
            raise ValueError("Request URL cannot be None")

        # Get request data for signature
        data: dict[str, Any] = {}
        if request.body:
            if isinstance(request.body, bytes):
                data = orjson.loads(request.body)
            else:
                data = orjson.loads(request.body.encode())

        # Move URL parameters to request body
        if request.url and '?' in request.url:
            # Split URL and parameters
            url_base, params_str = request.url.split('?', 1)
            
            # Parse parameters into dict
            from urllib.parse import parse_qs
            url_params: dict[str, list[str]] = parse_qs(params_str)
            
            # Convert single item lists to single values
            url_params_single = {k: v[0] if len(v) == 1 else v for k, v in url_params.items()}
            
            # Update request body with URL parameters
            data.update(url_params_single)
            
            # Update request URL to remove parameters
            request.url = url_base
            
            # Update request body
            request.body = merit_dumps(data)

        # Generate timestamp in Merit's format (UTC)
        timestamp = format_auth_timestamp()
        
        # Create signature data string using merit_dumps
        data_string = f"{self.api_id}{timestamp}{merit_dumps(data).decode()}"
        
        # Generate signature
        signature = base64.b64encode(
            hmac.new(
                self.api_key.encode(),
                data_string.encode(),
                hashlib.sha256
            ).digest()
        ).decode()
        
        # Add auth parameters to URL
        auth_params = f"ApiId={self.api_id}&timestamp={timestamp}&signature={signature}"
        
        # Ensure we add auth params after any existing params
        if '?' in request.url:
            request.url = f"{request.url}&{auth_params}"
        else:
            request.url = f"{request.url}?{auth_params}"
        
        return request

    