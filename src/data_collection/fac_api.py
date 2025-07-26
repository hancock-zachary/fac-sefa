"""Federal Audit Clearinghouse (FAC) API Client | Basic structure for querying FAC data"""
#%%
# Import modules and libraries needed within code.
import requests
import os
from typing import Dict, List, Optional
import time
from dotenv import load_dotenv
load_dotenv()


#%%
# API error exception class.
class APIError(Exception):
    """Base exception for FAC API errors"""
    pass


#%%
# FAC API Client.
class FACClient:
    """Basic client for interacting with the Federal Audit Clearinghouse API."""
    def __init__(self) -> None:
        """
        Initialize the FAC API client and key class-level characters.
        """
        # Set up headers.
        self.api_key = os.getenv('API_KEY_FAC')
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({'X-API-Key': self.api_key})  # Header gets sent with every request automatically.
        else:
            raise APIError("API key is required. Set API_KEY_FAC environment variable.")
        
        # Set up API url.
        self.base_url = 'https://api.fac.gov'
        self.endpoints = {
            'general': '/general'
            , 'federal_awards': '/federal_awards'
            , 'additional_eins': '/additional_eins'
            , 'additional_ueis': '/additional_ueis'
            , 'corrective_action_plans': '/corrective_action_plans'
            , 'findings': '/findings'
            , 'findings_text': '/findings_text'
            , 'notes_to_sefa': '/notes_to_sefa'
            , 'passthrough': '/passthrough'
            , 'secondary_auditors': '/secondary_auditors'
        }

        # Features of the FAC API.
        self.max_single_request_size = 20_000
        self.min_audit_year = 2016
        self.max_audit_year = int(time.strftime('%Y'))
        self.all_auditee_states = [
            'AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'FM', 'GA', 'GU', 'HI', 'IA', 'ID'
            , 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MH', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND'
            , 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'PW', 'RI', 'SC', 'SD', 'TN', 'TX'
            , 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY'
        ]

    def _validate_string(self, input_string: str) -> str:
        """
        Purpose:
            Validate and normalize string inputs. Strings are normalized to be lower case and to be stripped of extra spaces.
        Args:
            input_string: String to validate.
        Returns:
            Normalized string value.
        Raises:
            ValueError: If string is None or invalid.
            TypeError: If string is not a string.
        """
        if input_string is None:
            raise ValueError("input_string cannot be None.")
        
        if not isinstance(input_string, str):
            raise TypeError(f"input_string must be str, got {type(input_string).__name__}.")
        
        output_string = input_string.strip().lower()  # Normalize the string variable.
        return output_string
    
    def _make_request(self, endpoint_name: str, params: Dict = None, handle_429: bool = False) -> List[Dict]:
        """
        Purpose:
            Make an endpoint specific API request with error handling.
        Args:
            endpoint_name: Name of the endpoint (e.g., 'general', 'findings')
            params: Query parameters to include in the request
            handle_429: If True, automatically retry on 429 errors indefinitely using Retry-After header
        Returns:
            List of records from the API.
        Raises:
            APIError: If the API request fails
            ValueError: If endpoint_name is invalid
            TypeError: If endpoint_name is not a string
        """
        # Exception and type handling for endpoint_name variable.
        endpoint_name = self._validate_string(endpoint_name)
        if endpoint_name not in self.endpoints:
            available = ', '.join(self.endpoints.keys())
            raise ValueError(f"Unknown endpoint: '{endpoint_name}'. Available: {available}")
        
        endpoint = self.endpoints.get(f"{endpoint_name}")  # Identify the endpoint to add to the url.
        url = f"{self.base_url}{endpoint}"  # Add endpoint to the base url.
        
        while True:
            try:
                response = self.session.get(url, params=params or {})
                response.raise_for_status()  # Raises exception for bad status codes.
                result = response.json()

                if isinstance(result, list):  # FAC API returns data as a list
                    return result
                else:
                    print(f"Warning: Expected list from {endpoint_name}, got {type(result)}")  # Log unexpected response format
                    return []
            except requests.exceptions.HTTPError as e:
                if response.status_code == 401:
                    raise APIError("Authentication failed. Check your API key.") from e
                elif response.status_code == 429:
                    if not handle_429:
                        raise APIError("Rate limit exceeded. Please wait before making more requests.") from e
                    else:
                        retry_after = response.headers.get('Retry-After') or response.headers.get('retry-after')
                        if retry_after:
                            try:
                                wait_time = float(retry_after)
                                print(f"Rate limit hit (Server requested {retry_after}s wait. Waiting {wait_time:.1f}s...")
                                time.sleep(wait_time)
                                continue
                            except ValueError:
                                print(f"Invalid Retry-After header: {retry_after}")
                
                elif response.status_code == 404:
                    raise APIError(f"Endpoint not found: {endpoint_name}") from e
                else:
                    raise APIError(f"HTTP {response.status_code} error for {endpoint_name}: {e}") from e
            except requests.exceptions.ConnectionError as e:
                raise APIError(f"Failed to connect to FAC API: {e}") from e
            except requests.exceptions.Timeout as e:
                raise APIError(f"Request timeout for {endpoint_name}: {e}") from e
            except requests.exceptions.RequestException as e:
                raise APIError(f"Request failed for {endpoint_name}: {e}") from e
            except ValueError as e:  # JSON decode error
                raise APIError(f"Invalid JSON response from {endpoint_name}: {e}") from e
            
            if not handle_429:
                break

    def get_general(self
                    , report_id: str | None = None
                    , auditee_uei: str | None = None
                    , auditee_ein: str | None = None
                    , auditee_name: str | None = None
                    , auditee_city: str | None = None
                    , auditee_state: str | None = None
                    , audit_year: int | None = None
                    , handle_429: bool = False
                    ) -> List[Dict]:
        """
        Purpose:
            Get request from \"general\" endpoint of the Federal Audit Clearinghouse API.
        Args:
            report_id: Specific SEFA report ID
            auditee_uei: UEI number tied to auditee
            auditee_ein: EIN number tied to auditee
            auditee_name: Name of auditee as it appears on the SEFA report
            auditee_city: City name where the auditee is located
            auditee_state: State abbreviation where the auditee is located
            audit_year: Audit year of SEFA report, typically lagged by one calendar year
            handle_429: If True, automatically retry on 429 errors using Retry-After header
            max_retries: Maximum number of retry attempts for 429 errors (only used if handle_429=True)
            max_backoff: Maximum backoff delay in seconds (only used if handle_429=True)
        Returns:
            List of audit records from the general endpoint.
        """
        params = {}
        if report_id is not None:
            params['report_id'] = f"eq.{report_id}"
        if auditee_uei is not None:
            params['auditee_uei'] = f"eq.{auditee_uei}"
        if auditee_ein is not None:
            params['auditee_ein'] = f"eq.{auditee_ein}"
        if auditee_name is not None:
            params['auditee_name'] = f"ilike.*{auditee_name}*"  # "ilike" is case insensitive. "like" is case sensitive.
        if auditee_city is not None:
            params['auditee_city'] = f"eq.{auditee_city}"
        if auditee_state is not None:
            params['auditee_state'] = f"eq.{auditee_state.strip().upper()}"  # State abbreviations and not state names.
        if audit_year is not None:
            params['audit_year'] = f"eq.{audit_year}"
        
        return self._make_request(endpoint_name='general', params=params, handle_429=handle_429)

    def get_all_general(self, show_progress: bool = False):
        all_results = []
        for year in range(self.min_audit_year, self.max_audit_year + 1):
            for state in self.all_auditee_states:
                if show_progress:
                    print(f"Processing {year}-{state}...")
                results = self.get_general(audit_year=year, auditee_state=state, handle_429=True)
                all_results.extend(results)
        if show_progress:
            print(f"Total records retrieved: {len(all_results)}")
        return all_results


#%%
# Test code.
if __name__ == "__main__":
    results = FACClient().get_all_general(show_progress=True)


#%%