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
                    , columns: List[str] | None = None
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
            columns: List of column names to select from the general endpoint. If None, all columns are returned.
            report_id: Specific SEFA report ID
            auditee_uei: UEI number tied to auditee
            auditee_ein: EIN number tied to auditee
            auditee_name: Name of auditee as it appears on the SEFA report
            auditee_city: City name where the auditee is located
            auditee_state: State abbreviation where the auditee is located
            audit_year: Audit year of SEFA report, typically lagged by one calendar year
            handle_429: If True, automatically retry on 429 errors using Retry-After header
        Returns:
            List of audit records from the general endpoint.
        """
        params = {}  # Initialize an empty dictionary for query parameters.
        if columns is not None:
            if isinstance(columns, list):
                params['select'] = ','.join(columns)
            else:
                raise TypeError(f"columns must be a list, got {type(columns).__name__}.")
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

    def get_all_general(self, columns: list[str] | None = None, show_progress: bool = False) -> list[Dict]:
        """
        Purpose:
            Collect all results from the \"general\" endpoint from the FAC database.
        Args:
            columns: List of column names to select from the general endpoint. If None, all columns are returned.
            show_progress: Boolean value to print out results in the terminal for testing reasons.
        Returns:
            all_results: A list of all of the dictionary responses that are returned from the API.
        """
        all_results = []  # Store all queried results from the FAC database.
        
        # The max number of results that can be pulled from the API is 20,000 results. Looping over time and states ensures no call asks for too many records.
        for year in range(self.min_audit_year, self.max_audit_year + 1):  # Loop from 2016 to the current year.
            for state in self.all_auditee_states:  # Loop across all states.
                if show_progress:  # Print out the current year and state being processed.
                    print(f"Processing {year}-{state}...")
                try:  # Exception handling for API calls.
                    results = self.get_general(columns=columns, audit_year=year, auditee_state=state, handle_429=True)  # Get results for the current year and state.
                    all_results.extend(results)  # Add pulled results to the all_results list.
                except APIError as e:
                    print(f"Error retrieving data for {year}-{state}: {e}")
        if show_progress:  # Return the total number of records retrieved.
            print(f"Total records retrieved: {len(all_results)}")
        return all_results

    def get_federal_awards(self
                           , columns: List[str] | None = None
                           , report_id: str | None = None
                           , federal_agency_prefix: str | None = None
                           , federal_award_extension: str | None = None
                           , additional_award_identification: str | None = None
                           , federal_program_name: str | None = None
                           , cluster_name: str | None = None
                           , handle_429: bool = False
                           ) -> List[Dict]:
        """
        Purpose:
            Get federal awards data for a specific report ID.
        Args:
            columns: List of column names to select from the federal_awards endpoint. If None, all columns are returned.
            report_id: Specific SEFA report ID
            federal_agency_prefix: Prefix of the federal agency. First two numbers of the federal assistance listing number.
            federal_award_extension: Extension of the federal award. Remaining numbers of the federal assistance listing number. Should only be used along with federal_agency_prefix.
            additional_award_identification: Additional award identification number
            federal_program_name: Name of the federal program
            cluster_name: Name of the cluster
            handle_429: If True, automatically retry on 429 errors using Retry-After header
        Returns:
            List of federal awards records.
        """
        params = {}  # Initialize an empty dictionary for query parameters.
        if columns is not None:
            if isinstance(columns, list):
                params['select'] = ','.join(columns)
            else:
                raise TypeError(f"columns must be a list, got {type(columns).__name__}.")
        if report_id is not None:
            params = {'report_id': f"eq.{report_id}"}
        if federal_agency_prefix is not None:
            params = {'federal_agency_prefix': f"eq.{federal_agency_prefix}"}
            if federal_award_extension is not None:
                params = {'federal_award_extension': f"eq.{federal_award_extension}"}
        if additional_award_identification is not None:
            params = {'additional_award_identification': f"eq.*{additional_award_identification}*"}
        if federal_program_name is not None:
            params = {'federal_program_name': f"ilike.*{federal_program_name}*"}  # "ilike" is case insensitive. "like" is case sensitive.
        if cluster_name is not None:
            params = {'cluster_name': f"ilike.*{cluster_name}*"}

        return self._make_request(endpoint_name='federal_awards', params=params, handle_429=handle_429)
    
    def get_all_federal_awards(self, batch_size: int = 250, show_progress: bool = False, save_progress: bool = False):
        """
        Purpose:
            Retrieve all federal award records by batching report_ids from general endpoint.
            This approach works around the federal_awards endpoint's limited filtering options.
        Args:
            batch_size: Number of report_ids to include in each API call (adjust to stay under 20K limit)
            show_progress: Whether to print progress updates
        Returns:
            List of all federal award records
        """
        if show_progress:
            print("Step 1: Getting all report_ids from general endpoint...")
        
        # Get all report id values from the general records
        try:
            report_id_records = self.get_all_general(columns=['report_id'], show_progress=show_progress)
            if show_progress:
                print(f"Retrieved {len(report_id_records)} report id records")
            report_ids = list(set([record['report_id'] for record in report_id_records if 'report_id' in record]))  # Extract unique report_ids
        except Exception as e:
            raise APIError(f"Failed to get general records: {e}")
        
        if show_progress:
            print(f"Step 2: Found {len(report_ids)} unique report_ids")
            print(f"Step 3: Processing in batches of {batch_size}...")
        
        all_results = []
        total_batches = (len(report_ids) + batch_size - 1) // batch_size  # Ceiling division

        failed_batches = []
    
        for i in range(0, len(report_ids), batch_size):
            batch_num = i // batch_size + 1
            batch_ids = report_ids[i:i + batch_size]

            if show_progress:
                print(f"Processing batch {batch_num}/{total_batches} ({len(batch_ids)} report_ids)...")

            # Add retry logic for network issues
            max_retries = 3
            retry_delay = 5  # seconds
            batch_success = False

            for attempt in range(max_retries):
                try:
                    # Create filter for this batch of report_ids
                    # PostgREST syntax: report_id=in.(id1,id2,id3,...)
                    id_filter = f"in.({','.join(batch_ids)})"

                    params = {'report_id': id_filter}
                    results = self._make_request(endpoint_name='federal_awards', params=params, handle_429=True)

                    all_results.extend(results)
                    batch_success = True

                    if show_progress:
                        print(f"  Found {len(results)} federal award records")

                    break  # Success, exit retry loop

                except APIError as e:
                    if attempt < max_retries - 1:  # Not the last attempt
                        if "Failed to connect" in str(e) or "Failed to resolve" in str(e) or "NameResolutionError" in str(e):
                            if show_progress:
                                print(f"  Network error on attempt {attempt + 1}, retrying in {retry_delay}s...")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                            continue
                        
                    # Last attempt or non-network error
                    if show_progress:
                        print(f"  Error processing batch {batch_num} (attempt {attempt + 1}): {e}")
                    break
                
            if not batch_success:
                failed_batches.append((batch_num, batch_ids))

            # Add small delay between batches to be nice to the API
            if batch_num < total_batches:
                time.sleep(0.5)

            # Save progress periodically
            if save_progress and batch_num % 100 == 0:
                if show_progress:
                    print(f"  Progress checkpoint: {len(all_results)} records collected so far")

        if show_progress:
            print(f"\nCompleted! Total federal award records retrieved: {len(all_results)}")
            if failed_batches:
                print(f"Warning: {len(failed_batches)} batches failed due to network issues")
                print("You may want to retry those specific batches")

    

#%%
# Test code.
if __name__ == "__main__":
    # city = FACClient().get_general(auditee_city='Vacaville', auditee_state='CA')
    # print(city)
    # report = FACClient().get_federal_awards(report_id='2023-06-GSAFAC-0000050078')
    # print(report)
    # gen_results = FACClient().get_all_general(show_progress=True)
    fed_results = FACClient().get_all_federal_awards(show_progress=True)


#%%