"""
SearchCarriers API client for fetching insurance and authority data.
Implements rate limiting, error handling, and data normalization.
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class SearchCarriersClient:
    """Client for interacting with the SearchCarriers API.
    
    Handles authentication, rate limiting, retries, and data fetching
    for insurance history, authority status, and compliance information.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the SearchCarriers client.
        
        Args:
            api_key: API key for authentication. If not provided, uses SEARCH_CARRIERS_API_TOKEN env var
        """
        self.api_key = api_key or os.getenv('SEARCH_CARRIERS_API_TOKEN') or os.getenv('SEARCHCARRIERS_API_KEY')
        if not self.api_key:
            raise ValueError("SearchCarriers API key is required. Set SEARCH_CARRIERS_API_TOKEN environment variable.")
        
        self.base_url = "https://searchcarriers.com/api"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Rate limiting configuration
        self.rate_limit_delay = 1.0  # Seconds between requests
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Implement rate limiting to respect API limits."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make a rate-limited request to the API.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            dict: JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: On API errors
        """
        self._rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        logger.info(f"Making request to {endpoint}")
        
        try:
            response = self.session.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Resource not found: {endpoint}")
                return {"data": [], "error": "Not found"}
            elif e.response.status_code == 429:
                logger.warning("Rate limit exceeded, backing off...")
                time.sleep(5)  # Extra backoff for rate limit
                return self._make_request(endpoint, params)  # Retry
            else:
                logger.error(f"API error: {e}")
                raise
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def get_carrier_insurance_history(self, dot_number: int, 
                                           page: int = 1, 
                                           per_page: int = 100) -> Dict:
        """Fetch current and historical insurance information for a carrier.
        
        Uses the v2 endpoint for comprehensive insurance data.
        
        Args:
            dot_number: USDOT number of the carrier
            page: Page number for pagination
            per_page: Number of results per page
            
        Returns:
            dict: Insurance history data including policies and providers
        """
        endpoint = f"/v2/company/{dot_number}/insurances"
        params = {"page": page, "perPage": per_page}
        
        logger.info(f"Fetching insurance history for DOT {dot_number}")
        result = self._make_request(endpoint, params)
        
        # Normalize the response
        if "data" in result:
            insurance_records = result["data"]
            logger.info(f"Found {len(insurance_records)} insurance records for DOT {dot_number}")
            
            # Parse and enhance the data
            for record in insurance_records:
                record["dot_number"] = dot_number
                record["fetched_at"] = datetime.utcnow().isoformat()
                
                # Ensure date fields are properly formatted
                for date_field in ["effective_date", "expiration_date", "cancellation_date"]:
                    if date_field in record and record[date_field]:
                        try:
                            # Parse and reformat date to ensure consistency
                            if isinstance(record[date_field], str):
                                # Handle various date formats
                                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"]:
                                    try:
                                        dt = datetime.strptime(record[date_field], fmt)
                                        record[date_field] = dt.date().isoformat()
                                        break
                                    except ValueError:
                                        continue
                        except Exception as e:
                            logger.warning(f"Could not parse date {record[date_field]}: {e}")
        
        return result
    
    def get_authority_history(self, docket_number: str,
                                   page: int = 1,
                                   per_page: int = 100) -> Dict:
        """Fetch authority history for a given MC, FF, or MX number.
        
        Args:
            docket_number: MC, FF, or MX number
            page: Page number for pagination
            per_page: Number of results per page
            
        Returns:
            dict: Authority history including status changes
        """
        endpoint = f"/v1/authority/{docket_number}/history"
        params = {"page": page, "perPage": per_page}
        
        logger.info(f"Fetching authority history for {docket_number}")
        result = self._make_request(endpoint, params)
        
        if "data" in result:
            logger.info(f"Found {len(result['data'])} authority events for {docket_number}")
        
        return result
    
    def check_insurance_compliance(self, dot_number: int) -> Dict:
        """Check if a carrier meets insurance compliance requirements.
        
        Args:
            dot_number: USDOT number of the carrier
            
        Returns:
            dict: Compliance status including violations and requirements
        """
        # Get current insurance
        insurance_data = self.get_carrier_insurance_history(dot_number, per_page=10)
        
        compliance_result = {
            "dot_number": dot_number,
            "is_compliant": True,
            "violations": [],
            "current_coverage": None,
            "required_minimum": 750000.0,  # Default for general freight
            "checked_at": datetime.utcnow().isoformat()
        }
        
        if not insurance_data.get("data"):
            compliance_result["is_compliant"] = False
            compliance_result["violations"].append({
                "type": "NO_INSURANCE",
                "description": "No active insurance found",
                "severity": "CRITICAL"
            })
            return compliance_result
        
        # Find active policies
        active_policies = []
        for policy in insurance_data["data"]:
            if policy.get("filing_status") == "ACTIVE":
                active_policies.append(policy)
        
        if not active_policies:
            compliance_result["is_compliant"] = False
            compliance_result["violations"].append({
                "type": "NO_ACTIVE_INSURANCE",
                "description": "No active insurance policies",
                "severity": "CRITICAL"
            })
        else:
            # Check coverage amounts
            max_coverage = max(p.get("coverage_amount", 0) for p in active_policies)
            compliance_result["current_coverage"] = max_coverage
            
            if max_coverage < compliance_result["required_minimum"]:
                compliance_result["is_compliant"] = False
                compliance_result["violations"].append({
                    "type": "UNDERINSURED",
                    "description": f"Coverage ${max_coverage:,.0f} below minimum ${compliance_result['required_minimum']:,.0f}",
                    "severity": "HIGH"
                })
        
        return compliance_result
    
    def detect_coverage_gaps(self, insurance_history: List[Dict]) -> List[Dict]:
        """Detect gaps in insurance coverage from historical data.
        
        Args:
            insurance_history: List of insurance policies with dates
            
        Returns:
            list: Detected coverage gaps with details
        """
        if not insurance_history or len(insurance_history) < 2:
            return []
        
        # Sort by effective date
        sorted_policies = sorted(
            insurance_history,
            key=lambda x: x.get("effective_date", "")
        )
        
        gaps = []
        
        for i in range(len(sorted_policies) - 1):
            current = sorted_policies[i]
            next_policy = sorted_policies[i + 1]
            
            # Determine end date of current policy
            end_date = current.get("cancellation_date") or current.get("expiration_date")
            if not end_date:
                continue
            
            # Parse dates
            try:
                end = datetime.fromisoformat(end_date).date()
                start = datetime.fromisoformat(next_policy["effective_date"]).date()
                
                gap_days = (start - end).days
                
                if gap_days > 0:
                    gaps.append({
                        "from_policy": current.get("policy_id"),
                        "to_policy": next_policy.get("policy_id"),
                        "gap_start": end_date,
                        "gap_end": next_policy["effective_date"],
                        "gap_days": gap_days,
                        "from_provider": current.get("provider_name"),
                        "to_provider": next_policy.get("provider_name"),
                        "is_violation": gap_days > 30  # Federal requirement
                    })
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not calculate gap: {e}")
                continue
        
        return gaps
    
    def detect_provider_shopping(self, insurance_history: List[Dict],
                                months_window: int = 12) -> Dict:
        """Detect insurance shopping patterns from historical data.
        
        Args:
            insurance_history: List of insurance policies
            months_window: Time window to check for provider changes
            
        Returns:
            dict: Analysis of provider shopping behavior
        """
        if not insurance_history:
            return {"is_shopping": False, "provider_count": 0}
        
        # Filter policies within the time window
        cutoff_date = datetime.now().date() - timedelta(days=months_window * 30)
        recent_policies = []
        
        for policy in insurance_history:
            try:
                effective_date = datetime.fromisoformat(policy["effective_date"]).date()
                if effective_date >= cutoff_date:
                    recent_policies.append(policy)
            except (ValueError, KeyError):
                continue
        
        # Count unique providers
        providers = set()
        for policy in recent_policies:
            if "provider_name" in policy:
                providers.add(policy["provider_name"])
        
        provider_count = len(providers)
        
        return {
            "is_shopping": provider_count >= 3,
            "provider_count": provider_count,
            "providers": list(providers),
            "months_window": months_window,
            "policy_count": len(recent_policies),
            "risk_score": min(provider_count / 3.0, 1.0)  # Normalize to 0-1
        }
    
    def get_carrier_authorities(self, dot_number: int) -> Dict:
        """Get all authorities associated with a carrier.
        
        Args:
            dot_number: USDOT number of the carrier
            
        Returns:
            dict: List of authorities and their statuses
        """
        endpoint = f"/v1/company/{dot_number}/authorities"
        params = {"perPage": 100}
        
        logger.info(f"Fetching authorities for DOT {dot_number}")
        return self._make_request(endpoint, params)
    
    def get_safety_summary(self, dot_number: int, since_months: int = 24) -> Dict:
        """Fetch comprehensive safety metrics for a carrier.
        
        Args:
            dot_number: USDOT number of the carrier
            since_months: Months to look back for metrics
            
        Returns:
            dict: Safety summary including OOS rates and SMS BASIC scores
        """
        endpoint = f"/v1/company/{dot_number}/safety-summary"
        params = {"sinceMonths": since_months}
        
        logger.info(f"Fetching safety summary for DOT {dot_number}")
        result = self._make_request(endpoint, params)
        
        if "data" in result and result["data"] and not isinstance(result["data"], list):
            safety_data = result["data"]
            logger.info(f"Retrieved safety metrics for DOT {dot_number}")
            
            # Normalize the response
            safety_data["dot_number"] = dot_number
            safety_data["fetched_at"] = datetime.now(timezone.utc).isoformat()
            
            # Add risk flags based on national averages
            if "driver_oos_rate" in safety_data:
                safety_data["driver_oos_high_risk"] = safety_data["driver_oos_rate"] > 10.0  # 2x national avg
            if "vehicle_oos_rate" in safety_data:
                safety_data["vehicle_oos_high_risk"] = safety_data["vehicle_oos_rate"] > 40.0  # 2x national avg
        
        return result
    
    def get_crashes(self, dot_number: int, page: int = 1, per_page: int = 100) -> Dict:
        """Fetch crash history for a carrier.
        
        Args:
            dot_number: USDOT number of the carrier
            page: Page number for pagination
            per_page: Number of results per page
            
        Returns:
            dict: Crash history with fatalities, injuries, and dates
        """
        endpoint = f"/v1/company/{dot_number}/crashes"
        params = {"page": page, "perPage": per_page}
        
        logger.info(f"Fetching crash history for DOT {dot_number}")
        result = self._make_request(endpoint, params)
        
        if "data" in result and isinstance(result["data"], list):
            crashes = result["data"]
            logger.info(f"Found {len(crashes)} crashes for DOT {dot_number}")
            
            # Enhance crash data
            for crash in crashes:
                crash["dot_number"] = dot_number
                crash["fetched_at"] = datetime.now(timezone.utc).isoformat()
                
                # Determine severity level
                if crash.get("fatalities", 0) > 0:
                    crash["severity_level"] = "FATAL"
                elif crash.get("injuries", 0) > 0:
                    crash["severity_level"] = "INJURY"
                else:
                    crash["severity_level"] = "PROPERTY"
        
        return result
    
    def get_inspections(self, dot_number: int, since_months: int = 24, 
                       page: int = 1, per_page: int = 100) -> Dict:
        """Fetch inspection records with violations for a carrier.
        
        Args:
            dot_number: USDOT number of the carrier
            since_months: Months to look back for inspections
            page: Page number for pagination
            per_page: Number of results per page
            
        Returns:
            dict: Inspection records with violation details
        """
        endpoint = f"/v1/company/{dot_number}/inspections"
        params = {
            "sinceMonths": since_months,
            "page": page,
            "perPage": per_page
        }
        
        logger.info(f"Fetching inspections for DOT {dot_number}")
        result = self._make_request(endpoint, params)
        
        if "data" in result and isinstance(result["data"], list):
            inspections = result["data"]
            logger.info(f"Found {len(inspections)} inspections for DOT {dot_number}")
            
            # Process inspection data
            for inspection in inspections:
                inspection["dot_number"] = dot_number
                inspection["fetched_at"] = datetime.now(timezone.utc).isoformat()
                
                # Categorize inspection result
                if inspection.get("oos_count", 0) > 0:
                    inspection["result"] = "OOS"
                elif inspection.get("violations_count", 0) > 0:
                    inspection["result"] = "Violations"
                else:
                    inspection["result"] = "Clean"
        
        return result
    
    def get_out_of_service_orders(self, dot_number: int, 
                                 page: int = 1, per_page: int = 100) -> Dict:
        """Fetch out-of-service orders for a carrier.
        
        Args:
            dot_number: USDOT number of the carrier
            page: Page number for pagination
            per_page: Number of results per page
            
        Returns:
            dict: Out-of-service violations and orders
        """
        endpoint = f"/v1/company/{dot_number}/out-of-service-orders"
        params = {"page": page, "perPage": per_page}
        
        logger.info(f"Fetching OOS orders for DOT {dot_number}")
        result = self._make_request(endpoint, params)
        
        if "data" in result and isinstance(result["data"], list):
            oos_orders = result["data"]
            logger.info(f"Found {len(oos_orders)} OOS orders for DOT {dot_number}")
            
            # Add metadata
            for order in oos_orders:
                order["dot_number"] = dot_number
                order["fetched_at"] = datetime.now(timezone.utc).isoformat()
                order["is_critical"] = True  # All OOS orders are critical
        
        return result
    
    def batch_enrich_carriers(self, dot_numbers: List[int],
                                   delay_seconds: float = 1.0) -> List[Dict]:
        """Batch process multiple carriers with rate limiting.
        
        Args:
            dot_numbers: List of USDOT numbers to process
            delay_seconds: Delay between requests
            
        Returns:
            list: Enriched data for all carriers
        """
        results = []
        total = len(dot_numbers)
        
        for i, dot in enumerate(dot_numbers, 1):
            logger.info(f"Processing carrier {i}/{total}: DOT {dot}")
            
            try:
                # Fetch insurance history
                insurance = self.get_carrier_insurance_history(dot)
                
                # Check compliance
                compliance = self.check_insurance_compliance(dot)
                
                # Detect patterns
                gaps = []
                shopping = {}
                if insurance.get("data"):
                    gaps = self.detect_coverage_gaps(insurance["data"])
                    shopping = self.detect_provider_shopping(insurance["data"])
                
                results.append({
                    "dot_number": dot,
                    "insurance_history": insurance.get("data", []),
                    "compliance": compliance,
                    "coverage_gaps": gaps,
                    "provider_shopping": shopping,
                    "enriched_at": datetime.now(timezone.utc).isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error processing DOT {dot}: {e}")
                results.append({
                    "dot_number": dot,
                    "error": str(e),
                    "enriched_at": datetime.now(timezone.utc).isoformat()
                })
            
            # Rate limiting between carriers
            if i < total:
                time.sleep(delay_seconds)
        
        return results