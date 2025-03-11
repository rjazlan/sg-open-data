from datetime import datetime
from typing import Optional, Any, List
import aiohttp
import json
from aiohttp import ClientSession
from logging import getLogger
from projects.sg_weather.src.schemas.weather import (
    WeatherReadingData,
    TwoHourForecastData,
    TwentyFourHourOutlookData,
    FourDayOutlookData,
    LightningData,
    WBGTData,
    PM25Data,
    PSIData,
    UVIndexData,
    # WeatherResponse,
    # TwoHourForecastResponse,
    # TwentyFourHourOutlookResponse,
    # FourDayOutlookResponse,
    # LightningResponse,
    # PM25Response,
    # PSIResponse,
    # UVIndexResponse,
    # WBGTResponse,
)

logger = getLogger(__name__)


class WeatherAPIClient:
    """Client for the Singapore Weather Data API."""

    def __init__(self, base_url: str = "https://api-open.data.gov.sg/v2/real-time/api"):
        self.base_url = base_url
        self._session: Optional[ClientSession] = None

    async def __aenter__(self) -> "WeatherAPIClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def connect(self):
        """Initialize the aiohttp session."""
        if self._session is None:
            self._session = aiohttp.ClientSession()

    async def disconnect(self):
        """Close the aiohttp session."""
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _make_request(
        self, endpoint: str, params: Optional[dict[str, Any]] = None
    ) -> dict:
        """Make a request to the API."""
        if self._session is None:
            raise RuntimeError(
                "Client not connected. Use 'async with' or call connect()"
            )

        url = f"{self.base_url}{endpoint}"
        try:
            async with self._session.get(url, params=params) as response:
                response.raise_for_status()
                raw_data = await response.json()
                # Debug print
                # print(raw_data["code"], type(raw_data["code"]))
                # print(raw_data["errorMsg"], type(raw_data["errorMsg"]))
                # print(raw_data["data"], type(raw_data["data"]))
                # print(f"\nRaw API Response for {endpoint}:")
                # print(json.dumps(raw_data, indent=2) + "...")

                return raw_data
        except aiohttp.ClientError as e:
            logger.error(f"Error making request to {url}: {e}")
            raise

    async def fetch_paginated_api_results(
        self, endpoint: str, params: Optional[dict[str, Any]] = None
    ) -> dict:
        """
        Make paginated requests to the API until all data is retrieved.
        Preserves all data fields while aggregating paginated items/records/readings.
        """
        if self._session is None:
            raise RuntimeError(
                "Client not connected. Use 'async with' or call connect()"
            )

        params = params or {}
        page_count = 0
        total_items = 0
        seen_tokens = set()  # Track unique tokens
        token_sequence = []  # Preserve token order

        # Make initial request to get structure and first page
        first_response = await self._make_request(endpoint, params)

        # Initialize with first response data
        aggregated_data = first_response["data"].copy()

        # Track which key contains paginated items
        paginated_key = None
        if "items" in aggregated_data:
            paginated_key = "items"
        elif "records" in aggregated_data:
            paginated_key = "records"
        elif "readings" in aggregated_data:
            paginated_key = "readings"

        if not paginated_key:
            logger.info(f"No pagination detected for {endpoint}")
            # No pagination detected, return as is
            return first_response

        # Initialize list of paginated items
        all_items = aggregated_data[paginated_key]
        page_count += 1
        total_items += len(all_items)
        logger.info(
            f"Retrieved page {page_count} for {endpoint}: {len(all_items)} {paginated_key}"
        )

        # Log first page info
        current_token = aggregated_data.get("paginationToken")
        if current_token:
            seen_tokens.add(current_token)
            token_sequence.append(current_token)
            logger.info(
                f"Page {page_count} - New token: {current_token[:20]}... ({len(all_items)} {paginated_key}"
            )

        # Continue fetching if there's a pagination token
        while current_token:
            # update params for next request
            params["paginationToken"] = current_token

            try:
                # Get next page
                next_response = await self._make_request(endpoint, params)
                next_data = next_response["data"]

                # Verify response structure matches
                if paginated_key not in next_data:
                    logger.error(
                        f"Inconsistent response structure in page {page_count + 1}"
                    )
                    break

                # Get new token and check for cycles
                next_token = next_data.get("paginationToken")

                if next_token:
                    if next_token in seen_tokens:
                        logger.warning(
                            f"Duplicate token detected: {next_token[:20]}..."
                        )
                        logger.warning("Token sequence:")
                        for i, token in enumerate(token_sequence, 1):
                            logger.warning(f"  Page {i}: {token[:20]}...")
                        logger.warning(f"Duplicate found at page {page_count + 1}")
                        break
                    seen_tokens.add(next_token)
                    token_sequence.append(next_token)

                # Append paginated items
                new_items = next_data[paginated_key]
                all_items.extend(new_items)

                # Update counters and log progress
                page_count += 1
                total_items += len(new_items)
                logger.info(
                    f"Page {page_count} - {next_token[:20] if next_token else 'No'}"
                )

                # update token for next iteration
                current_token = next_token

                # Add rate limiting
                # await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error during paginated request to {endpoint}: {e}")
                break

        # Update final data with all items
        aggregated_data[paginated_key] = all_items

        # Remove pagination token from final resopnse
        aggregated_data.pop("paginationToken", None)

        # Log final statistics
        logger.info(f"\nPagination Summary for {endpoint}:")
        logger.info(f"- Total pages: {page_count}")
        logger.info(f"- Total {paginated_key}: {total_items}")
        logger.info(f"- Unique tokens: {len(seen_tokens)}")
        logger.info(f"- Token sequence length: {len(token_sequence)}")

        if len(seen_tokens) != len(token_sequence):
            logger.warning("Number of unique tokens doesn't match sequence length!")
            logger.warning("This suggests some tokens were repeated")

        # Perform basic validation
        if page_count > 1:
            logger.info("Validating aggregated data:")
            logger.info(
                f"- First page timestamp: {all_items[0].get('timestamp', 'N/A')}"
            )
            logger.info(
                f"- Last page timestamp: {all_items[-1].get('timestamp', 'N/A')}"
            )

        return aggregated_data

    async def get_temperature(
        self, date: Optional[datetime] = None
    ) -> WeatherReadingData:
        """Get temperature readings."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/air-temperature", params)
        return WeatherReadingData(**data)

    async def get_rainfall(self, date: Optional[datetime] = None) -> WeatherReadingData:
        """Get rainfall readings."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/rainfall", params)
        return WeatherReadingData(**data)

    async def get_humidity(self, date: Optional[datetime] = None) -> WeatherReadingData:
        """Get relative humidity readings."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/relative-humidity", params)
        return WeatherReadingData(**data)

    async def get_wind_speed(
        self, date: Optional[datetime] = None
    ) -> WeatherReadingData:
        """Get wind speed readings."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/wind-speed", params)
        return WeatherReadingData(**data)

    async def get_wind_direction(
        self, date: Optional[datetime] = None
    ) -> WeatherReadingData:
        """Get wind direction readings."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/wind-direction", params)
        return WeatherReadingData(**data)

    async def get_two_hour_forecast(
        self, date: Optional[datetime] = None
    ) -> TwoHourForecastData:
        """Get 2-hour weather forecast."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/two-hr-forecast", params)
        return TwoHourForecastData(**data)

    async def get_24_hour_forecast(
        self, date: Optional[datetime] = None
    ) -> TwentyFourHourOutlookData:
        """Get 24-hour weather forecast."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results(
            "/twenty-four-hr-forecast", params
        )
        return TwentyFourHourOutlookData(**data)

    async def get_four_day_forecast(
        self, date: Optional[datetime] = None
    ) -> FourDayOutlookData:
        """Get 4-day weather forecast."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/four-day-outlook", params)
        return FourDayOutlookData(**data)

    async def get_wbgt(self, date: Optional[datetime] = None) -> WBGTData:
        """Get WBGT (Wet Bulb Globe Temperature) readings."""
        params = {"date": date.isoformat() if date else None, "api": "wbgt"}
        data = await self.fetch_paginated_api_results("/weather", params)
        return WBGTData(**data)

    async def get_lightning(self, date: Optional[datetime] = None) -> LightningData:
        """Get lightning observations."""
        params = {"date": date.isoformat() if date else None, "api": "lightning"}
        data = await self.fetch_paginated_api_results("/weather", params)
        return LightningData(**data)

    async def get_pm25(self, date: Optional[datetime] = None) -> PM25Data:
        """Get PM2.5 readings."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/pm25", params)
        return PM25Data(**data)

    async def get_psi(self, date: Optional[datetime] = None) -> PSIData:
        """Get PSI readings."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/psi", params)
        return PSIData(**data)

    async def get_uv_index(self, date: Optional[datetime] = None) -> UVIndexData:
        """Get UV index readings."""
        params = {"date": date.isoformat()} if date else None
        data = await self.fetch_paginated_api_results("/uv", params)
        return UVIndexData(**data)
