#!/usr/bin/env python3
import asyncio
import logging
from projects.sg_weather.config.settings import get_settings
from projects.sg_weather.src.ingestion.storage import WeatherStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


async def clear_weather_tables():
    """Clear all weather-related tables."""
    storage = WeatherStorage(settings.database_url)

    try:
        await storage.connect()
        await storage.clear_tables()
        logger.info("Successfully cleared all weather tables")
    except Exception as e:
        logger.error(f"Error clearing tables: {e}")
        raise
    finally:
        await storage.disconnect()


if __name__ == "__main__":
    asyncio.run(clear_weather_tables())
