#!/usr/bin/env python3
import asyncio
import asyncpg
from projects.sg_weather.config.settings import get_settings


async def test_connection():
    settings = get_settings()
    print(f"Testing connection to: {settings.DB_HOST}")
    print(f"Database: {settings.DB_NAME}")
    print(f"User: {settings.DB_USER}")

    try:
        # Try to establish connection
        conn = await asyncpg.connect(settings.database_url)
        print("\n✅ Successfully connected to database!")

        # Test permissions by creating and dropping a test table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS test_connection (
                id serial PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        print("✅ Successfully created test table")

        await conn.execute("DROP TABLE test_connection")
        print("✅ Successfully dropped test table")

        # Test extensions
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        print("✅ TimescaleDB extension available")

        await conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        print("✅ PostGIS extension available")

        await conn.close()

    except Exception as e:
        print("\n❌ Connection test failed!")
        print(f"Error: {str(e)}")
        raise
    finally:
        try:
            await conn.close()
        except:
            pass


if __name__ == "__main__":
    asyncio.run(test_connection())
