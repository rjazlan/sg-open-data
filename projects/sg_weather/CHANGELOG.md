# Singapore Weather Data Pipeline

## feat/weather-ingestion

### Design Philosophy

The weather data pipeline follows an ELT (Extract, Load, Transform) approach rather than a traditional ETL. This decision was made to:

- Preserve raw data integrity
- Enable flexible future transformations
- Reduce initial processing overhead
- Allow data reprocessing when needed

### Key Design Decisions

#### 1. Data validation first

**Why**: Ensure data quality at ingestion while maintaining raw format
**Implementation**:

- Pydantic 2.0 models for all API responses
- Shared base models for common structures
- Validation before storage, preserving original structure
- Automatic conversion of field names to Pythonic format

#### 2. Single Table Raw storage

**Why**: Simplify initial load while maintaining flexibility
**Implementation**:

- TimescaleDB hypertable for time-series optimization
- JSONB storage of validated data
- Consistent timestamp handling
- Parameter-based partitioning

#### 3. Async Processing

**Why**: Handle multiple API endpoints efficiently
**Implementation**:

- Async client with connection pooling
- Concurrent downloads with optional rate limiting
- Batch database operations
- Metadata cacheing
- Error handling with retries

#### 4. State management

**Why**: Enable reliable incremental updates
**Implementation**:

- JSON-based download tracking
- Parameter-specific state management
- Timestamp-based file organization
- Skip logic for existing data
  - Metadata tracking to reduce API downloads
  - File hash checking to prevent unnecessary transactions

#### 5. Storage Optimization

**Why**: Balance between performance and flexibility
**Implementation**:

- TimescaleDB for time-series data
- Hypertable partitioning by timestamp
- Efficient JSONB storage
- Minimal indexes for common queries

### Technical Implementation

#### Database Schema

```sql
CREATE TABLE raw_weather_data (
    data_timestamp TIMESTAMPTZ NOT NULL,
    data_type TEXT NOT NULL,
    parameter TEXT NOT NULL,
    validated_data JSONB NOT NULL,
    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (data_timestamp, data_type, parameter)
);

```

## Future Considerations

### Transformation layer

- Create materialized views for common queries
- Add aggregation functions
- Implement data cleanup processes
- Add derived metrics calculations

## Performance optimization

- Add index optimization
- implement data retention policies
- Add compression strategies
- Create query optimization layer
