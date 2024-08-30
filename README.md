# NIV Processing

This project automatically collects and processes nonimmigrant visa (NIV) data from various sources. It regularly updates a Snowflake database with the latest visa statistics, including issuances by post and visa class. This data can be used for analysis, reporting, or tracking trends in visa issuances over time.

## Requirements

See `requirements.txt` for dependencies.

## Structure

Multiple Python files for data processing.

## Configuration

Set environment variables in Heroku dashboard:
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA
- SNOWFLAKE_TABLE

## Deployment

Deployed on Heroku. Runs daily.

## Functionality

1. Scrapes visa data from various sources
2. Processes data in different formats
3. Updates Snowflake table with new data