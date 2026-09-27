CREATE STAGE IF NOT EXISTS CYBER_INSURANCE.RAW.SECURITY_EVENTS_STAGE
    STORAGE_INTEGRATION = s3_kaggle_int
    URL = 's3://kevin-data-engineering-project/raw/security_events/'
    FILE_FORMAT = (
        TYPE = JSON
    );


CREATE STAGE IF NOT EXISTS
    CYBER_INSURANCE.RAW.CUSTOMERS_STAGE
    STORAGE_INTEGRATION = S3_KAGGLE_INT
    URL = 's3://kevin-data-engineering-project/raw/customers/'
    FILE_FORMAT = (TYPE = JSON);