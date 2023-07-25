--create storage integration
CREATE OR REPLACE STORAGE INTEGRATION snowflake_gsp_test
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake_gsp/');

 --create notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION test_load
   TYPE = QUEUE
   NOTIFICATION_PROVIDER = GCP_PUBSUB
   ENABLED = true
   GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/iron-atom-392011/subscriptions/snowpipe_subscription';

--create stage for data load
CREATE OR REPLACE STAGE stage.insurance_csv
    URL = 'gcs://snowflake_gsp/insurance/'
    STORAGE_INTEGRATION = snowflake_gsp_test;

--create pipe
CREATE OR REPLACE PIPE stage.load_finsurance_csv
    AUTO_INGEST = true
    INTEGRATION = 'TEST_LOAD'
    AS
    COPY INTO stage.insurance (all_data)
         FROM (SELECT $1::VARIANT
               FROM @stage.insurance_csv)
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = '\t',
    SKIP_HEADER = 1);

ALTER PIPE stage.load_finsurance_csv REFRESH;