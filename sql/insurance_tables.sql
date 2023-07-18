CREATE TABLE IF NOT EXISTS stage.insurance (
    all_data VARIANT);

CREATE TABLE IF NOT EXISTS core.insurance (
    age INT,
    sex VARCHAR(8),
    bmi NUMERIC(9,4),
    children INT,
    smoker VARCHAR(4),
    region VARCHAR(16),
    charges NUMERIC(17,7),
    source_table VARCHAR(16),
    insert_date DATE DEFAULT CURRENT_DATE());

CREATE OR REPLACE DYNAMIC TABLE anlz.smokers_people
TARGET_LAG = '24 hours'
WAREHOUSE = COMPUTE_WH
AS
    SELECT region,
           COUNT(1) AS smokers_people
      FROM core.insurance
     WHERE smoker = 'yes'
     GROUP BY region;

CREATE OR REPLACE DYNAMIC TABLE anlz.obesity_people
TARGET_LAG = '24 hours'
WAREHOUSE = COMPUTE_WH
AS
    SELECT region,
           COUNT(1) AS obesity_people
      FROM core.insurance
     WHERE bmi > 30
     GROUP BY region;

CREATE OR REPLACE DYNAMIC TABLE anlz.ins_pays
TARGET_LAG = '24 hours'
WAREHOUSE = COMPUTE_WH
AS
    SELECT age,
           SUM(charges) AS ins_pays
      FROM core.INSURANCE
     GROUP BY age;