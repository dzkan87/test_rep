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

CREATE TABLE IF NOT EXISTS anlz.smokers_people (
    region VARCHAR(16),
    smokers_people INT);

CREATE TABLE IF NOT EXISTS anlz.obesity_people (
    region VARCHAR(16),
    obesity_people INT);

CREATE TABLE IF NOT EXISTS anlz.ins_pays (
    age INT,
    ins_pays NUMERIC(17,7));