CREATE OR REPLACE PROCEDURE core.data_load_insurance()
    RETURNS VARCHAR(32)
    LANGUAGE SQL
 AS
$$
BEGIN
    INSERT OVERWRITE INTO core.insurance(
        age,
        sex,
        bmi,
        children,
        smoker,
        region,
        charges,
        source_table)
--избавляемся от дублирующихся строк(исходя из характера данных, считаю, что дублями могут быть только целые строки)
    SELECT DISTINCT
           SPLIT_PART(all_data, ',', 1)::INT AS age,
           SPLIT_PART(all_data, ',', 2)::VARCHAR(8) AS sex,
           SPLIT_PART(all_data, ',', 3)::NUMERIC(9,4) AS bmi,
           SPLIT_PART(all_data, ',', 4)::INT AS children,
           SPLIT_PART(all_data, ',', 5)::VARCHAR(4) AS smoker,
           SPLIT_PART(all_data, ',', 6)::VARCHAR(16) AS region,
           SPLIT_PART(all_data, ',', 7)::NUMERIC(17,7) AS charges,
           'stage.insurance'::VARCHAR(16) AS source_table
      FROM stage.insurance
--в случае null значений в любом из полей таблицы--можно считать такие строки как неинформативные
     WHERE SPLIT_PART(all_data, ',', 1) IS NOT NULL
        OR SPLIT_PART(all_data, ',', 2) IS NOT NULL
        OR SPLIT_PART(all_data, ',', 3) IS NOT NULL
        OR SPLIT_PART(all_data, ',', 4) IS NOT NULL
        OR SPLIT_PART(all_data, ',', 5) IS NOT NULL
        OR SPLIT_PART(all_data, ',', 6) IS NOT NULL
        OR SPLIT_PART(all_data, ',', 7) IS NOT NULL;
COMMIT;
RETURN 'Data transferred successfully.';
END;
$$;