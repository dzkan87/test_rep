CREATE OR REPLACE PROCEDURE core.data_load_ratings ()
 RETURNS VARCHAR()
 LANGUAGE SQL
 AS
$$
BEGIN
     MERGE INTO core.ratings cr
     USING stage.ratings_all sr
        ON cr.id = sr.all_data:id::INT
      WHEN NOT MATCHED
      THEN
     INSERT (
         id,
         name,
         year,
         metacritic_rating,
         reviewer_rating,
         positivity_ratio,
         to_beat_main,
         to_beat_extra,
         to_beat_completionist,
         extra_content_length,
         tags,
         source_table,
         insert_date)
     VALUES (sr.all_data:id::INT,
             sr.all_data:name::VARCHAR,
             COALESCE(NULLIF(sr.all_data:year, ''), 0)::INT, --Тут и далее встречаются NULL
             COALESCE(NULLIF(sr.all_data:metacritic_rating, ''), 0)::INT,
             COALESCE(NULLIF(sr.all_data:reviewer_rating, ''), 0)::INT,
             COALESCE(NULLIF(sr.all_data:positivity_ratio, ''), 0)::NUMERIC(17, 14),
             COALESCE(NULLIF(sr.all_data:to_beat_main, ''), 0)::NUMERIC(6, 2),
             COALESCE(NULLIF(sr.all_data:to_beat_extra, ''), 0)::NUMERIC(6, 2),
             COALESCE(NULLIF(sr.all_data:to_beat_completionist, ''), 0)::NUMERIC(7, 2),
             COALESCE(NULLIF(sr.all_data:extra_content_length, ''), 0)::NUMERIC(7, 2),
             all_data:tags::VARCHAR(2048),
             'stage.ratings_all'::VARCHAR(32),
             CURRENT_DATE()::DATE);
COMMIT;
RETURN 'Data transferred successfully.';
END;
$$;