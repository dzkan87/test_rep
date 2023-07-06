CREATE OR REPLACE PROCEDURE core.data_load_ratings ()
 RETURNS VARCHAR()
 LANGUAGE SQL
 AS
$$
BEGIN
     INSERT INTO core.ratings (
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
     SELECT sr.all_data:id::INT AS id,
            sr.all_data:name::VARCHAR AS name,
            COALESCE(NULLIF(sr.all_data:year, ''), 0)::INT AS YEAR, --Тут и далее встречаются NULL
            COALESCE(NULLIF(sr.all_data:metacritic_rating, ''), 0)::INT AS metacritic_rating,
            COALESCE(NULLIF(sr.all_data:reviewer_rating, ''), 0)::INT AS reviewer_rating,
            COALESCE(NULLIF(sr.all_data:positivity_ratio, ''), 0)::NUMERIC(17, 14) AS positivity_ratio,
            COALESCE(NULLIF(sr.all_data:to_beat_main, ''), 0)::NUMERIC(6, 2) AS to_beat_main,
            COALESCE(NULLIF(sr.all_data:to_beat_extra, ''), 0)::NUMERIC(6, 2) AS to_beat_extra,
            COALESCE(NULLIF(sr.all_data:to_beat_completionist, ''), 0)::NUMERIC(7, 2) AS to_beat_completionist,
            COALESCE(NULLIF(sr.all_data:extra_content_length, ''), 0)::NUMERIC(7, 2) AS extra_content_length,
            all_data:tags::VARCHAR AS tags,
            'stage.ratings_all' AS source_table,
            current_date()::DATE AS date_insert
       FROM stage.ratings_all sr
--Выбрал такой подход, что бы не грузить дубли(если понадобиться перезапустить даг),
--не удалять предыдущие загруки. И исходя из размера датасета и времени переноса даже
--в размере хранилища SMALL. Протестировал на полностью загруженном и на разной степени загруженности датасета на stage-слой
WHERE NOT EXISTS (SELECT 1
                    FROM core.ratings cr
                   WHERE sr.all_data:id::INT = cr.id);
COMMIT;
RETURN 'Data transferred successfully.';
END;
$$;