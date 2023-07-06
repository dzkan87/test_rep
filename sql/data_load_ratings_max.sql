CREATE OR REPLACE PROCEDURE anlz.data_load_ratings_max()
 RETURNS VARCHAR()
 LANGUAGE SQL
 AS
$$
BEGIN
    TRUNCATE TABLE anlz.max_ratings;
    INSERT INTO anlz.max_ratings (
        year,
        max_metacritic_rating,
        max_reviewer_rating)
    SELECT year,
           COALESCE(MAX(metacritic_rating), 0) AS max_metacritic_rating,
           MAX(reviewer_rating) AS max_reviewer_rating
      FROM core.ratings
--Данные по годам с 1997 по 2023 без пропусков.
--Возможно это 1996. Платформа запущена в 2003 году, поэтому данные ниже точно взяты с головы и т.к. они без пропуска
--строку решил не загружать, т.к. для анализа она не будет использована. На всех предыдущих этапах она есть.
     WHERE year <> 0
  GROUP BY year;
COMMIT;
RETURN 'Data transferred successfully.';
END;
$$;