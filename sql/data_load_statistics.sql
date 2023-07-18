--т.к. источник у нас 1 и логика рассчетов очень похожа,
--можно сделать процедуру, которая на вход принимает одну из таблиц в которой нужно собрать статистику
CREATE OR REPLACE PROCEDURE anlz.data_load_statistics(target_table VARCHAR(32))
    RETURNS VARCHAR(32)
    LANGUAGE SQL
AS
$$
BEGIN
    CASE
        WHEN target_table = 'anlz.smokers_people' THEN
--т.к. нет привязки к датам сбора статистики в исходном датасете, данные гружу удаляя предыдущую загрузку
            INSERT OVERWRITE INTO anlz.smokers_people (
                region,
                smokers_people)
            SELECT region,
                   COUNT(1) AS smokers_people
              FROM core.INSURANCE
             WHERE smoker = 'yes'
          GROUP BY region;
        WHEN target_table = 'anlz.obesity_people' THEN
            INSERT OVERWRITE INTO anlz.obesity_people (
                region,
                obesity_people)
            SELECT region,
                   COUNT(1) AS obesity_people
              FROM core.insurance
             WHERE bmi > 30
          GROUP BY region;
        ELSE
           INSERT OVERWRITE INTO anlz.ins_pays (
               age,
               ins_pays)
           SELECT age,
                  SUM(charges) AS ins_pays
             FROM core.INSURANCE
         GROUP BY age;
    END CASE;
    COMMIT;
    RETURN 'Data transferred successfully.';
END;
$$;