CREATE TABLE IF NOT EXISTS stage.ratings_all (
    all_data VARIANT
);

CREATE TABLE IF NOT EXISTS core.ratings (
    id INT,
    name VARCHAR(256),
    year INT,
    metacritic_rating INT,
    reviewer_rating INT,
    positivity_ratio NUMERIC(17, 14),
    to_beat_main NUMERIC(6, 2),
    to_beat_extra NUMERIC(6, 2),
    to_beat_completionist NUMERIC(7, 2),
    extra_content_length NUMERIC(7, 2),
    tags VARCHAR(2048),
    source_table VARCHAR(32),
    insert_date DATE);

CREATE TABLE IF NOT EXISTS anlz.max_ratings (
    year INT,
    max_metacritic_rating INT,
    max_reviewer_rating INT);
