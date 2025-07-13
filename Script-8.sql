-- Инсайт 1: Топ жанров по популярности (MATERIALIZED VIEW)
CREATE MATERIALIZED VIEW evgeniy_zubkov.mv_genre_popularity AS
SELECT 
    g.genre_name,
    COUNT(t.track_id) as total_tracks,
    SUM(t.monthly_listens) as total_monthly_listens,
    AVG(t.monthly_listens) as avg_monthly_listens,
    MAX(t.monthly_listens) as max_monthly_listens,
    ROUND(AVG(t.track_length_seconds)::numeric, 2) as avg_track_length_seconds,
    COUNT(CASE WHEN t.is_explicit THEN 1 END) as explicit_tracks_count,
    ROUND(
        COUNT(CASE WHEN t.is_explicit THEN 1 END) * 100.0 / COUNT(t.track_id), 2
    ) as explicit_percentage
FROM evgeniy_zubkov.genres g
LEFT JOIN evgeniy_zubkov.tracks t ON g.genre_id = t.genre_id
GROUP BY g.genre_id, g.genre_name
ORDER BY total_monthly_listens DESC;

-- Создание индекса для MATERIALIZED VIEW
CREATE INDEX idx_mv_genre_popularity_listens ON evgeniy_zubkov.mv_genre_popularity(total_monthly_listens DESC);

-- Инсайт 2: Анализ исполнителей и их треков (VIEW)
CREATE VIEW evgeniy_zubkov.v_artists_analysis AS
SELECT
    a.artist_name,
    a.total_likes,
    COUNT(t.track_id) as tracks_count,
    SUM(t.monthly_listens) as total_monthly_listens,
    AVG(t.monthly_listens) as avg_monthly_listens_per_track,
    MAX(t.monthly_listens) as best_track_listens,
    MIN(t.monthly_listens) as worst_track_listens,
    ROUND(AVG(t.track_length_seconds)::numeric, 2) as avg_track_length,
    COUNT(DISTINCT t.genre_id) as genres_diversity,
    COUNT(DISTINCT t.chart_id) as charts_presence,
    -- Рейтинг эффективности: соотношение прослушиваний к лайкам
    CASE
        WHEN a.total_likes > 0 THEN ROUND((SUM(t.monthly_listens)::numeric / a.total_likes), 2)
        ELSE 0
    END as listens_per_like_ratio,
    -- Категоризация исполнителей
    CASE
        WHEN COUNT(t.track_id) >= 5 THEN 'Продуктивный'
        WHEN COUNT(t.track_id) >= 3 THEN 'Активный'
        WHEN COUNT(t.track_id) >= 1 THEN 'Начинающий'
        ELSE 'Неактивный'
    END as artist_category
FROM evgeniy_zubkov.artists a
LEFT JOIN evgeniy_zubkov.track_artists ta ON a.artist_id = ta.artist_id
LEFT JOIN evgeniy_zubkov.tracks t ON ta.track_id = t.track_id
GROUP BY a.artist_id, a.artist_name, a.total_likes
ORDER BY total_monthly_listens DESC;

-- Инсайт 3: Временной анализ и тренды (MATERIALIZED VIEW)
CREATE MATERIALIZED VIEW evgeniy_zubkov.mv_track_trends AS
WITH track_with_artists AS (
    SELECT 
        t.track_id,
        t.track_name,
        t.monthly_listens,
        t.track_length_seconds,
        t.is_explicit,
        t.genre_id,
        t.chart_id,
        -- Объединяем всех исполнителей через запятую
        string_agg(a.artist_name, ', ' ORDER BY a.artist_name) as artist_names
    FROM evgeniy_zubkov.tracks t
    LEFT JOIN evgeniy_zubkov.track_artists ta ON t.track_id = ta.track_id
    LEFT JOIN evgeniy_zubkov.artists a ON ta.artist_id = a.artist_id
    GROUP BY t.track_id, t.track_name, t.monthly_listens, t.track_length_seconds, t.is_explicit, t.genre_id, t.chart_id
),
track_metrics AS (
    SELECT
        twa.track_id,
        twa.track_name,
        twa.artist_names as artist_name,
        g.genre_name,
        c.chart_name,
        twa.monthly_listens,
        twa.track_length_seconds,
        twa.is_explicit,
        -- Остальные поля как в оригинале...
        CASE
            WHEN twa.track_length_seconds < 180 THEN 'Короткий (<3 мин)'
            WHEN twa.track_length_seconds BETWEEN 180 AND 240 THEN 'Средний (3-4 мин)'
            WHEN twa.track_length_seconds BETWEEN 240 AND 300 THEN 'Длинный (4-5 мин)'
            ELSE 'Очень длинный (>5 мин)'
        END as duration_category,
        CASE
            WHEN twa.monthly_listens >= 10000000 THEN 'Мега-хит'
            WHEN twa.monthly_listens >= 5000000 THEN 'Хит'
            WHEN twa.monthly_listens >= 1000000 THEN 'Популярный'
            WHEN twa.monthly_listens >= 100000 THEN 'Известный'
            ELSE 'Нишевый'
        END as popularity_category,
        PERCENT_RANK() OVER (ORDER BY twa.monthly_listens) * 100 as popularity_percentile
    FROM track_with_artists twa
    JOIN evgeniy_zubkov.genres g ON twa.genre_id = g.genre_id
    JOIN evgeniy_zubkov.charts c ON twa.chart_id = c.chart_id
)
SELECT
    tm.*,
    ROW_NUMBER() OVER (PARTITION BY tm.genre_name ORDER BY tm.monthly_listens DESC) as genre_rank,
    ROW_NUMBER() OVER (PARTITION BY tm.chart_name ORDER BY tm.monthly_listens DESC) as chart_rank,
    AVG(tm.monthly_listens) OVER (PARTITION BY tm.genre_name) as genre_avg_listens,
    ROUND(
        (tm.monthly_listens - AVG(tm.monthly_listens) OVER (PARTITION BY tm.genre_name)) /
        NULLIF(STDDEV(tm.monthly_listens) OVER (PARTITION BY tm.genre_name), 0) * 100, 2
    ) as genre_deviation_percent
FROM track_metrics tm
ORDER BY tm.monthly_listens DESC;

-- Создание индексов для оптимизации
CREATE INDEX idx_mv_track_trends_popularity ON evgeniy_zubkov.mv_track_trends(popularity_category);
CREATE INDEX idx_mv_track_trends_genre ON evgeniy_zubkov.mv_track_trends(genre_name);
CREATE INDEX idx_mv_track_trends_duration ON evgeniy_zubkov.mv_track_trends(duration_category);

-- Дополнительный инсайт: Топ комбинаций жанр-чарт
CREATE VIEW evgeniy_zubkov.v_genre_chart_combinations AS
SELECT 
    g.genre_name,
    c.chart_name,
    COUNT(t.track_id) as tracks_count,
    SUM(t.monthly_listens) as total_listens,
    AVG(t.monthly_listens) as avg_listens,
    MAX(t.monthly_listens) as max_listens,
    ROUND(AVG(t.track_length_seconds)::numeric, 2) as avg_duration,
    COUNT(CASE WHEN t.is_explicit THEN 1 END) as explicit_count,
    -- Доля этой комбинации в общем числе треков
    ROUND(
        COUNT(t.track_id) * 100.0 / SUM(COUNT(t.track_id)) OVER (), 2
    ) as market_share_percent
FROM evgeniy_zubkov.genres g
CROSS JOIN evgeniy_zubkov.charts c
LEFT JOIN evgeniy_zubkov.tracks t ON g.genre_id = t.genre_id AND c.chart_id = t.chart_id
GROUP BY g.genre_id, g.genre_name, c.chart_id, c.chart_name
HAVING COUNT(t.track_id) > 0
ORDER BY total_listens DESC;

-- Функция для обновления MATERIALIZED VIEW
CREATE OR REPLACE FUNCTION evgeniy_zubkov.refresh_analytics()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW evgeniy_zubkov.mv_genre_popularity;
    REFRESH MATERIALIZED VIEW evgeniy_zubkov.mv_track_trends;
    
    -- Логирование обновления
    INSERT INTO evgeniy_zubkov.system_log (action, description, created_at)
    VALUES ('REFRESH_ANALYTICS', 'Обновлены аналитические представления', CURRENT_TIMESTAMP);
END;
$$ LANGUAGE plpgsql;

-- Создание таблицы для логирования (если её нет)
CREATE TABLE IF NOT EXISTS evgeniy_zubkov.system_log (
    log_id SERIAL PRIMARY KEY,
    action VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Примеры запросов для анализа:

-- 1. Самые популярные жанры
-- SELECT * FROM evgeniy_zubkov.mv_genre_popularity LIMIT 10;

-- 2. Топ исполнителей по эффективности
-- SELECT artist_name, tracks_count, total_monthly_listens, listens_per_like_ratio, artist_category
-- FROM evgeniy_zubkov.v_artists_analysis 
-- WHERE listens_per_like_ratio > 0
-- ORDER BY listens_per_like_ratio DESC LIMIT 15;

-- 3. Анализ длительности треков по жанрам
-- SELECT genre_name, duration_category, COUNT(*) as count, 
--        AVG(monthly_listens) as avg_popularity
-- FROM evgeniy_zubkov.mv_track_trends
-- GROUP BY genre_name, duration_category
-- ORDER BY genre_name, avg_popularity DESC;

-- 4. Корреляция между explicit content и популярностью
-- SELECT 
--     is_explicit,
--     COUNT(*) as tracks_count,
--     AVG(monthly_listens) as avg_listens,
--     PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthly_listens) as median_listens
-- FROM evgeniy_zubkov.mv_track_trends
-- GROUP BY is_explicit;