-- Скрипт миграции данных из исходной таблицы в нормализованную структуру
-- Предполагается, что исходная таблица имеет название yandex_music_raw

-- Шаг 1: Проверка исходных данных
SELECT COUNT(*) as total_records, 
       COUNT(DISTINCT name) as unique_tracks,
       COUNT(DISTINCT "artist(s)") as unique_artists,
       COUNT(DISTINCT genre) as unique_genres,
       COUNT(DISTINCT chart) as unique_charts
FROM evgeniy_zubkov.yandex_music_raw;

-- Шаг 2: Анализ качества данных
SELECT 
    'name' as field, COUNT(*) as total, COUNT(name) as not_null, 
    COUNT(*) - COUNT(name) as null_count
FROM evgeniy_zubkov.yandex_music_raw
UNION ALL
SELECT 
    'artist' as field, COUNT(*) as total, COUNT("artist(s)") as not_null, 
    COUNT(*) - COUNT("artist(s)") as null_count
FROM evgeniy_zubkov.yandex_music_raw
UNION ALL
SELECT 
    'genre' as field, COUNT(*) as total, COUNT(genre) as not_null, 
    COUNT(*) - COUNT(genre) as null_count
FROM evgeniy_zubkov.yandex_music_raw
UNION ALL
SELECT 
    'chart' as field, COUNT(*) as total, COUNT(chart) as not_null, 
    COUNT(*) - COUNT(chart) as null_count
FROM evgeniy_zubkov.yandex_music_raw;

-- Шаг 3: Очистка и подготовка данных
-- Создание временной таблицы для очищенных данных
CREATE TEMP TABLE cleaned_raw_data AS
SELECT
    TRIM(name) as track_name,
    TRIM("artist(s)") as artist_name,
    TRIM(genre) as genre_name,
    TRIM(chart::text) as chart_name,  -- приведение bigint к text и затем TRIM
    COALESCE(track_len, '0') as track_length_seconds,  -- оставляем как text
    TRIM(link) as track_url,
    CASE
        WHEN "Explicit_content" = 1 THEN TRUE  -- исправлено название колонки
        ELSE FALSE
    END as is_explicit,
    COALESCE(monthly_listens_total, 0) as monthly_listens,
    COALESCE(artists_likes_total, 0) as artist_likes
FROM evgeniy_zubkov.yandex_music_raw
WHERE name IS NOT NULL
    AND "artist(s)" IS NOT NULL  -- исправлено название колонки
    AND genre IS NOT NULL
    AND chart IS NOT NULL;

-- Шаг 4: Заполнение справочника жанров
INSERT INTO evgeniy_zubkov.genres (genre_name)
SELECT DISTINCT genre_name 
FROM cleaned_raw_data 
WHERE genre_name IS NOT NULL AND genre_name != ''
ON CONFLICT (genre_name) DO NOTHING;

-- Проверка результата
SELECT 'Genres loaded: ' || COUNT(*) as result FROM evgeniy_zubkov.genres;

-- Шаг 5: Заполнение справочника исполнителей
INSERT INTO evgeniy_zubkov.artists (artist_name, total_likes)
SELECT DISTINCT 
    artist_name,
    MAX(artist_likes) as total_likes  -- берем максимальное значение лайков для артиста
FROM cleaned_raw_data 
WHERE artist_name IS NOT NULL AND artist_name != ''
GROUP BY artist_name;

-- Проверка результата
SELECT 'Artists loaded: ' || COUNT(*) as result FROM evgeniy_zubkov.artists;

-- Шаг 6: Заполнение справочника чартов
INSERT INTO evgeniy_zubkov.charts (chart_name, description)
SELECT DISTINCT 
    chart_name,
    'Чарт Яндекс.Музыки: ' || chart_name
FROM cleaned_raw_data 
WHERE chart_name IS NOT NULL AND chart_name != ''
ON CONFLICT (chart_name) DO NOTHING;

-- Проверка результата
SELECT 'Charts loaded: ' || COUNT(*) as result FROM evgeniy_zubkov.charts;


SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'evgeniy_zubkov';


SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'yandex_music_raw'
ORDER BY ordinal_position;

-- Шаг 7: Заполнение основной таблицы треков
-- Вставка треков с учетом связи многие ко многим через track_artists
INSERT INTO evgeniy_zubkov.tracks (
    track_name, 
    genre_id, 
    chart_id, 
    track_length_seconds, 
    track_url, 
    is_explicit, 
    monthly_listens, 
    chart_position
)
SELECT 
    ymr.name,
    g.genre_id,
    c.chart_id,
    -- Преобразование времени MM:SS в секунды
    (SPLIT_PART(ymr.track_len, ':', 1)::integer * 60 + SPLIT_PART(ymr.track_len, ':', 2)::integer),
    ymr.link,
    (ymr."Explicit_content" = 1),
    ymr.monthly_listens_total,
    ROW_NUMBER() OVER (
        PARTITION BY ymr.chart 
        ORDER BY ymr.monthly_listens_total DESC
    ) as chart_position
FROM yandex_music_raw ymr
JOIN evgeniy_zubkov.genres g ON g.genre_name = ymr.genre
JOIN evgeniy_zubkov.charts c ON c.chart_name = ymr.chart::text
ON CONFLICT DO NOTHING;

-- Вставка связей трек-исполнитель в промежуточную таблицу
INSERT INTO evgeniy_zubkov.track_artists (track_id, artist_id)
SELECT DISTINCT
    t.track_id,
    a.artist_id
FROM yandex_music_raw ymr
JOIN evgeniy_zubkov.tracks t ON t.track_name = ymr.name
JOIN evgeniy_zubkov.artists a ON a.artist_name = ymr."artist(s)"
ON CONFLICT (track_id, artist_id) DO NOTHING;

-- Проверка результата
SELECT 'Tracks loaded: ' || COUNT(*) as result FROM evgeniy_zubkov.tracks;

-- Шаг 8: Обновление MATERIALIZED VIEW
REFRESH MATERIALIZED VIEW evgeniy_zubkov.mv_genre_popularity;
REFRESH MATERIALIZED VIEW evgeniy_zubkov.mv_track_trends;

-- Шаг 9: Тестирование триггеров аудита
-- Добавим новый жанр для проверки
INSERT INTO evgeniy_zubkov.genres (genre_name) VALUES ('Тестовый жанр');

-- Изменим жанр
UPDATE evgeniy_zubkov.genres 
SET genre_name = 'Тестовый жанр (обновлен)' 
WHERE genre_name = 'Тестовый жанр';

-- Удалим жанр
DELETE FROM evgeniy_zubkov.genres 
WHERE genre_name = 'Тестовый жанр (обновлен)';

-- Проверим аудит
SELECT * FROM evgeniy_zubkov.fnc_audit('genres');

-- Шаг 10: Финальная проверка целостности данных
SELECT 
    'Total tracks' as metric, COUNT(*) as value FROM evgeniy_zubkov.tracks
UNION ALL
SELECT 
    'Total artists' as metric, COUNT(*) as value FROM evgeniy_zubkov.artists
UNION ALL
SELECT 
    'Total genres' as metric, COUNT(*) as value FROM evgeniy_zubkov.genres
UNION ALL
SELECT 
    'Total charts' as metric, COUNT(*) as value FROM evgeniy_zubkov.charts
UNION ALL
SELECT 
    'Audit records' as metric, COUNT(*) as value FROM evgeniy_zubkov.genres_audit;

-- Проверка внешних ключей
-- Data quality check for missing relationships
SELECT 
    'Tracks without artist' as issue, 
    COUNT(*) as count
FROM evgeniy_zubkov.tracks t
LEFT JOIN evgeniy_zubkov.track_artists ta ON t.track_id = ta.track_id
WHERE ta.track_id IS NULL

UNION ALL

SELECT 
    'Tracks without genre' as issue, 
    COUNT(*) as count
FROM evgeniy_zubkov.tracks t
LEFT JOIN evgeniy_zubkov.genres g ON t.genre_id = g.genre_id
WHERE t.genre_id IS NULL OR g.genre_id IS NULL

UNION ALL

SELECT 
    'Tracks without chart' as issue, 
    COUNT(*) as count
FROM evgeniy_zubkov.tracks t
LEFT JOIN evgeniy_zubkov.charts c ON t.chart_id = c.chart_id
WHERE t.chart_id IS NULL OR c.chart_id IS NULL;

-- Дополнительные статистики для презентации
SELECT 
    'Самый популярный жанр' as metric,
    genre_name as value
FROM evgeniy_zubkov.mv_genre_popularity
ORDER BY total_monthly_listens DESC
LIMIT 1;

SELECT 
    'Самый продуктивный исполнитель' as metric,
    artist_name as value
FROM evgeniy_zubkov.v_artists_analysis
ORDER BY tracks_count DESC
LIMIT 1;

SELECT 
    'Средняя длительность трека' as metric,
    ROUND(AVG(track_length_seconds)::numeric, 2) || ' сек' as value
FROM evgeniy_zubkov.tracks;