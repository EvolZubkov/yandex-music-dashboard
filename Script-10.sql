-- SQL запросы для создания дашбордов в SuperSet

-- Запрос 1: Топ жанров по популярности (для Bar Chart)
SELECT 
    genre_name as "Жанр",
    total_monthly_listens as "Общие прослушивания",
    total_tracks as "Количество треков",
    avg_monthly_listens as "Средние прослушивания",
    explicit_percentage as "% Explicit контента"
FROM evgeniy_zubkov.mv_genre_popularity
ORDER BY total_monthly_listens DESC
LIMIT 15;

-- Запрос 2: Анализ исполнителей (для Bubble Chart)
SELECT 
    artist_name as "Исполнитель",
    tracks_count as "Количество треков",
    total_monthly_listens as "Общие прослушивания",
    total_likes as "Общие лайки",
    listens_per_like_ratio as "Прослушиваний на лайк",
    artist_category as "Категория артиста",
    genres_diversity as "Разнообразие жанров"
FROM evgeniy_zubkov.v_artists_analysis
WHERE total_monthly_listens > 0
ORDER BY total_monthly_listens DESC
LIMIT 20;

-- Запрос 3: Распределение треков по длительности и популярности (для Scatter Plot)
SELECT 
    track_name as "Название трека",
    artist_name as "Исполнитель",
    genre_name as "Жанр",
    track_length_seconds as "Длительность (сек)",
    monthly_listens as "Месячные прослушивания",
    duration_category as "Категория длительности",
    popularity_category as "Категория популярности",
    popularity_percentile as "Процентиль популярности",
    is_explicit as "Explicit контент"
FROM evgeniy_zubkov.mv_track_trends
WHERE monthly_listens > 0
ORDER BY monthly_listens DESC
LIMIT 100;

-- Запрос 4: Heatmap жанров и чартов (для Heatmap)
SELECT 
    genre_name as "Жанр",
    chart_name as "Чарт",
    tracks_count as "Количество треков",
    total_listens as "Общие прослушивания",
    avg_listens as "Средние прослушивания",
    market_share_percent as "Доля рынка %"
FROM evgeniy_zubkov.v_genre_chart_combinations
ORDER BY total_listens DESC;

-- Запрос 5: Топ треков с деталями (для Table Chart)
SELECT
    t.track_name as "Название",
    a.artist_name as "Исполнитель",
    g.genre_name as "Жанр",
    c.chart_name as "Чарт",
    t.monthly_listens as "Месячные прослушивания",
    ROUND(t.track_length_seconds::numeric / 60, 2) as "Длительность (мин)",
    CASE WHEN t.is_explicit THEN 'Да' ELSE 'Нет' END as "Explicit",
    t.chart_position as "Позиция в чарте"
FROM evgeniy_zubkov.tracks t
JOIN evgeniy_zubkov.track_artists ta ON t.track_id = ta.track_id
JOIN evgeniy_zubkov.artists a ON ta.artist_id = a.artist_id
JOIN evgeniy_zubkov.genres g ON t.genre_id = g.genre_id
JOIN evgeniy_zubkov.charts c ON t.chart_id = c.chart_id
ORDER BY t.monthly_listens DESC
LIMIT 50;

-- Запрос 6: Анализ Explicit контента (для Pie Chart)
SELECT 
    CASE WHEN is_explicit THEN 'Explicit' ELSE 'Чистый' END as "Тип контента",
    COUNT(*) as "Количество треков",
    SUM(monthly_listens) as "Общие прослушивания",
    ROUND(AVG(monthly_listens)::numeric, 0) as "Средние прослушивания",
    ROUND(AVG(track_length_seconds)::numeric, 2) as "Средняя длительность"
FROM evgeniy_zubkov.mv_track_trends
GROUP BY is_explicit
ORDER BY "Общие прослушивания" DESC;

-- Запрос 7: Топ исполнителей по эффективности (для Horizontal Bar Chart)
SELECT 
    artist_name as "Исполнитель",
    total_monthly_listens as "Общие прослушивания",
    total_likes as "Общие лайки",
    tracks_count as "Количество треков",
    ROUND(avg_monthly_listens_per_track::numeric, 0) as "Средние прослушивания на трек",
    listens_per_like_ratio as "Коэффициент эффективности"
FROM evgeniy_zubkov.v_artists_analysis
WHERE listens_per_like_ratio > 0 AND total_monthly_listens > 1000000
ORDER BY listens_per_like_ratio DESC
LIMIT 15;

-- Запрос 8: Анализ длительности треков по жанрам (для Grouped Bar Chart)
SELECT 
    genre_name as "Жанр",
    duration_category as "Категория длительности",
    COUNT(*) as "Количество треков",
    ROUND(AVG(monthly_listens)::numeric, 0) as "Средняя популярность",
    ROUND(AVG(track_length_seconds)::numeric, 2) as "Средняя длительность"
FROM evgeniy_zubkov.mv_track_trends
GROUP BY genre_name, duration_category
HAVING COUNT(*) > 1
ORDER BY genre_name, "Средняя популярность" DESC;

-- Запрос 9: Корреляционный анализ (для Scatter Plot)
SELECT 
    track_length_seconds as "Длительность трека",
    monthly_listens as "Месячные прослушивания",
    genre_name as "Жанр",
    popularity_category as "Категория популярности",
    CASE WHEN is_explicit THEN 'Explicit' ELSE 'Чистый' END as "Тип контента"
FROM evgeniy_zubkov.mv_track_trends
WHERE monthly_listens > 0 AND track_length_seconds > 0
ORDER BY monthly_listens DESC;

-- Запрос 10: Статистика по чартам (для Donut Chart)
SELECT 
    c.chart_name as "Чарт",
    COUNT(t.track_id) as "Количество треков",
    SUM(t.monthly_listens) as "Общие прослушивания",
    AVG(t.monthly_listens) as "Средние прослушивания",
    COUNT(DISTINCT t.artist_id) as "Уникальные исполнители",
    COUNT(DISTINCT t.genre_id) as "Уникальные жанры"
FROM evgeniy_zubkov.charts c
LEFT JOIN evgeniy_zubkov.tracks t ON c.chart_id = t.chart_id
GROUP BY c.chart_id, c.chart_name
ORDER BY "Общие прослушивания" DESC;

-- Дополнительные запросы для расширенной аналитики:

-- Запрос 11: Ранжирование треков внутри жанров
SELECT 
    genre_name as "Жанр",
    track_name as "Название трека",
    artist_name as "Исполнитель",
    monthly_listens as "Прослушивания",
    genre_rank as "Ранг в жанре",
    genre_avg_listens as "Среднее по жанру",
    genre_deviation_percent as "Отклонение от среднего %"
FROM evgeniy_zubkov.mv_track_trends
WHERE genre_rank <= 5
ORDER BY genre_name, genre_rank;

-- Запрос 12: Сравнение популярности жанров
SELECT 
    genre_name as "Жанр",
    total_tracks as "Всего треков",
    total_monthly_listens as "Общие прослушивания",
    avg_monthly_listens as "Среднее на трек",
    max_monthly_listens as "Максимум",
    explicit_tracks_count as "Explicit треков",
    explicit_percentage as "% Explicit",
    avg_track_length_seconds as "Средняя длительность"
FROM evgeniy_zubkov.mv_genre_popularity
ORDER BY total_monthly_listens DESC;