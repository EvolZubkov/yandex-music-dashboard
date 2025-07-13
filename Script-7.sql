SELECT * FROM evgeniy_zubkov.yandex_music_raw LIMIT 10;


-- Схема evgeniy_zubkov
-- Нормализация данных Yandex Music

-- 1. Справочник жанров
CREATE TABLE evgeniy_zubkov.genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Справочник исполнителей
CREATE TABLE evgeniy_zubkov.artists (
    artist_id SERIAL PRIMARY KEY,
    artist_name VARCHAR(200) NOT NULL,
    total_likes BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Справочник чартов
CREATE TABLE evgeniy_zubkov.charts (
    chart_id SERIAL PRIMARY KEY,
    chart_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Основная таблица треков (оперативные данные)
CREATE TABLE evgeniy_zubkov.tracks (
    track_id SERIAL PRIMARY KEY,
    track_name VARCHAR(300) NOT NULL,
    artist_id INTEGER REFERENCES evgeniy_zubkov.artists(artist_id),
    genre_id INTEGER REFERENCES evgeniy_zubkov.genres(genre_id),
    chart_id INTEGER REFERENCES evgeniy_zubkov.charts(chart_id),
    track_length_seconds INTEGER,
    track_url VARCHAR(500),
    is_explicit BOOLEAN DEFAULT FALSE,
    monthly_listens BIGINT DEFAULT 0,
    chart_position INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Таблица аудита для справочника жанров
CREATE TABLE evgeniy_zubkov.genres_audit (
    audit_id SERIAL PRIMARY KEY,
    genre_id INTEGER,
    operation_type VARCHAR(10) NOT NULL, -- INSERT, UPDATE, DELETE
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100) DEFAULT current_user,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации
CREATE INDEX idx_tracks_artist_id ON evgeniy_zubkov.tracks(artist_id);
CREATE INDEX idx_tracks_genre_id ON evgeniy_zubkov.tracks(genre_id);
CREATE INDEX idx_tracks_chart_id ON evgeniy_zubkov.tracks(chart_id);
CREATE INDEX idx_tracks_monthly_listens ON evgeniy_zubkov.tracks(monthly_listens DESC);
CREATE INDEX idx_tracks_name ON evgeniy_zubkov.tracks USING gin(to_tsvector('russian', track_name));

-- Составной индекс для аналитики
CREATE INDEX idx_tracks_genre_chart ON evgeniy_zubkov.tracks(genre_id, chart_id);

-- Триггерная функция для аудита изменений в справочнике жанров
CREATE OR REPLACE FUNCTION evgeniy_zubkov.audit_genres()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO evgeniy_zubkov.genres_audit (genre_id, operation_type, new_values)
        VALUES (NEW.genre_id, 'INSERT', to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO evgeniy_zubkov.genres_audit (genre_id, operation_type, old_values, new_values)
        VALUES (NEW.genre_id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO evgeniy_zubkov.genres_audit (genre_id, operation_type, old_values)
        VALUES (OLD.genre_id, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Создание триггера
CREATE TRIGGER trigger_genres_audit
    AFTER INSERT OR UPDATE OR DELETE ON evgeniy_zubkov.genres
    FOR EACH ROW EXECUTE FUNCTION evgeniy_zubkov.audit_genres();

-- Функция для просмотра аудита
CREATE OR REPLACE FUNCTION evgeniy_zubkov.fnc_audit(table_name TEXT)
RETURNS TABLE (
    audit_date TIMESTAMP,
    operation_type VARCHAR(10),
    changed_data TEXT
) AS $$
BEGIN
    IF table_name = 'genres' THEN
        RETURN QUERY
        SELECT 
            ga.changed_at as audit_date,
            ga.operation_type,
            CASE 
                WHEN ga.operation_type = 'INSERT' THEN 'Добавлен жанр: ' || (ga.new_values->>'genre_name')
                WHEN ga.operation_type = 'UPDATE' THEN 'Изменен жанр: ' || (ga.old_values->>'genre_name') || ' -> ' || (ga.new_values->>'genre_name')
                WHEN ga.operation_type = 'DELETE' THEN 'Удален жанр: ' || (ga.old_values->>'genre_name')
            END as changed_data
        FROM evgeniy_zubkov.genres_audit ga
        ORDER BY ga.changed_at DESC;
    ELSE
        RAISE EXCEPTION 'Аудит для таблицы % не настроен', table_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Заполнение справочных данных из исходной таблицы
-- (предполагается, что исходная таблица называется yandex_music_raw)

-- Заполнение жанров
INSERT INTO evgeniy_zubkov.genres (genre_name)
SELECT DISTINCT genre 
FROM evgeniy_zubkov.yandex_music_raw 
WHERE genre IS NOT NULL
ON CONFLICT (genre_name) DO NOTHING;

TRUNCATE evgeniy_zubkov.tracks CASCADE; 
select * from evgeniy_zubkov.tracks;
-- Проверка типов данных в исходной таблице
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'yandex_music_raw' 
AND table_schema = 'evgeniy_zubkov';

-- Проверка типов данных в целевой таблице
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'tracks' 
AND table_schema = 'evgeniy_zubkov';

-- Создание промежуточной таблицы для связи треков и исполнителей
CREATE TABLE evgeniy_zubkov.track_artists (
    track_id INTEGER REFERENCES evgeniy_zubkov.tracks(track_id),
    artist_id INTEGER REFERENCES evgeniy_zubkov.artists(artist_id),
    PRIMARY KEY (track_id, artist_id)
);


-- Заполнение исполнителей
INSERT INTO evgeniy_zubkov.artists (artist_name, total_likes)
SELECT DISTINCT 
    trim(both ' "''' from unnest(string_to_array(trim(both '[]' from "artist(s)"), ','))) as artist_name,
    COALESCE(artists_likes_total, 0)
FROM evgeniy_zubkov.yandex_music_raw
WHERE "artist(s)" IS NOT NULL
ON CONFLICT DO NOTHING;

ALTER TABLE evgeniy_zubkov.tracks DROP COLUMN IF EXISTS artist_id;

INSERT INTO evgeniy_zubkov.tracks (
    track_name, genre_id, chart_id,
    track_length_seconds, track_url, is_explicit,
    monthly_listens, chart_position
)
SELECT
    r.name,
    g.genre_id,
    c.chart_id,
    -- Альтернативный способ через interval
    EXTRACT(EPOCH FROM ('00:' || r.track_len)::interval)::integer,
    r.link,
    CASE WHEN r."Explicit_content" = 1 THEN TRUE ELSE FALSE END,
    r.monthly_listens_total,
    ROW_NUMBER() OVER (PARTITION BY r.chart ORDER BY r.monthly_listens_total DESC)::integer
FROM evgeniy_zubkov.yandex_music_raw r
JOIN evgeniy_zubkov.genres g ON g.genre_name = r.genre
JOIN evgeniy_zubkov.charts c ON c.chart_name = r.chart::text
;

WITH artist_names AS (
    SELECT 
        r.name as track_name,
        trim(both ' "''' from unnest(string_to_array(trim(both '[]' from r."artist(s)"), ','))) as artist_name
    FROM evgeniy_zubkov.yandex_music_raw r
)
INSERT INTO evgeniy_zubkov.track_artists (track_id, artist_id)
SELECT DISTINCT 
    t.track_id,
    a.artist_id
FROM artist_names an
JOIN evgeniy_zubkov.tracks t ON t.track_name = an.track_name
JOIN evgeniy_zubkov.artists a ON a.artist_name = an.artist_name
ON CONFLICT DO NOTHING;

-- Заполнение чартов
INSERT INTO evgeniy_zubkov.charts (chart_name)
SELECT DISTINCT chart
FROM evgeniy_zubkov.yandex_music_raw 
WHERE chart IS NOT NULL
ON CONFLICT (chart_name) DO NOTHING;

