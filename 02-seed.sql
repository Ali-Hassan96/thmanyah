CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-------------------
------------2. Populate engagement_events (10 rows)
--------------------------------
INSERT INTO public.content (
    id,
    slug,
    title,
    content_type,
    length_seconds,
    publish_ts
)
VALUES
(uuid_generate_v4(), 'intro-to-ai', 'Introduction to AI', 'podcast', 1800, now() - interval '30 days'),
(uuid_generate_v4(), 'weekly-tech-01', 'Weekly Tech Newsletter #1', 'newsletter', NULL, now() - interval '28 days'),
(uuid_generate_v4(), 'docker-basics', 'Docker Basics Explained', 'video', 900, now() - interval '25 days'),
(uuid_generate_v4(), 'cloud-security', 'Cloud Security Essentials', 'podcast', 2400, now() - interval '22 days'),
(uuid_generate_v4(), 'data-streaming', 'Data Streaming 101', 'video', 1200, now() - interval '20 days'),
(uuid_generate_v4(), 'ml-news-02', 'Machine Learning News #2', 'newsletter', NULL, now() - interval '18 days'),
(uuid_generate_v4(), 'postgres-performance', 'Postgres Performance Tips', 'podcast', 2100, now() - interval '15 days'),
(uuid_generate_v4(), 'event-driven', 'Event-Driven Architecture', 'video', 1500, now() - interval '12 days'),
(uuid_generate_v4(), 'ai-ethics', 'AI Ethics & Responsibility', 'podcast', 2700, now() - interval '10 days'),
(uuid_generate_v4(), 'weekly-tech-02', 'Weekly Tech Newsletter #2', 'newsletter', NULL, now() - interval '7 days')
ON CONFLICT (slug) DO NOTHING;


--------------------
--. Populate engagement_events (10 rows)
----------------------
INSERT INTO public.engagement_events (
    content_id,
    user_id,
    event_type,
    event_ts,
    duration_ms,
    device,
    raw_payload
)
SELECT
    c.id,
    uuid_generate_v4(),
    e.event_type,
    now() - (random() * interval '7 days'),
    e.duration_ms,
    e.device,
    e.raw_payload::jsonb
FROM (
    VALUES
    ('intro-to-ai',        'play',   300000, 'ios',        '{"source":"home"}'),
    ('intro-to-ai',        'finish', 1800000, 'ios',        '{"completed":true}'),
    ('docker-basics',      'play',   120000, 'web-chrome', '{"autoplay":false}'),
    ('docker-basics',      'pause',   45000, 'web-chrome', '{"reason":"user"}'),
    ('cloud-security',     'play',   600000, 'android',    '{"network":"wifi"}'),
    ('data-streaming',     'finish',1200000, 'web-safari', '{"completed":true}'),
    ('postgres-performance','play',  900000, 'ios',        '{"speed":"1.25x"}'),
    ('event-driven',       'click',     NULL, 'web-edge', '{"cta":"subscribe"}'),
    ('ai-ethics',          'play',   400000, 'android',    '{"language":"en"}'),
    ('weekly-tech-02',     'click',     NULL, 'web-chrome','{"link":"footer"}')
) AS e(slug, event_type, duration_ms, device, raw_payload)
JOIN public.content c ON c.slug = e.slug;