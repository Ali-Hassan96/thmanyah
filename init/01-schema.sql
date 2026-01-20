

-- Content catalogue ----------------------------------------------

DROP TABLE IF EXISTS public.content;
CREATE TABLE public.content (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    content_type TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds INTEGER,
    publish_ts TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Raw engagement telemetry ---------------------------------------
DROP TABLE IF EXISTS public.engagement_events;
CREATE TABLE engagement_events (
    id BIGSERIAL PRIMARY KEY,
    content_id UUID REFERENCES content(id) ON DELETE CASCADE,
    user_id UUID,
    event_type TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts TIMESTAMPTZ NOT NULL,
    duration_ms INTEGER,
    device TEXT,
    raw_payload JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
