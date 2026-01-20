CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- content table
DROP TRIGGER IF EXISTS trg_content_updated ON content;
CREATE TRIGGER trg_content_updated
BEFORE UPDATE ON content
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- engagement_events table
DROP TRIGGER IF EXISTS trg_engagement_updated ON engagement_events;
CREATE TRIGGER trg_engagement_updated
BEFORE UPDATE ON engagement_events
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
