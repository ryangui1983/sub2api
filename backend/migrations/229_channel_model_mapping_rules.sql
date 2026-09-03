-- Add model_mapping_rules column to channels table
-- This supports channel-level model mapping without platform grouping
ALTER TABLE channels ADD COLUMN IF NOT EXISTS model_mapping_rules JSONB DEFAULT '{}'::jsonb;

-- Add comment
COMMENT ON COLUMN channels.model_mapping_rules IS 'Channel-level model mapping rules: map[string]ModelMappingRule';
