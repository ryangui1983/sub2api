-- Keep a single mapping column. New mapping rules are stored in model_mapping.
UPDATE channels
SET model_mapping = model_mapping_rules
WHERE (model_mapping IS NULL OR model_mapping = '{}'::jsonb)
  AND model_mapping_rules IS NOT NULL
  AND model_mapping_rules <> '{}'::jsonb;

ALTER TABLE channels DROP COLUMN IF EXISTS model_mapping_rules;
