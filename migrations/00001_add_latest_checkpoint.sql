ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS latest_checkpoint_uri TEXT;