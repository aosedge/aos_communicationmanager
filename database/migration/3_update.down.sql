-- Down Migration Script

-- Rename 'state' column to 'cached' for instances, services and layers tables

ALTER TABLE instances RENAME COLUMN 'state' TO 'cached';
ALTER TABLE services RENAME COLUMN 'state' TO 'cached';
ALTER TABLE layers RENAME COLUMN 'state' TO 'cached';
