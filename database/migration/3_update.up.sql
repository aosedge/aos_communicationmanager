-- Up Migration Script

-- Rename 'cached' column to 'state' for instances, services and layers tables

ALTER TABLE instances RENAME COLUMN 'cached' TO 'state';
ALTER TABLE services RENAME COLUMN 'cached' TO 'state';
ALTER TABLE layers RENAME COLUMN 'cached' TO 'state';
