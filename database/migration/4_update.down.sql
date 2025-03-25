-- Down Migration Script for network table

-- Create temporary table with old structure
CREATE TABLE network_old (
    networkID TEXT NOT NULL PRIMARY KEY,
    ip TEXT,
    subnet TEXT,
    vlanID INTEGER
);

-- Copy data from new table to old table
INSERT INTO network_old (networkID, ip, subnet, vlanID)
SELECT DISTINCT networkID, ip, subnet, vlanID FROM network;

-- Drop new table
DROP TABLE network;

-- Rename old table to original name
ALTER TABLE network_old RENAME TO network;
