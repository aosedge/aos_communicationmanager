-- Up Migration Script for network table

-- Create temporary table with new structure
CREATE TABLE network_new (
    networkID TEXT NOT NULL,
    ip TEXT,
    subnet TEXT,
    vlanID INTEGER,
    nodeID TEXT,
    PRIMARY KEY(networkID, nodeID)
);

-- Copy data from old table to new table
-- For existing records, set nodeID to empty string(invalid network)
INSERT INTO network_new (networkID, ip, subnet, vlanID, nodeID)
SELECT networkID, ip, subnet, vlanID, '' FROM network;

-- Drop old table
DROP TABLE network;

-- Rename new table to original name
ALTER TABLE network_new RENAME TO network; 
