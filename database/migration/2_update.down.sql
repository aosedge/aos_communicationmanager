-- Create a new table without the 'timestamp' and 'cached' columns
CREATE TABLE instances_new (
    serviceId TEXT,
    subjectId TEXT,
    instance INTEGER,
    uid INTEGER,
    PRIMARY KEY(serviceId, subjectId, instance)
);

-- Copy data from the old table to the new table
INSERT INTO instances_new (serviceId, subjectId, instance, uid)
SELECT serviceId, subjectId, instance, uid
FROM instances;

-- Drop the old table
DROP TABLE instances;

-- Rename the new table to the original table name
ALTER TABLE instances_new RENAME TO instances;
