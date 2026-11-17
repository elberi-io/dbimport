-- Runs once when the compose MariaDB server starts on an empty data
-- directory. tests/integration/test_schema.py creates the schema of the
-- ORM in this second database and compares it with the migrated schema
-- in DBImport.
CREATE DATABASE `DBImport_orm` CHARACTER SET latin1;
GRANT ALL PRIVILEGES ON `DBImport_orm`.* TO 'dbimport'@'%';
