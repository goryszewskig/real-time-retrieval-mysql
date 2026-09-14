-- CDC source database. Runs once on first container initialisation
-- (mounted into /docker-entrypoint-initdb.d by docker-compose).

CREATE DATABASE IF NOT EXISTS usersdb;

-- Columns mirror data/jobseekers_10000.csv. DATETIME(6) keeps
-- microsecond precision so the write benchmark's replication check
-- can compare updated_at values exactly.
CREATE TABLE IF NOT EXISTS usersdb.users (
    id          BIGINT PRIMARY KEY,
    name        VARCHAR(255),
    email       VARCHAR(255),
    company     VARCHAR(255),
    job_title   VARCHAR(255),
    location    VARCHAR(255),
    skills      TEXT,
    bio         TEXT,
    experience  INT,
    created_at  DATETIME(6),
    updated_at  DATETIME(6)
);

-- Debezium's replication user. mysql_native_password avoids
-- caching_sha2_password RSA/SSL requirements over the plain Docker
-- network. LOCK TABLES is needed in case a snapshot ever runs.
CREATE USER IF NOT EXISTS 'debezium'@'%'
    IDENTIFIED WITH mysql_native_password BY 'debezium';

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
    ON *.* TO 'debezium'@'%';

-- Separate role with UPDATE on users, used only by the write benchmark.
CREATE USER IF NOT EXISTS 'writer'@'%'
    IDENTIFIED WITH mysql_native_password BY 'writerpass';

GRANT SELECT, UPDATE ON usersdb.users TO 'writer'@'%';

FLUSH PRIVILEGES;
