-- CDC destination database. The consumer upserts Debezium events here;
-- search/query.py reads through the FULLTEXT index.

CREATE DATABASE IF NOT EXISTS searchdb;

CREATE TABLE IF NOT EXISTS searchdb.users (
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
    updated_at  DATETIME(6),
    -- Replaces the OpenSearch BM25 index from the original pipeline.
    FULLTEXT INDEX ft_users (job_title, skills, bio, company, location)
);

-- Consumer / query user.
CREATE USER IF NOT EXISTS 'consumer'@'%'
    IDENTIFIED WITH mysql_native_password BY 'consumerpass';

GRANT SELECT, INSERT, UPDATE, DELETE ON searchdb.* TO 'consumer'@'%';

FLUSH PRIVILEGES;
