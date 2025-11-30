DO
$do$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
      CREATE USER airflow WITH PASSWORD 'airflow';
   END IF;
END
$do$;

DO
$do$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db') THEN
      CREATE DATABASE airflow_db OWNER airflow;
   END IF;
END
$do$;
