-- Switch to the database
\connect thumbsy_db

-- Make thumbsy_user a superuser and preserve it
ALTER ROLE thumbsy_user WITH 
    LOGIN 
    SUPERUSER 
    INHERIT 
    CREATEDB 
    CREATEROLE 
    REPLICATION 
    BYPASSRLS 
    PASSWORD 'Matt.4483';

-- Set search path for the user
ALTER ROLE thumbsy_user SET search_path TO public;

-- Drop and recreate public schema with proper ownership
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

-- Make thumbsy_user the owner of the public schema
ALTER SCHEMA public OWNER TO thumbsy_user;

-- Grant PUBLIC schema privileges
GRANT ALL ON SCHEMA public TO PUBLIC;
GRANT ALL ON SCHEMA public TO thumbsy_user;
GRANT CREATE ON SCHEMA public TO PUBLIC;
GRANT USAGE ON SCHEMA public TO PUBLIC;

-- Set default privileges for PUBLIC
ALTER DEFAULT PRIVILEGES FOR ROLE thumbsy_user IN SCHEMA public 
    GRANT ALL ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE thumbsy_user IN SCHEMA public 
    GRANT ALL ON SEQUENCES TO PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE thumbsy_user IN SCHEMA public 
    GRANT ALL ON FUNCTIONS TO PUBLIC;

-- Create the products table as thumbsy_user
SET ROLE thumbsy_user;
CREATE TABLE IF NOT EXISTS public.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    description VARCHAR,
    price FLOAT,
    category VARCHAR
);

-- Grant PUBLIC table permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO PUBLIC;

-- Verify final state
SELECT 
    current_user,
    session_user,
    current_database(),
    current_setting('search_path'),
    has_schema_privilege('public', 'CREATE') as schema_create,
    has_schema_privilege('public', 'USAGE') as schema_usage,
    has_table_privilege('public.products', 'INSERT') as table_insert,
    has_table_privilege('public.products', 'SELECT') as table_select;

-- Show current permissions
\dn+
\dt+
\du

\echo 'Schema Permissions:'
\dp public.*

\echo 'Role Memberships:'
SELECT r.rolname, r.rolsuper, r.rolinherit,
       r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
       r.rolreplication, r.rolconnlimit, r.rolvaliduntil,
       ARRAY(SELECT b.rolname
             FROM pg_catalog.pg_auth_members m
             JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
             WHERE m.member = r.oid) as memberof
FROM pg_catalog.pg_roles r
WHERE r.rolname = 'thumbsy_user';

\echo 'Schema ACL:'
SELECT n.nspname as "schema",
       pg_catalog.array_to_string(n.nspacl, E'\n') as "access privileges"
FROM pg_catalog.pg_namespace n
WHERE n.nspname = 'public';

\echo 'Table Permissions:'
SELECT relname, relacl
FROM pg_class
WHERE relnamespace = 'public'::regnamespace
  AND relkind = 'r';

\echo 'Current Settings:'
SHOW search_path;
SELECT current_user, session_user, current_database();

-- Verify permissions explicitly
SELECT 
    n.nspname as schema_name,
    c.relname as table_name,
    pg_get_userbyid(c.relowner) as owner,
    has_table_privilege('thumbsy_user', c.oid, 'SELECT') as has_select,
    has_table_privilege('thumbsy_user', c.oid, 'INSERT') as has_insert,
    has_table_privilege('thumbsy_user', c.oid, 'UPDATE') as has_update,
    has_table_privilege('thumbsy_user', c.oid, 'DELETE') as has_delete
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' 
AND c.relkind = 'r';  -- 'r' means regular table 