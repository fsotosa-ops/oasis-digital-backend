-- ==========================================
-- 1. ESQUEMAS
-- ==========================================
CREATE SCHEMA IF NOT EXISTS bronze; 
CREATE SCHEMA IF NOT EXISTS silver; 
CREATE SCHEMA IF NOT EXISTS gold;   

-- ==========================================
-- 2. CAPA BRONZE: CAPTURA GENÉRICA
-- ==========================================

-- Ingesta desde Webhooks (Real-time)
CREATE TABLE IF NOT EXISTS bronze.raw_responses_delta (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES auth.users(id), 
    source_platform TEXT DEFAULT 'typeform', 
    ingestion_method TEXT DEFAULT 'webhook',
    response_token TEXT,
    payload JSONB NOT NULL,
    is_processed BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Ingesta desde API (Backfill/Snapshot)
CREATE TABLE IF NOT EXISTS bronze.raw_responses_snapshot (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES auth.users(id),
    source_platform TEXT DEFAULT 'typeform',
    ingestion_method TEXT DEFAULT 'api_backfill',
    response_token TEXT UNIQUE,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- ==========================================
-- 3. CAPA SILVER: TRANSFORMACIÓN E INTEGRACIÓN
-- ==========================================

-- [VIEW] stg_tf__responses: Unificación de orígenes
CREATE OR REPLACE VIEW silver.stg_tf__responses AS
WITH unified AS (
    SELECT user_id, source_platform, response_token, payload, created_at FROM bronze.raw_responses_delta
    UNION ALL
    SELECT user_id, source_platform, response_token, payload, ingested_at FROM bronze.raw_responses_snapshot
    WHERE response_token NOT IN (SELECT response_token FROM bronze.raw_responses_delta)
)
SELECT 
    user_id,
    source_platform AS source,
    response_token,
    payload->>'form_id' AS form_id,
    NULL::text AS field_id, -- Placeholder para lógica de desanidación
    NULL::text AS field_ref,
    NULL::text AS response_value,
    payload->'hidden' AS hidden_fields,
    created_at AS submitted_at
FROM unified;

-- [MATERIALIZED VIEW] int_tf__core: Procesamiento denso para BI
-- Esta vista guarda físicamente los datos para máxima velocidad
DROP MATERIALIZED VIEW IF EXISTS silver.int_tf__core;

CREATE MATERIALIZED VIEW silver.int_tf__core AS
SELECT 
    user_id,
    response_token,
    form_id,
    NULL::text AS form_title, -- Se llenará con JOIN a dim_questions o metadata
    NULL::text AS field_id,
    NULL::text AS question_text,
    NULL::text AS question_type,
    NULL::text AS response_value,
    source,
    hidden_fields,
    submitted_at,
    submitted_at::date AS submitted_date
FROM silver.stg_tf__responses
WITH NO DATA; -- Se crea la estructura, requiere REFRESH MATERIALIZED VIEW para poblarse

-- ==========================================
-- 4. CAPA GOLD: MODELO DIMENSIONAL
-- ==========================================

CREATE TABLE IF NOT EXISTS gold.dim_questions (
    question_id TEXT PRIMARY KEY,
    question_ref TEXT,
    question_text TEXT,
    question_type TEXT,
    form_id TEXT,
    form_title TEXT,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.dim_respondents (
    response_token TEXT PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id),
    form_id TEXT,
    form_title TEXT,
    submitted_date DATE,
    company TEXT,
    workshop_type TEXT,
    age FLOAT8,
    email TEXT,
    occupation TEXT,
    age_range TEXT,
    age_range_order INTEGER
);

CREATE TABLE IF NOT EXISTS gold.fact_responses (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id),
    response_token TEXT REFERENCES gold.dim_respondents(response_token),
    question_text TEXT, -- Según tu CSV original
    response_value TEXT,
    submitted_date DATE,
    workshop_type TEXT,
    company TEXT
);

-- ==========================================
-- 5. SEGURIDAD (RLS) - CORREGIDO
-- ==========================================
ALTER TABLE gold.dim_respondents ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold.fact_responses ENABLE ROW LEVEL SECURITY;

-- 1. Política para fact_responses
DROP POLICY IF EXISTS "Usuarios ven sus respuestas" ON gold.fact_responses;
CREATE POLICY "Usuarios ven sus respuestas" ON gold.fact_responses
FOR SELECT USING (auth.uid() = user_id);

-- 2. Política para dim_respondents
DROP POLICY IF EXISTS "Usuarios ven su perfil" ON gold.dim_respondents;
CREATE POLICY "Usuarios ven su perfil" ON gold.dim_respondents
FOR SELECT USING (auth.uid() = user_id);

-- ==========================================
-- 6. PERMISOS (GRANTS)
-- ==========================================
-- (Mantén aquí el bloque de GRANTS que agregamos en el paso anterior)
GRANT USAGE ON SCHEMA bronze, silver, gold TO service_role;
GRANT ALL ON ALL TABLES IN SCHEMA bronze, silver, gold TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold GRANT ALL ON TABLES TO service_role;