-- ==========================================
-- 0. UPDATE SCHEMAS
-- ==========================================

--DROP SCHEMA IF EXISTS bronze CASCADE;
--DROP SCHEMA IF EXISTS silver CASCADE;
--DROP SCHEMA IF EXISTS gold CASCADE;

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
--DROP TABLE bronze.raw_responses_delta CASCADE;
CREATE TABLE IF NOT EXISTS bronze.raw_responses_delta (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES auth.users(id), 
    source_platform TEXT DEFAULT 'typeform', 
    ingestion_method TEXT DEFAULT 'webhook',
    response_token TEXT,
    form_id TEXT,
    payload JSONB NOT NULL,
    is_processed BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Al activar RLS sin crear políticas, se bloquea todo acceso público (anon/authenticated).
-- Solo el 'service_role' (tu Edge Function) podrá escribir aquí.
ALTER TABLE bronze.raw_responses_delta ENABLE ROW LEVEL SECURITY;


-- Ingesta desde API (Backfill/Snapshot)
--DROP TABLE IF EXISTS bronze.raw_responses_snapshot CASCADE;
CREATE TABLE IF NOT EXISTS bronze.raw_responses_snapshot (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES auth.users(id),
    source_platform TEXT DEFAULT 'typeform',
    ingestion_method TEXT DEFAULT 'api_backfill',
    form_id TEXT,
    response_token TEXT UNIQUE,
    submitted_at TEXT,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()

);


-- CAPA BRONZE: Snapshot de definiciones de preguntas
--DROP TABLE bronze.raw_questions_snapshot;
CREATE TABLE IF NOT EXISTS bronze.raw_questions_snapshot (
    id BIGSERIAL PRIMARY KEY,           -- ID interno de la fila
    question_id TEXT,                   -- ID de Typeform
    form_id TEXT,
    form_title TEXT,
    question_text TEXT,
    question_ref TEXT,
    type TEXT,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Permisos para que Mage pueda escribir
GRANT ALL ON TABLE bronze.raw_questions_snapshot TO service_role;

-- 🔥 SEGURIDAD: Activamos RLS también para snapshots
ALTER TABLE bronze.raw_responses_snapshot ENABLE ROW LEVEL SECURITY;
-- Permisos para que Mage pueda escribir
GRANT ALL ON TABLE bronze.raw_questions_snapshot TO service_role;

-- ==========================================
-- 3. CAPA SILVER: TRANSFORMACIÓN E INTEGRACIÓN
-- ==========================================

-- [VIEW] stg_tf__responses: Unificación de orígenes
--DROP VIEW IF EXISTS silver.stg_tf__responses CASCADE;
CREATE OR REPLACE VIEW silver.stg_tf__responses AS
WITH unnested_webhook AS (
    -- 1. Prioridad 1: Datos del Webhook (Delta)
    SELECT 
        id,
        user_id,
        source_platform,
        ingestion_method,
        (payload->'form_response'->>'submitted_at')::timestamptz AS submitted_at,
        created_at AS ingested_at,
        form_id,
        payload->'form_response'->'hidden' AS hidden_fields,
        response_token,
        elem->'field'->>'id' AS field_id,
        elem->'field'->>'ref' AS field_ref,
        elem->'field'->>'type' AS field_type, -- <--- Columna 11
        COALESCE(
          elem->>'text', elem->>'email', elem->>'phone_number', 
          elem->'choice'->>'label', (elem->>'number')::text, 
          (elem->>'boolean')::text, elem->>'date'
        ) AS response_value, -- <--- Columna 12
        1 AS priority -- <--- Columna 13
    FROM 
        bronze.raw_responses_delta,
        LATERAL jsonb_array_elements(payload->'form_response'->'answers') AS elem
    WHERE 
        jsonb_typeof(payload->'form_response'->'answers') = 'array'
),
unnested_api_backfill AS (
    -- 2. Prioridad 2: Datos de la API (Snapshot)
    SELECT 
        id,
        user_id,
        source_platform,
        ingestion_method,
        (payload->>'submitted_at')::timestamptz AS submitted_at,
        ingested_at,
        form_id,
        payload->'hidden' AS hidden_fields, 
        response_token, -- Usamos la columna directa
        elem->'field'->>'id' AS field_id,
        elem->'field'->>'ref' AS field_ref,
        elem->'field'->>'type' AS field_type, -- <--- AGREGADA para igualar el UNION
        COALESCE(
          elem->>'text', elem->>'email', elem->>'phone_number', 
          elem->'choice'->>'label', (elem->>'number')::text, 
          (elem->>'boolean')::text, elem->>'date'
        ) AS response_value,
        2 AS priority
    FROM 
        bronze.raw_responses_snapshot,
        LATERAL jsonb_array_elements(payload->'answers') AS elem
    WHERE 
        jsonb_typeof(payload->'answers') = 'array'
),
combined_data AS (
    SELECT * FROM unnested_webhook
    UNION ALL 
    SELECT * FROM unnested_api_backfill
)
SELECT DISTINCT ON (response_token, field_id)
    user_id,
    response_token,
    field_id,
    form_id,
    submitted_at,
    ingested_at,
    hidden_fields,
    field_ref,
    field_type,
    response_value,
    source_platform,
    priority
FROM combined_data
ORDER BY response_token, field_id, priority ASC, ingested_at DESC;

-- [MATERIALIZED VIEW] int_tf__core: Procesamiento denso para BI
-- Nota: Las vistas materializadas no soportan RLS directo, se controla en el acceso al esquema
DROP MATERIALIZED VIEW IF EXISTS silver.int_tf__core CASCADE;
CREATE MATERIALIZED VIEW silver.int_tf__core AS
SELECT 
    tf.user_id,
    tf.response_token,
    tf.form_id,
    qs.form_title,
    tf.field_id,
    tf.response_value,
    tf.source_platform,
    qs.question_text,
    tf.hidden_fields,
    tf.submitted_at,
    tf.submitted_at::date AS submitted_date
FROM silver.stg_tf__responses tf
LEFT JOIN bronze.raw_questions_snapshot qs 
ON tf.field_id = qs.question_id
WITH NO DATA;

-- ==========================================
-- 4. CAPA GOLD: MODELO DIMENSIONAL
-- ==========================================
--DROP TABLE IF EXISTS gold.dim_questions CASCADE;

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
---DROP TABLE gold.dim_respondents CASCADE;
CREATE TABLE IF NOT EXISTS gold.dim_respondents (
    response_token TEXT PRIMARY KEY,
    user_id UUID, -- Ojo: En dbt la FK se maneja distinto (ver punto 2)
    submitted_at TIMESTAMP, -- Agregado (antes tenías submitted_date, ajusta según origen)
    form_id TEXT,
    form_title TEXT,
    
    -- Datos demográficos y edad
    birth_date DATE,
    age FLOAT8, -- O NUMERIC
    age_range TEXT,
    age_range_order INTEGER,
    
    -- Geo
    city TEXT,
    state TEXT,
    country TEXT,
    
    -- Info del taller/usuario
    workshop_type TEXT, -- Alias de event_type
    gender TEXT,
    occupation TEXT,
    education_level TEXT,
    company TEXT,
    
    -- Contacto y Flags
    email TEXT,
    email_tenant BOOLEAN,
    phone_number TEXT,
    phone_number_tenant BOOLEAN
);

--DROP TABLE IF EXISTS gold.fact_responses CASCADE;
CREATE TABLE IF NOT EXISTS gold.fact_responses (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id),
    response_token TEXT REFERENCES gold.dim_respondents(response_token),
    question_id TEXT REFERENCES gold.dim_questions(question_id),
    response_value TEXT,
    submitted_at TIMESTAMP,
    submitted_date DATE
);

-- ==========================================
-- 5. SEGURIDAD (RLS) - POLÍTICAS DE USUARIO
-- ==========================================
ALTER TABLE gold.dim_respondents ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold.fact_responses ENABLE ROW LEVEL SECURITY;

-- 1. Política para fact_responses (Idempotente)
DROP POLICY IF EXISTS "Usuarios ven sus respuestas" ON gold.fact_responses;
CREATE POLICY "Usuarios ven sus respuestas" ON gold.fact_responses
FOR SELECT USING (auth.uid() = user_id);

-- 2. Política para dim_respondents (Idempotente)
DROP POLICY IF EXISTS "Usuarios ven su perfil" ON gold.dim_respondents;
CREATE POLICY "Usuarios ven su perfil" ON gold.dim_respondents
FOR SELECT USING (auth.uid() = user_id);

-- ==========================================
-- 6. PERMISOS (GRANTS) PARA AUTOMATIZACIÓN
-- ==========================================

-- Permitir que la Edge Function (service_role) opere en estos esquemas
GRANT USAGE ON SCHEMA bronze, silver, gold TO service_role;
GRANT ALL ON ALL TABLES IN SCHEMA bronze, silver, gold TO service_role;

-- Asegurar que futuras tablas hereden estos permisos automáticamente
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold GRANT ALL ON TABLES TO service_role;
CREATE EXTENSION IF NOT EXISTS pg_trgm;