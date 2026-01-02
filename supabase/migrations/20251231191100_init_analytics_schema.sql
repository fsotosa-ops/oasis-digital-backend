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
CREATE OR REPLACE VIEW silver.stg_tf__responses AS
WITH combined_data AS (
    -- 1. Prioridad 1: Datos del Webhook (Delta)
    SELECT 
        user_id, 
        source_platform, 
        response_token, 
        payload, 
        form_id, -- Usamos la columna directa que agregamos
        created_at AS source_timestamp,
        1 AS priority -- El número más bajo tiene prioridad
    FROM bronze.raw_responses_delta

    UNION ALL

    -- 2. Prioridad 2: Datos del Backfill (Snapshot)
    SELECT 
        user_id, 
        source_platform, 
        response_token, 
        payload, 
        form_id,
        ingested_at AS source_timestamp,
        2 AS priority -- Prioridad secundaria
    FROM bronze.raw_responses_snapshot
)
SELECT DISTINCT ON (response_token) -- <--- LA MAGIA: Mantiene solo 1 fila por token
    user_id,
    source_platform AS source,
    response_token,
    COALESCE(form_id, payload->>'form_id') AS form_id, -- Prioriza columna, si no, busca en JSON
    NULL::text AS field_id, 
    NULL::text AS field_ref,
    NULL::text AS response_value,
    payload->'hidden' AS hidden_fields,
    source_timestamp AS submitted_at
FROM combined_data
ORDER BY response_token, priority ASC;

-- [MATERIALIZED VIEW] int_tf__core: Procesamiento denso para BI
-- Nota: Las vistas materializadas no soportan RLS directo, se controla en el acceso al esquema
DROP MATERIALIZED VIEW IF EXISTS silver.int_tf__core;

CREATE MATERIALIZED VIEW silver.int_tf__core AS
SELECT 
    user_id,
    response_token,
    form_id,
    NULL::text AS form_title,
    NULL::text AS field_id,
    NULL::text AS question_text,
    NULL::text AS question_type,
    NULL::text AS response_value,
    source,
    hidden_fields,
    submitted_at,
    submitted_at::date AS submitted_date
FROM silver.stg_tf__responses
WITH NO DATA;

-- ==========================================
-- 4. CAPA GOLD: MODELO DIMENSIONAL
-- ==========================================
DROP TABLE IF EXISTS gold.dim_questions CASCADE;

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

DROP TABLE IF EXISTS gold.fact_responses CASCADE;
CREATE TABLE IF NOT EXISTS gold.fact_responses (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id),
    response_token TEXT REFERENCES gold.dim_respondents(response_token),
    question_id TEXT REFERENCES gold.dim_questions(question_id),
    question_text TEXT, 
    response_value TEXT,
    submitted_date DATE,
    workshop_type TEXT,
    company TEXT
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