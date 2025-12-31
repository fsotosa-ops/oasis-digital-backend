import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

// Función para verificar la firma de Typeform (HMAC-SHA256)
async function verifySignature(receivedSignature: string, payload: string, secret: string): Promise<boolean> {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const signatureBuffer = await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(payload)
  );

  // Convertir a Base64 (formato que envía Typeform)
  const hashArray = Array.from(new Uint8Array(signatureBuffer));
  const hashHex = btoa(String.fromCharCode.apply(null, hashArray));
  const expectedSignature = `sha256=${hashHex}`;

  return receivedSignature === expectedSignature;
}

serve(async (req) => {
  const signature = req.headers.get("Typeform-Signature");
  const secret = Deno.env.get("TYPEFORM_SECRET");

  // 1. Validar que la petición sea legítima (Seguridad)
  const bodyText = await req.text(); // Leemos el cuerpo como texto para validar la firma
  
  if (!signature || !secret || !(await verifySignature(signature, bodyText, secret))) {
    return new Response(JSON.stringify({ error: "Unauthorized: Invalid Signature" }), { status: 401 });
  }

  try {
    const payload = JSON.parse(bodyText);
    
    // Configuración del cliente de Supabase
    const supabaseClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    );

    // Intentamos extraer el user_id (puede ser nulo si no viene en campos ocultos)
    const userId = payload.form_response.hidden?.user_id;

    // 2. Inserción en la base de datos (Capa Bronze)
    // ✅ EL ORDEN ES CRÍTICO: .schema() debe ir antes de .from()
    const { error } = await supabaseClient
      .schema('bronze') // Especificamos el esquema definido en el SQL
      .from('raw_responses_delta') // Tabla para ingesta real-time
      .insert({
        user_id: userId, // Puede ser null
        source_platform: 'typeform',
        ingestion_method: 'webhook', // Nombre exacto de la columna en tu SQL
        response_token: payload.form_response.token,
        payload: payload, // Guardamos el JSON completo
        is_processed: false
      });

    if (error) {
      console.error("Error insertando en Supabase:", error.message);
      throw error;
    }

    return new Response(JSON.stringify({ ok: true, message: "Data ingested into Bronze layer" }), { 
      status: 200,
      headers: { "Content-Type": "application/json" } 
    });

  } catch (error) {
    console.error("Error en el procesamiento del webhook:", error.message);
    return new Response(JSON.stringify({ error: error.message }), { 
      status: 400,
      headers: { "Content-Type": "application/json" }
    });
  }
})