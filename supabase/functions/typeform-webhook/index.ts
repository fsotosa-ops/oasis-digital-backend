import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

// Función para verificar la firma de Typeform
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

  // Convertir a Base64 (formato de Typeform)
  const hashArray = Array.from(new Uint8Array(signatureBuffer));
  const hashHex = btoa(String.fromCharCode.apply(null, hashArray));
  const expectedSignature = `sha256=${hashHex}`;

  return receivedSignature === expectedSignature;
}

serve(async (req) => {
  const signature = req.headers.get("Typeform-Signature");
  const secret = Deno.env.get("TYPEFORM_SECRET");

  // 1. Validar que la petición sea legítima
  const bodyText = await req.text(); // Leemos como texto para verificar firma
  
  if (!signature || !secret || !(await verifySignature(signature, bodyText, secret))) {
    return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401 });
  }

  try {
    const payload = JSON.parse(bodyText);
    const supabaseClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    );

    const userId = payload.form_response.hidden?.user_id;

    const { error } = await supabaseClient
      .from('raw_responses_delta')
      .insert({
        user_id: userId,
        source_platform: 'typeform',
        external_event_id: payload.event_id,
        response_token: payload.form_response.token,
        payload: payload
      })
      .schema('bronze');

    if (error) throw error;

    return new Response(JSON.stringify({ ok: true }), { status: 200 });

  } catch (error) {
    return new Response(JSON.stringify({ error: error.message }), { status: 400 });
  }
})