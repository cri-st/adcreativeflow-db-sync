# Guía de Configuración: BigQuery → Supabase Sync

He configurado la estructura completa del Worker y la tabla en Supabase. Para que el sistema empiece a funcionar, debes seguir estos pasos:

## 1. Configurar Secretos en Cloudflare

Debes ejecutar los siguientes comandos en tu terminal (en la carpeta del proyecto) para subir las credenciales de forma segura:

```bash
# JSON completo de tu Service Account de Google Cloud
# Asegúrate de que el JSON esté en una sola línea o pégalo cuando te lo pida
npx wrangler secret put GOOGLE_SERVICE_ACCOUNT_JSON

# ID del proyecto de Google Cloud (acf-ecomerce-database)
npx wrangler secret put GOOGLE_PROJECT_ID

# URL de tu proyecto Supabase (ej: https://xyz.supabase.co)
npx wrangler secret put SUPABASE_URL

# Service Role Key de Supabase (OJO: no la anon key)
npx wrangler secret put SUPABASE_SERVICE_KEY

# URL de conexión directa a la DB (Transaction Pooler recomendado)
# Formato: postgres://postgres.[USER]:[PASS]@[HOST]:6543/postgres
npx wrangler secret put SUPABASE_POSTGRES_URL

# Crea una clave aleatoria segura para tu endpoint (ej: un UUID o frase larga)
npx wrangler secret put SYNC_API_KEY
```

## 2. Permisos en Google Cloud

Asegúrate de que la Service Account que uses tenga los siguientes roles en el proyecto `acf-ecomerce-database`:
- `BigQuery Data Viewer`
- `BigQuery Job User`

## 3. Despliegue

Una vez configurados los secretos, puedes desplegar el worker:

```bash
npm run deploy
```

## 4. Disparo Manual (n8n / Webhook)

El endpoint solo acepta peticiones **POST** con un header de autorización para mayor seguridad.

### Configuración en n8n (HTTP Request Node):
- **Method**: `POST`
- **URL**: `https://tu-worker.workers.dev/`
- **Authentication**: `Header Auth`
- **Name**: `Authorization`
- **Value**: `Bearer TU_SYNC_API_KEY` (Sustituye por la clave que configuraste en el paso 1)

## Detalles Técnicos Implementados

- **Seguridad Robusta**: El endpoint manual ahora requiere una clave API secreta enviada mediante el estándar Bearer Token. Solo permite métodos POST.
- **Deduplicación**: La tabla en Supabase tiene una constraint UNIQUE en `(date_monday, campaign_id)`.
- **Lotes**: Los datos se suben en lotes de 500 registros para optimizar el performance.
- **Seguridad**: Autenticación vía JWT (RS256) para Google Cloud integrada directamente en el worker.

---
*Cualquier error de sincronización aparecerá en los logs de Cloudflare Workers:* `npx wrangler tail`
