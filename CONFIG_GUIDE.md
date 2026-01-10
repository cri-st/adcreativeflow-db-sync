# Guía de Configuración: BigQuery → Supabase Sync

He configurado la estructura completa del Worker y el Dashboard. Para que el sistema funcione correctamente con soporte para "Dynamic Schema", sigue estos pasos:

## 1. Configurar Función RPC en Supabase (CRÍTICO)

Para que el Worker pueda crear y actualizar tablas automáticamente sin agotar los límites de Cloudflare, debes crear una función "Helper" en tu base de datos de Supabase.

1. Ve a tu **Supabase Dashboard** -> **SQL Editor**.
2. Pega y ejecuta el siguiente código:

```sql
-- Función para ejecutar SQL dinámico desde el Worker
CREATE OR REPLACE FUNCTION exec_sql(query text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  EXECUTE query;
END;
$$;
```

## 2. Configurar Secretos en Cloudflare

Ejecuta los siguientes comandos en tu terminal para subir las credenciales:

```bash
# JSON completo de tu Service Account de Google Cloud
npx wrangler secret put GOOGLE_SERVICE_ACCOUNT_JSON

# ID del proyecto de Google Cloud (acf-ecomerce-database)
npx wrangler secret put GOOGLE_PROJECT_ID

# URL de tu proyecto Supabase (ej: https://xyz.supabase.co)
npx wrangler secret put SUPABASE_URL

# Service Role Key de Supabase (OJO: no la anon key)
npx wrangler secret put SUPABASE_SERVICE_KEY

# Clave Maestra del Dashboard (Cualquier frase larga o UUID)
npx wrangler secret put SYNC_API_KEY

# (Opcional) URL de conexión directa a la DB (Para uso futuro o depuración)
npx wrangler secret put SUPABASE_POSTGRES_URL
```

## 3. Despliegue

```bash
npm run deploy
```

## 4. Gestión vía Dashboard

Accede a la URL de tu worker (ej: `https://adcreativeflow-db-sync.crist.workers.dev`) e ingresa tu `SYNC_API_KEY` para gestionar los jobs.

---

## Detalles Técnicos Optimizados

- **Performance**: Los datos se suben en lotes grandes (2500 registros) para minimizar las sub-peticiones de Cloudflare.
- **Dynamic Schema**: El Worker detecta si la tabla existe en Supabase y la crea automáticamente con el schema de BigQuery vía la función `exec_sql`.
- **Deduplicación**: Se crea un índice UNIQUE automático basado en las columnas seleccionadas en el Dashboard.
- **Logs en Vivo**: `npx wrangler tail` para ver el progreso de la sincronización.
