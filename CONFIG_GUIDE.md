# Guía de Configuración: BigQuery → Supabase Sync

He configurado el sistema para que sea resistente a los problemas de caché de Supabase. Para que el Worker funcione correctamente, se requieren dos funciones auxiliares en tu base de datos.

## 1. Configurar Funciones RPC en Supabase (OBLIGATORIO)

Ve a tu **Supabase Dashboard** -> **SQL Editor** y ejecuta estas dos consultas:

### A. Función para DDL (Crear/Modificar Tablas)
```sql
CREATE OR REPLACE FUNCTION exec_sql(query text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  EXECUTE query;
  -- Notifica a PostgREST para intentar recargar el schema en background
  NOTIFY pgrst, 'reload schema';
END;
$$;
```

### B. Función para Consultas Dinámicas (Bypass de Caché)
```sql
CREATE OR REPLACE FUNCTION exec_sql_query(query text)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE result json;
BEGIN
  EXECUTE 'SELECT json_agg(t) FROM (' || query || ') t' INTO result;
  RETURN COALESCE(result, '[]'::json);
END;
$$;
```

---

## 2. Configurar Secretos en Cloudflare

Ejecuta estos comandos en tu terminal local:

```bash
# JSON de Service Account de Google
npx wrangler secret put GOOGLE_SERVICE_ACCOUNT_JSON

# ID del Proyecto (acf-ecomerce-database)
npx wrangler secret put GOOGLE_PROJECT_ID

# Credenciales de Supabase
npx wrangler secret put SUPABASE_URL
npx wrangler secret put SUPABASE_SERVICE_KEY

# Clave de acceso al Dashboard
npx wrangler secret put SYNC_API_KEY
```

---

## 3. Despliegue y Uso

1. **Desplegar**: `npm run deploy`
2. **Dashboard**: Entra a la URL de tu Worker y usa la `SYNC_API_KEY` para configurar tus sincronizaciones.
3. **Logs**: Si algo falla, mira los logs en tiempo real con `npx wrangler tail`.

## Características del Sistema
- **Sin Dependencias Pesadas**: Eliminamos `postgres.js` para evitar límites de sub-peticiones de Cloudflare.
- **Bypass de Caché**: Las verificaciones de schema y fecha usan SQL directo vía RPC para evitar el error "Table not found in schema cache".
- **Performance**: Carga masiva en batches de 2500 registros.
