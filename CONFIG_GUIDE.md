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

---

## 4. Guía de Configuración: Google Sheets → BigQuery

Esta funcionalidad permite sincronizar datos desde Google Sheets hacia BigQuery. Requiere una configuración única de permisos y luego compartir cada hoja individualmente.

### A. Configuración Inicial (Service Account)

Necesitas tener una **Service Account** de Google Cloud con permisos para leer Sheets y escribir en BigQuery.

1.  **Obtén tu Service Account Email**:
    *   Busca en tu archivo de credenciales JSON (o en la variable de entorno `GOOGLE_SERVICE_ACCOUNT_JSON`) el campo `client_email`.
    *   Se verá algo como: `adcreativeflow-sync@tu-proyecto.iam.gserviceaccount.com`.

2.  **Verifica los Permisos en Google Cloud**:
    *   Asegúrate de que esta cuenta de servicio tenga habilitada la API de Google Sheets (`Google Sheets API`).
    *   Debe tener roles de BigQuery (ej: `BigQuery Admin` o `BigQuery Data Editor` + `Job User`).

### B. Conectar una Hoja de Cálculo (Por cada Sheet)

Para que el sistema pueda leer una hoja de cálculo privada, debes darle acceso explícito a tu Service Account. Es igual que compartir el archivo con un compañero de trabajo:

1.  Abre tu **Google Sheet** en el navegador.
2.  Haz clic en el botón **Compartir** (Share) arriba a la derecha.
3.  En el campo de "Agregar personas y grupos", pega el **email de tu Service Account**.
4.  Asignale el rol de **Lector** (Viewer).
5.  Haz clic en **Enviar** (o Guardar).
    *   *Tip: Desmarca "Notificar a los usuarios" si no quieres que envíe un email.*

### C. Configurar en el Dashboard

Una vez compartido el Sheet:

1.  Ve a tu dashboard (`/`).
2.  Haz clic en el botón **"Add Sheets Sync Job"** (azul).
3.  Copia la **URL completa** de tu Google Sheet y pégala en el campo "Spreadsheet URL".
4.  El sistema extraerá automáticamente el ID de la hoja.
5.  Selecciona el nombre de la tabla destino (debe estar en la whitelist, ej: `shm_au_dimension_meta`).
6.  Haz clic en **"Test Connection"**.
    *   Si compartiste correctamente la hoja, verás un mensaje verde: `✅ Connection Successful: Sheet is accessible`.
7.  Guarda el job y ejecútalo manualmente ("RUN") para probar.

