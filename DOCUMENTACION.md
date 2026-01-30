# Documentación Técnica: AdCreativeFlow DB Sync

## 📋 Descripción General

**AdCreativeFlow DB Sync** es un servicio serverless implementado como un **Cloudflare Worker** diseñado para sincronizar datos masivos desde **Google BigQuery** (origen) hacia **Supabase/PostgreSQL** (destino).

El sistema funciona de manera autónoma mediante cron triggers (programados cada 6 horas) o puede ser invocado manualmente vía API. Su objetivo principal es mantener una réplica actualizada de los datos analíticos para su uso en aplicaciones frontend, manejando tanto inserciones/actualizaciones como la eliminación de registros obsoletos.

---

## 🏗 Arquitectura

El proyecto está construido sobre las siguientes tecnologías:

- **Runtime**: Cloudflare Workers (Edge Computing).
- **Framework**: Hono (Servidor web ligero y tipado).
- **Almacenamiento de Estado**: Cloudflare KV (Key-Value Store) para configuraciones y cursores de paginación.
- **Origen de Datos**: Google BigQuery (API REST con autenticación JWT vía Service Account).
- **Destino de Datos**: Supabase (Cliente JS oficial).
- **Dashboard**: Interfaz web estática (HTML/JS) servida desde el mismo Worker para gestionar configuraciones.

```mermaid
graph TD
    subgraph Cloudflare Worker
        API[Hono API]
        Cron[Cron Trigger]
        SyncEngine[Sync Engine]
        Dashboard[UI Dashboard]
    end

    subgraph "External Services"
        BQ[(Google BigQuery)]
        Supabase[(Supabase DB)]
        KV[(Cloudflare KV)]
    end

    Cron -->|Every 6h| SyncEngine
    API -->|Manual Trigger| SyncEngine
    API -->|Manage Config| KV
    
    SyncEngine -->|1. Fetch State| KV
    SyncEngine -->|2. Fetch Data| BQ
    SyncEngine -->|3. Upsert Data| Supabase
    SyncEngine -->|4. Delete Old Rows| Supabase
    SyncEngine -->|5. Save State| KV
```

---

## ⚙️ Flujo de Sincronización

El proceso de sincronización (`handleSync`) es el núcleo del sistema y sigue un flujo robusto diseñado para manejar grandes volúmenes de datos y evitar timeouts.

### 1. Inicialización y Validación de Esquema
Al iniciar un trabajo (Job):
1. **Fetch Metadata**: Obtiene el esquema de la tabla origen en BigQuery.
2. **Sync Schema**:
   - Crea la tabla en Supabase si no existe.
   - Detecta diferencias de columnas (nuevas o eliminadas).
   - Aplica cambios DDL (`ALTER TABLE`) automáticamente para mantener ambos esquemas idénticos.
3. **Validación**: Verifica que las `upsertColumns` (claves únicas) existan y sean válidas.

### 2. Sincronización Incremental (Insert/Update)
El sistema utiliza una estrategia de paginación por cursores para procesar datos eficientemente:

- **Batching**: Procesa registros en lotes de 5,000 filas desde BigQuery.
- **Sub-batching**: Inserta en Supabase en sub-lotes de 2,500 filas para respetar límites de tamaño de payload.
- **Upsert**: Utiliza la operación `UPSERT` (Insertar o Actualizar) basada en las columnas clave definidas (`upsertColumns`).
- **Persistencia de Estado**: Guarda el progreso en KV. Si el Worker se detiene por límites de tiempo (15 min), la próxima ejecución retoma exactamente donde quedó usando el cursor compuesto (`incrementalColumn` + `tieBreaker`).

### 3. Detección y Eliminación de Borrados (Delete Phase) 🆕
Esta fase se ejecuta **solo en el último lote** de la sincronización, cuando ya no hay más datos nuevos que traer de BigQuery. Su objetivo es mantener la consistencia eliminando registros que ya no existen en el origen.

#### Estrategia "Hybrid Approach"
Diseñada para soportar tablas de hasta 1 millón de filas sin exceder los límites de memoria (128MB) o CPU del Worker.

1. **Fase de Obtención (Fetch)**:
   - Descarga **todos** los IDs (`upsertColumns`) de BigQuery (consulta ligera, ignora filtros incrementales).
   - Descarga **todos** los IDs de Supabase mediante paginación (bloques de 10,000 registros).

2. **Fase de Comparación (Compare)**:
   - Carga los IDs en memoria usando estructuras `Set` de JavaScript para una comparación O(n) rápida.
   - Identifica los IDs que existen en Supabase pero **NO** en BigQuery.
   - Serializa claves compuestas usando JSON para garantizar precisión (ej: `["id1", "2024-01-01"]`).

3. **Fase de Eliminación (Delete)**:
   - Ejecuta eliminaciones físicas (`DELETE`) en Supabase.
   - Procesa en lotes de **200 registros** para evitar límites de longitud de URL en la API de Supabase.

#### Mecanismos de Seguridad (Circuit Breakers)
Para evitar desastres (como borrar toda una tabla por error), el sistema incluye protecciones estrictas:

- ⛔ **Abortar si BigQuery = 0**: Si BigQuery retorna 0 filas, se asume un error de conexión o configuración y se aborta el proceso de borrado.
- ⛔ **Límite del 50%**: Si el sistema detecta que debe eliminar más del 50% de la tabla de destino, aborta la operación y lanza un error, asumiendo que es una anomalía que requiere revisión manual.
- ⏭️ **Skip en Primer Sync**: Si la tabla de Supabase está vacía, se salta esta fase para optimizar recursos.

```mermaid
sequenceDiagram
    participant BQ as BigQuery
    participant W as Worker (Sync)
    participant SB as Supabase
    participant KV as KV State

    Note over W: Start Batch
    W->>KV: Get Last Cursor
    W->>BQ: Fetch Data (Limit 5000)
    activate BQ
    BQ-->>W: Return Rows
    deactivate BQ
    
    W->>SB: UPSERT Batch (2500 rows)
    W->>SB: UPSERT Batch (2500 rows)

    alt More Data Available (HasMore = true)
        W->>KV: Save Cursor State
        Note over W: End Batch (Continue later)
    else No More Data (HasMore = false)
        Note over W: Final Batch - Start Delete Phase
        W->>BQ: Fetch ALL IDs (upsertColumns)
        W->>SB: Fetch ALL IDs (upsertColumns)
        Note over W: Compare Sets (Memory)
        
        loop Batched Deletes
            W->>SB: DELETE Removed Rows (Batch 200)
        end
        
        W->>KV: Clear Cursor State
        Note over W: Job Complete
    end
```

---

## 🛡️ Seguridad y Autenticación

- **API Security**: Todos los endpoints del Worker están protegidos por un `Bearer Token` (`SYNC_API_KEY`).
- **BigQuery Auth**: Utiliza una Service Account de Google. Genera y firma JWTs (JSON Web Tokens) internamente usando la librería `jose` para autenticarse con la API de Google Cloud.
- **Supabase Auth**: Utiliza la URL y Service Role Key de Supabase para tener permisos de administración (DDL y manipulación de datos).

---

## 📊 Configuración de Jobs

Los trabajos se configuran mediante objetos JSON almacenados en KV (`SYNC_CONFIGS`).

**Estructura del JSON:**

```json
{
  "id": "job_marketing_data",
  "name": "Marketing Data Sync",
  "enabled": true,
  "bigquery": {
    "projectId": "mi-proyecto-gcp",
    "datasetId": "analytics",
    "tableOrView": "marketing_kpis",
    "incrementalColumn": "updated_at", // Opcional: para sync incremental
    "forceStringFields": ["ad_id"]     // Opcional: para preservar precisión de IDs largos
  },
  "supabase": {
    "tableName": "marketing_kpis",
    "upsertColumns": ["ad_id", "date"] // Clave única compuesta
  }
}
```

---

## 🚦 Monitoreo y Logs

El sistema genera logs estructurados que se almacenan en KV (`SYNC_LOGS`) y son visibles desde el dashboard.

- **Niveles**: INFO, SUCCESS, WARNING, ERROR, DEBUG.
- **Resumen Final**: Al terminar, genera un resumen legible:
  > *"15,000 rows synced, 320 deleted in 2m 45s"*

---

## 🚀 Despliegue

```bash
# Instalar dependencias
npm install

# Desarrollo local
npm run dev

# Desplegar a producción
npm run deploy
```

---

## 📑 Sincronización Google Sheets → BigQuery

Esta funcionalidad permite ingerir datos directamente desde hojas de cálculo de Google Sheets hacia tablas de BigQuery. Es ideal para datos manuales, metas, o configuraciones que gestionan equipos no técnicos.

### ⚙️ Flujo de Trabajo

A diferencia de la sincronización BQ->Supabase, este flujo es **Unidireccional** hacia el Data Warehouse.

1.  **Lectura de Headers**:
    -   Lee la primera fila de la hoja especificada.
    -   Sanitiza los nombres de columnas (elimina espacios, caracteres especiales) para que sean compatibles con BigQuery (ej: "Monto Total ($)" -> "Monto_Total___").

2.  **Extracción por Lotes (Batching)**:
    -   Lee la hoja en bloques de 5,000 filas para evitar timeouts de la API de Google Sheets.
    -   Gestiona la paginación interna y reintentos (backoff exponencial) en caso de errores 429 (Rate Limit).

3.  **Carga a BigQuery (Load Job)**:
    -   Transforma los datos a formato **NDJSON** (Newline Delimited JSON) en memoria.
    -   Utiliza la API de Jobs de BigQuery (`uploadType=multipart`) para una carga eficiente.
    -   **Detección de Tabla Existente**: Antes del primer lote, verifica si la tabla destino ya existe en BigQuery.
    -   **Modo Append vs Truncate**:
        -   Si `append: true`: Siempre usa `WRITE_APPEND` (preserva datos existentes y agrega nuevos).
        -   Si `append: false` (default): El primer lote usa `WRITE_TRUNCATE` (limpia la tabla), los siguientes usan `WRITE_APPEND`.
    -   **Evolución de Schema (Schema Evolution)**:
        -   Si la tabla **no existe**: Se crea con el schema del Sheet (todas las columnas como STRING).
        -   Si la tabla **ya existe**: No se proporciona schema, permitiendo que BigQuery haga evolución automática:
            -   **Nuevas columnas en Sheet**: Se agregan automáticamente a la tabla.
            -   **Columnas eliminadas en Sheet**: Se mantienen en la tabla con sus datos históricos; los nuevos inserts tendrán NULL en esas columnas.
            -   **Columnas renombradas**: Se tratan como columnas nuevas.

### 🛡️ Configuración y Permisos

Para que el worker pueda leer una hoja de cálculo privada ("Restricted"), se utiliza el mecanismo estándar de Google Drive:

1.  **Service Account**: Se utiliza la misma cuenta de servicio configurada en `GOOGLE_SERVICE_ACCOUNT_JSON`.
2.  **Acceso Explícito**: El usuario debe compartir el archivo ("Share") con el email de la Service Account (`client_email`), otorgándole rol de "Viewer" (Lector).
3.  **Validación**: El dashboard incluye un botón "Test Connection" que verifica la accesibilidad de la hoja antes de guardar el job.

### 📄 Configuración del Job (JSON)

Los jobs de este tipo se distinguen por el campo `type: "sheets-to-bq"`.

```json
{
  "id": "job_sales_targets",
  "name": "Objetivos de Ventas 2024",
  "enabled": true,
  "type": "sheets-to-bq",
  "sheets": {
    "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/1ABC...",
    "spreadsheetId": "1ABC...",       // ID extraído de la URL
    "sheetName": "Sheet1",            // Nombre exacto de la pestaña
    "projectId": "mi-proyecto-gcp",
    "datasetId": "raw_data",
    "append": false                   // false = Reemplazar tabla (default)
                                       // true = Agregar a datos existentes (preserva historial)
  },
  "bigquery": {
    "projectId": "mi-proyecto-gcp",
    "datasetId": "raw_data",
    "tableId": "sales_targets_2024"
  }
}
```

#### Opciones de Configuración Importantes

- **`append`** (boolean, default: `false`):
  - **`false`**: Cada sync reemplaza completamente los datos de la tabla (útil para datos que cambian completamente).
  - **`true`**: Los datos nuevos se agregan a los existentes. Ideal para:
    - Acumular datos históricos (ej: logs, métricas diarias).
    - Preservar columnas eliminadas del Sheet (la data histórica se mantiene).
    - Schema evolution: nuevas columnas en el Sheet se agregan automáticamente.
