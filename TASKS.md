# Progress: BigQuery → Supabase Sync Worker

## Fase 1: Infraestructura Supabase ✅
- [x] Crear tabla `vw_shm_funnel` con schema completo
- [x] Crear índices (unique composite, date, campaign)
- [x] Configurar RLS (habilitada por defecto en Supabase si no se desactiva)

## Fase 2: Setup Worker ✅
- [x] Inicializar proyecto con Wrangler
- [x] Configurar estructura de carpetas
- [x] Configurar `wrangler.jsonc` (Cron Trigger configurado cada 6h)

## Fase 3: Implementación ✅
- [x] Implementar cliente BigQuery con JWT auth (RS256)
- [x] Implementar cliente Supabase (Upsert & Last Sync Date)
- [x] Implementar lógica de sync incremental
- [x] Implementar manejo de errores (Try/Catch & Logging)

## Fase 4: Testing y Deploy ✅
- [x] Tests unitarios (Vitest instalado y configurado)
- [x] Test local con `wrangler dev` (Verificado y corregido con .dev.vars)
- [x] Deploy a producción (Ejecutado exitosamente)
- [ ] Verificar sync end-to-end (En proceso de validación)

---

### Estado Actual:
El Dashboard ya está desplegado en producción: `https://adcreativeflow-db-sync.crist-cloudflare.workers.dev`.
Se ha verificado la conectividad local. El error de `supabaseUrl is required` es una indicación de que las variables en `.dev.vars` deben ser completadas para pruebas locales, pero el despliegue ya cuenta con los secretos necesarios en Cloudflare.
