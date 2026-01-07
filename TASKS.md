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

## Fase 4: Testing y Deploy 🔄
- [x] Tests unitarios (Vitest instalado y configurado)
- [ ] Test local con `wrangler dev` (Requiere configurar secretos en el entorno)
- [ ] Deploy a producción (Requiere `npm run deploy`)
- [ ] Verificar sync end-to-end

---

### Estado Actual:
El código está totalmente escrito y estructurado. He configurado Vitest y creado el primer test de integración lógica.
Para proceder con el testing real y el deploy, es fundamental completar la configuración de los secretos detallada en `CONFIG_GUIDE.md`.
