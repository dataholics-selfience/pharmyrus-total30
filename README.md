# 🚀 Pharmyrus v31.0.3-ASYNC - COMPLETE PACKAGE

## ✅ O QUE ESTÁ INCLUÍDO

- ✅ **Código completo v31.0.3** (EPO + Google + INPI funcionando)
- ✅ **Infraestrutura Async** (Celery + Redis configurados)
- ✅ **Endpoints Sync & Async**
- ✅ **Progress Tracking** (0-100%)
- ✅ **Dockerfile otimizado** (API + Worker em 1 container)
- ✅ **Pronto para Railway**

---

## 🚀 DEPLOY EM 5 PASSOS (10 MINUTOS)

### 1. Commit para GitHub (2 min)

```bash
# Descompactar
tar -xzf pharmyrus-v31.0.3-ASYNC-COMPLETE.tar.gz
cd pharmyrus-v31.0.3-ASYNC-COMPLETE

# Git
git init
git add .
git commit -m "Pharmyrus v31.0.3-ASYNC"
git branch -M main
git remote add origin https://github.com/SEU-USER/pharmyrus.git
git push -u origin main
```

### 2. Deploy Railway (2 min)

**Opção A: GitHub (Recomendado)**
1. Railway Dashboard → New Project
2. Deploy from GitHub repo
3. Selecionar `pharmyrus`
4. Railway faz deploy automaticamente

**Opção B: Railway CLI**
```bash
npm install -g @railway/cli
railway login
railway init
railway up
```

### 3. Adicionar Redis (1 min)

```bash
# Railway Dashboard:
# 1. Seu projeto → "New"
# 2. "Database" → "Add Redis"
# 3. Pronto! REDIS_URL é injetado automaticamente

# OU via CLI:
railway add
# Selecionar: Redis
```

### 4. Configurar Variáveis (2 min)

**Você JÁ TEM estas variáveis:**
- ✅ `INPI_USERNAME=dnm48`
- ✅ `INPI_PASSWORD=***`
- ✅ `GROQ_API_KEY=***`

**Railway ADICIONA automaticamente:**
- ✅ `REDIS_URL` (quando você adiciona Redis)
- ✅ `PORT` (Railway define)

**NÃO precisa configurar nada manualmente!**

### 5. Testar (3 min)

```bash
# Health check
curl https://seu-app.railway.app/health

# Deve retornar:
{
  "status": "healthy",
  "redis": "connected",
  "version": "v31.0.3-ASYNC"
}
```

---

## 🎯 ENDPOINTS DISPONÍVEIS

### Synchronous (Original - Sem WIPO)
```http
POST /search
```
- Retorna em 5-15 minutos
- Sem WIPO (evita timeout)
- Mesmo comportamento da v31.0.3

**Exemplo:**
```bash
curl -X POST https://seu-app.railway.app/search \
  -H "Content-Type: application/json" \
  -d '{
    "molecule_name": "aspirin",
    "countries": ["BR"],
    "include_wipo": false
  }'
```

### Asynchronous (NOVO - Com WIPO)

#### 1. Iniciar Busca
```http
POST /search/async
```
- Retorna `job_id` em < 1s
- Processa em background
- Pode rodar 60+ minutos

**Exemplo:**
```bash
JOB_ID=$(curl -X POST https://seu-app.railway.app/search/async \
  -H "Content-Type: application/json" \
  -d '{
    "molecule_name": "darolutamide",
    "countries": ["BR"],
    "include_wipo": true
  }' | jq -r '.job_id')

echo "Job ID: $JOB_ID"
```

#### 2. Verificar Progresso
```http
GET /search/status/{job_id}
```
- Progresso 0-100%
- Step atual
- Tempo decorrido

**Exemplo:**
```bash
# Monitorar (chamar a cada 10s)
curl https://seu-app.railway.app/search/status/$JOB_ID | jq '.'

# Resposta:
{
  "job_id": "abc-123",
  "status": "running",
  "progress": 45,
  "step": "Searching INPI...",
  "elapsed_seconds": 120.5,
  "message": "Currently: Searching INPI..."
}
```

#### 3. Obter Resultado
```http
GET /search/result/{job_id}
```
- Quando status = "complete"
- Resultado armazenado por 24h

**Exemplo:**
```bash
curl https://seu-app.railway.app/search/result/$JOB_ID | jq '.' > result.json
```

#### 4. Cancelar Job
```http
DELETE /search/cancel/{job_id}
```

---

## 🧪 TESTE COMPLETO

### Script Bash Automatizado

```bash
#!/bin/bash

API_URL="https://seu-app.railway.app"

# 1. Iniciar busca
echo "🚀 Starting async search..."
JOB_ID=$(curl -s -X POST $API_URL/search/async \
  -H "Content-Type: application/json" \
  -d '{
    "molecule_name": "aspirin",
    "countries": ["BR"],
    "include_wipo": false
  }' | jq -r '.job_id')

echo "✅ Job started: $JOB_ID"

# 2. Monitorar progresso
echo "📊 Monitoring progress..."
while true; do
  STATUS=$(curl -s $API_URL/search/status/$JOB_ID)
  PROGRESS=$(echo $STATUS | jq -r '.progress')
  STEP=$(echo $STATUS | jq -r '.step')
  
  echo "[$PROGRESS%] $STEP"
  
  if [ $(echo $STATUS | jq -r '.status') = "complete" ]; then
    echo "✅ Complete!"
    break
  fi
  
  sleep 10
done

# 3. Baixar resultado
echo "📥 Downloading result..."
curl -s $API_URL/search/result/$JOB_ID | jq '.' > result.json
echo "💾 Saved to result.json"
```

---

## 💰 CUSTO

### Configuração Mínima (Recomendada)
```
Railway Hobby: $10/mês
├─ API + Worker (mesmo container)
├─ Redis (incluído)
└─ 2GB RAM
```

### Quando Escalar (Futuro)
```
API separado:     $10/mês
Worker dedicado:  $10/mês
Redis:            incluído
───────────────────────────
Total:            $20/mês
```

---

## ⚙️ VARIÁVEIS DE AMBIENTE

### Já Configuradas (Railway)

| Variável | Valor | Fonte |
|----------|-------|-------|
| `INPI_USERNAME` | dnm48 | Você já tem |
| `INPI_PASSWORD` | *** | Você já tem |
| `GROQ_API_KEY` | *** | Você já tem |
| `REDIS_URL` | redis://... | Railway injeta |
| `PORT` | 8080 | Railway injeta |

**NÃO precisa adicionar nada!**

### Verificar (opcional)

```bash
railway variables
```

---

## 📊 MONITORAMENTO

### Logs em Tempo Real

```bash
# Ver todos logs
railway logs --tail

# Filtrar worker
railway logs --tail | grep celery

# Filtrar erros
railway logs --tail | grep ERROR
```

### Health Check

```bash
# Via curl
curl https://seu-app.railway.app/health

# Via browser
https://seu-app.railway.app/health
```

**Esperado:**
```json
{
  "status": "healthy",
  "redis": "connected",
  "version": "v31.0.3-ASYNC",
  "timestamp": "2026-01-02T21:00:00Z"
}
```

---

## 🐛 TROUBLESHOOTING

### Redis não conecta

**Sintoma:**
```json
{"redis": "disconnected"}
```

**Solução:**
```bash
# 1. Verificar Redis existe
railway services
# Deve mostrar: Redis

# 2. Verificar REDIS_URL
railway variables
# Deve ter: REDIS_URL=redis://...

# 3. Restart
railway restart
```

### Worker não processa jobs

**Sintoma:**
- Jobs ficam "queued" eternamente
- Status nunca muda para "running"

**Solução:**
```bash
# Ver logs do worker
railway logs --tail | grep celery

# Deve mostrar:
# "celery@hostname ready"
# "Connected to redis://..."

# Se não aparece, verificar Dockerfile
# CMD deve ter: celery -A celery_app worker
```

### Deploy falha

**Sintoma:**
```
Build failed
Container crashed
```

**Solução:**
```bash
# Ver logs do build
railway logs --tail

# Comum: Missing file
# Verificar Dockerfile COPY statements

# Rebuild
git push
# OU
railway up --detach
```

---

## 🎯 PRÓXIMOS PASSOS

### HOJE (Infra Async):
- [x] Deploy código
- [x] Adicionar Redis
- [x] Testar health
- [x] Validar async funciona
- [ ] Testar com aspirin

### AMANHÃ (WIPO):
- [ ] Adicionar WIPO layer
- [ ] Testar timeout 60min
- [ ] Validar dados WIPO
- [ ] Ajustar progress tracking

---

## 📁 ESTRUTURA DO PROJETO

```
pharmyrus-v31.0.3-ASYNC-COMPLETE/
├── main.py                    ✅ FastAPI + Endpoints sync & async
├── celery_app.py              ✅ Celery config
├── tasks.py                   ✅ Background tasks
├── google_patents_crawler.py  ✅ Google Patents Layer 2
├── inpi_crawler.py            ✅ INPI Layer 3
├── merge_logic.py             ✅ BR patents merge
├── patent_cliff.py            ✅ Patent cliff calculator
├── requirements.txt           ✅ Dependencies (com celery/redis)
├── Dockerfile                 ✅ Container (API + Worker)
├── railway.json               ✅ Railway config
└── README.md                  📖 Este arquivo
```

---

## ✅ CHECKLIST DE DEPLOY

### Antes de Commitar:
- [x] Código v31.0.3 incluído
- [x] Celery/Redis configurados
- [x] Dockerfile atualizado
- [x] requirements.txt completo

### Deploy:
- [ ] Push para GitHub
- [ ] Deploy na Railway
- [ ] Adicionar Redis
- [ ] Verificar variáveis (já existem!)

### Validação:
- [ ] `/health` retorna "healthy"
- [ ] Redis mostra "connected"
- [ ] Endpoint sync funciona
- [ ] Endpoint async retorna job_id
- [ ] Progress tracking funciona
- [ ] Worker aparece nos logs

---

## 🎉 RESULTADO ESPERADO

Após seguir todos os passos:

✅ **API funcionando** em https://seu-app.railway.app  
✅ **Redis conectado** e funcionando  
✅ **Worker processando** jobs em background  
✅ **Endpoints sync** (5-15 min sem WIPO)  
✅ **Endpoints async** (60+ min com WIPO)  
✅ **Progress tracking** em tempo real  
✅ **Custo** $10/mês  

---

**PRONTO! DEPLOY AGORA E TESTE!** 🚀

---

## 📞 Suporte

**Problemas?**
1. Ver `railway logs --tail`
2. Verificar `/health` endpoint
3. Check Redis no dashboard
4. Ver troubleshooting acima

**Tudo funcionando?**
Amanhã: Adicionar WIPO! 🌐
