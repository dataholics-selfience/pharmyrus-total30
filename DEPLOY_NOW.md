# ⚡ DEPLOY AGORA - 5 COMANDOS

## 1️⃣ DESCOMPACTAR (10s)

```bash
tar -xzf pharmyrus-v31.0.3-ASYNC-COMPLETE.tar.gz
cd pharmyrus-v31.0.3-ASYNC-COMPLETE
```

## 2️⃣ GIT PUSH (1 min)

```bash
git init
git add .
git commit -m "Pharmyrus async"
git branch -M main
git remote add origin https://github.com/SEU-USER/pharmyrus.git
git push -u origin main
```

## 3️⃣ RAILWAY DEPLOY (2 min)

**Railway Dashboard:**
1. New Project
2. Deploy from GitHub
3. Selecionar repo `pharmyrus`
4. Aguardar deploy

## 4️⃣ ADICIONAR REDIS (1 min)

**Railway Dashboard:**
1. Seu projeto → "New"
2. "Database" → "Add Redis"
3. Pronto!

## 5️⃣ TESTAR (30s)

```bash
curl https://seu-app.railway.app/health
```

**Esperado:**
```json
{
  "status": "healthy",
  "redis": "connected"
}
```

---

## ✅ VARIÁVEIS

**Você JÁ TEM:**
- `INPI_USERNAME=dnm48` ✓
- `INPI_PASSWORD` ✓
- `GROQ_API_KEY` ✓

**Railway ADICIONA:**
- `REDIS_URL` (automático)
- `PORT` (automático)

**NÃO precisa fazer NADA!**

---

## 🧪 TESTE RÁPIDO

```bash
# Async search
JOB=$(curl -X POST https://seu-app.railway.app/search/async \
  -H "Content-Type: application/json" \
  -d '{"molecule_name":"aspirin","countries":["BR"]}' \
  | jq -r '.job_id')

# Status (repetir até complete)
curl https://seu-app.railway.app/search/status/$JOB | jq '.'

# Resultado
curl https://seu-app.railway.app/search/result/$JOB > result.json
```

---

## 🚨 SE DER ERRO

### Redis não conecta
```bash
railway variables  # Verificar REDIS_URL existe
railway restart    # Restart service
```

### Worker não processa
```bash
railway logs --tail | grep celery
# Deve mostrar: "celery@hostname ready"
```

### Deploy falha
```bash
railway logs --tail
# Ver erro e corrigir
```

---

**DEPLOY EM 5 MINUTOS!** ⚡

Ver `README.md` para detalhes completos.
