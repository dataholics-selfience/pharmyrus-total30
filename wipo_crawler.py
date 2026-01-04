"""
WIPO PatentScope Crawler V3 - Production Grade
===============================================

Baseado em análise real do DOM dinâmico WIPO (JSF/PrimeFaces)
Testado com WO2019028689 - HTML completo e screenshot validados

ARQUITETURA:
- Playwright para garantir JSF render completo
- BeautifulSoup para parsing resiliente baseado em labels
- Contextos isolados por WO (sem travamento)
- Timeout garantido em cada etapa
- Logs detalhados de falhas reais

ESTRUTURA DOM REAL IDENTIFICADA:
<div class="ps-field ps-biblio-field">
    <span class="ps-field--label">Publication Number</span>
    <span class="ps-field--value">WO/2019/028689</span>
</div>

DADOS DISPONÍVEIS (confirmados no HTML):
- Publication Number, Publication Date
- International Application No., International Filing Date
- IPC, CPC
- Applicants, Inventors
- Title, Abstract
- Priority Data, Agents
"""

import asyncio
import httpx
import logging
from typing import List, Dict, Optional, Any
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import re

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("wipo_v3")

# ============================================================================
# CONSTANTS
# ============================================================================

BASE_URL = "https://patentscope.wipo.int"
SEARCH_URL = f"{BASE_URL}/search/en/result.jsf"
DETAIL_URL = f"{BASE_URL}/search/en/detail.jsf"

# Timeouts (ms)
PAGE_TIMEOUT = 45000  # 45s max por página
NAVIGATION_TIMEOUT = 30000  # 30s para goto
NETWORKIDLE_TIMEOUT = 5000  # 5s após último request

# ============================================================================
# STEP 1: SEARCH WO NUMBERS (HTTPX - FAST)
# ============================================================================

async def search_wipo_wo_numbers(molecule: str, dev_codes: List[str] = None, 
                                  cas: str = None, max_results: int = 50) -> List[str]:
    """
    Busca WO numbers via HTTPX (não precisa Playwright)
    
    Retorna: Lista de WO numbers (ex: ['WO2019028689', 'WO2018036558'])
    """
    query_parts = [molecule]
    if dev_codes:
        query_parts.extend(dev_codes[:3])
    if cas:
        query_parts.append(cas)
    
    query = " OR ".join(query_parts)
    logger.info(f"🔍 WIPO search query: {query}")
    
    params = {"query": f"FP:({query})"}
    
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        try:
            response = await client.get(SEARCH_URL, params=params)
            response.raise_for_status()
            
            # Parse HTML simples para pegar WO numbers
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # WO numbers aparecem em links como: /search/en/detail.jsf?docId=WO2019028689
            wo_numbers = []
            for link in soup.find_all('a', href=True):
                if 'detail.jsf?docId=' in link['href']:
                    match = re.search(r'docId=(WO\d{4}\d{6})', link['href'])
                    if match:
                        wo_numbers.append(match.group(1))
            
            # Remove duplicatas e limita
            wo_numbers = list(dict.fromkeys(wo_numbers))[:max_results]
            
            logger.info(f"✅ Found {len(wo_numbers)} WO patents")
            return wo_numbers
            
        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return []


# ============================================================================
# STEP 2: FETCH DETAIL PAGE (PLAYWRIGHT - JSF DYNAMIC)
# ============================================================================

async def fetch_detail_html(wo_number: str, headless: bool = True) -> Optional[str]:
    """
    Carrega página de detalhe via Playwright e retorna HTML final
    
    CRÍTICO:
    - JSF leva ~25s para carregar completamente
    - Usa contexto isolado (não contamina entre WOs)
    - Timeout garantido (não trava)
    
    Retorna: HTML completo ou None se falhar
    """
    url = f"{DETAIL_URL}?docId={wo_number}"
    
    try:
        async with async_playwright() as p:
            # Contexto isolado para este WO
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = await context.new_page()
            
            # Timeout global da página
            page.set_default_timeout(PAGE_TIMEOUT)
            
            try:
                # Navigate
                logger.info(f"  Loading {wo_number}...")
                await page.goto(url, timeout=NAVIGATION_TIMEOUT, wait_until="domcontentloaded")
                
                # Esperar network idle (JSF faz múltiplos AJAX)
                # IMPORTANTE: Não usar como única condição!
                try:
                    await page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)
                except PlaywrightTimeout:
                    logger.warning(f"  {wo_number}: networkidle timeout, continuing...")
                
                # Esperar dado crítico aparecer (fallback robusto)
                # Se "Publication Number" não aparecer em 10s, desiste
                try:
                    await page.wait_for_selector(
                        'text="Publication Number"',
                        timeout=10000
                    )
                except PlaywrightTimeout:
                    logger.error(f"  {wo_number}: Publication Number never appeared!")
                    await browser.close()
                    return None
                
                # Pegar HTML final
                html = await page.content()
                await browser.close()
                
                logger.info(f"  ✅ HTML loaded: {len(html)} chars")
                return html
                
            except PlaywrightTimeout as e:
                logger.error(f"  ❌ Timeout loading {wo_number}: {e}")
                await browser.close()
                return None
                
            except Exception as e:
                logger.error(f"  ❌ Error loading {wo_number}: {e}")
                await browser.close()
                return None
                
    except Exception as e:
        logger.error(f"❌ Playwright init failed for {wo_number}: {e}")
        return None


# ============================================================================
# STEP 3: PARSE BIBLIO DATA (BEAUTIFULSOUP - LABEL-BASED)
# ============================================================================

def extract_field_by_label(soup: BeautifulSoup, label_text: str) -> Optional[str]:
    """
    Extrai valor de campo baseado no label (estrutura semântica)
    
    Estrutura esperada:
    <div class="ps-field ps-biblio-field">
        <span class="ps-field--label">Publication Number</span>
        <span class="ps-field--value">WO/2019/028689</span>
    </div>
    
    RESILIENTE:
    - Busca por texto do label, não por ID/classe específica
    - Sobe na árvore DOM até achar container
    - Desce para pegar value
    """
    try:
        # Buscar label
        label = soup.find('span', class_='ps-field--label', string=re.compile(label_text, re.IGNORECASE))
        if not label:
            return None
        
        # Subir para div container
        field_div = label.find_parent('div', class_='ps-field')
        if not field_div:
            return None
        
        # Pegar value (span seguinte)
        value_span = field_div.find('span', class_='ps-field--value')
        if not value_span:
            return None
        
        # Extrair texto limpo
        text = value_span.get_text(strip=True, separator=' ')
        return text if text else None
        
    except Exception as e:
        logger.debug(f"Field '{label_text}' extraction failed: {e}")
        return None


def extract_list_field(soup: BeautifulSoup, label_text: str) -> List[str]:
    """
    Extrai campos de lista (Applicants, Inventors)
    
    Estrutura real:
    <span class="ps-field--value">
        <span class="patent-person">
            <ul class="biblio-person-list">
                <li>
                    <span class="biblio-person-list--name">NAME</span>
                </li>
            </ul>
        </span>
    </span>
    """
    try:
        label = soup.find('span', class_='ps-field--label', string=re.compile(label_text, re.IGNORECASE))
        if not label:
            return []
        
        field_div = label.find_parent('div', class_='ps-field')
        if not field_div:
            return []
        
        # Pegar lista de pessoas
        person_list = field_div.find('ul', class_='biblio-person-list')
        if not person_list:
            return []
        
        names = []
        for li in person_list.find_all('li'):
            name_span = li.find('span', class_='biblio-person-list--name')
            if name_span:
                name = name_span.get_text(strip=True)
                if name:
                    names.append(name)
        
        return names
        
    except Exception as e:
        logger.debug(f"List field '{label_text}' extraction failed: {e}")
        return []


def extract_ipc_codes(soup: BeautifulSoup) -> List[str]:
    """
    Extrai códigos IPC
    
    Estrutura:
    <div class="patent-classification">
        <a href="...">C07D 231/14</a>
        <span>2006.1</span>
    </div>
    """
    try:
        ipc_codes = []
        
        # Buscar label IPC
        label = soup.find('span', class_='ps-field--label', string=re.compile('IPC', re.IGNORECASE))
        if not label:
            return []
        
        field_div = label.find_parent('div', class_='ps-field')
        if not field_div:
            return []
        
        # Pegar todos os classification divs
        for classification in field_div.find_all('div', class_='patent-classification'):
            link = classification.find('a')
            if link:
                code = link.get_text(strip=True)
                if code:
                    ipc_codes.append(code)
        
        return ipc_codes
        
    except Exception as e:
        logger.debug(f"IPC extraction failed: {e}")
        return []


def parse_biblio_data(html: str, wo_number: str) -> Dict[str, Any]:
    """
    Parser principal - extrai todos os campos bibliográficos
    
    RESILIENTE:
    - Se campo não existir, retorna None/[]
    - Nunca lança exceção fatal
    - Sempre retorna dict (mesmo que vazio)
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    data = {
        "wo_number": wo_number,
        "source": "WIPO",
        "extraction_successful": False,
        "biblio_data": {}
    }
    
    try:
        # Campos simples
        pub_number = extract_field_by_label(soup, "Publication Number")
        pub_date = extract_field_by_label(soup, "Publication Date")
        app_number = extract_field_by_label(soup, "International Application No")
        filing_date = extract_field_by_label(soup, "International Filing Date")
        title = extract_field_by_label(soup, "Title")
        abstract = extract_field_by_label(soup, "Abstract")
        priority = extract_field_by_label(soup, "Priority Data")
        
        # Campos de lista
        applicants = extract_list_field(soup, "Applicants")
        inventors = extract_list_field(soup, "Inventors")
        
        # IPC codes
        ipc_codes = extract_ipc_codes(soup)
        
        # CPC codes (mesma estrutura que IPC)
        cpc_codes = extract_ipc_codes(soup) if 'CPC' in html else []
        
        # Montar biblio_data
        data["biblio_data"] = {
            "publication_number": pub_number,
            "publication_date": pub_date,
            "application_number": app_number,
            "filing_date": filing_date,
            "title": title,
            "abstract": abstract,
            "applicants": applicants,
            "inventors": inventors,
            "ipc_codes": ipc_codes,
            "cpc_codes": cpc_codes,
            "priority_data": priority
        }
        
        # Considerar sucesso se tiver pelo menos pub_number e title
        if pub_number and title:
            data["extraction_successful"] = True
            logger.info(f"  ✅ Extracted: {pub_number} - {title[:50]}...")
        else:
            logger.warning(f"  ⚠️  Partial extraction: pub_number={pub_number}, title={bool(title)}")
        
    except Exception as e:
        logger.error(f"  ❌ Parsing failed for {wo_number}: {e}")
    
    return data


# ============================================================================
# STEP 4: PROCESS WO (ISOLATED + SAFE)
# ============================================================================

async def process_wo_safe(wo_number: str, headless: bool = True) -> Optional[Dict[str, Any]]:
    """
    Processa um WO de forma isolada e segura
    
    GARANTIAS:
    - Timeout máximo: 60s
    - Nunca trava o loop
    - Sempre retorna (sucesso ou None)
    - Logs claros do motivo de falha
    """
    try:
        # Timeout total para este WO
        result = await asyncio.wait_for(
            _process_wo_internal(wo_number, headless),
            timeout=60.0
        )
        return result
        
    except asyncio.TimeoutError:
        logger.error(f"❌ {wo_number}: TIMEOUT TOTAL (60s)")
        return None
    except Exception as e:
        logger.error(f"❌ {wo_number}: Unexpected error: {e}")
        return None


async def _process_wo_internal(wo_number: str, headless: bool) -> Optional[Dict[str, Any]]:
    """Internal processing (chamado via wait_for)"""
    
    # Step 1: Fetch HTML
    html = await fetch_detail_html(wo_number, headless=headless)
    if not html:
        logger.error(f"  ❌ Failed to fetch HTML for {wo_number}")
        return None
    
    # Step 2: Parse
    data = parse_biblio_data(html, wo_number)
    
    if not data["extraction_successful"]:
        logger.error(f"  ❌ Failed to extract data from {wo_number}")
        # Salvar HTML para debug (opcional)
        # with open(f"debug_{wo_number}.html", "w") as f:
        #     f.write(html)
        return None
    
    return data


# ============================================================================
# MAIN API FUNCTION
# ============================================================================

async def search_wipo_patents(
    molecule: str,
    dev_codes: List[str] = None,
    cas: str = None,
    max_results: int = 50,
    groq_api_key: str = None,
    progress_callback = None,
    headless: bool = True
) -> List[Dict[str, Any]]:
    """
    API Principal do crawler WIPO
    
    Args:
        molecule: Nome da molécula
        dev_codes: Códigos de desenvolvimento
        cas: Número CAS
        max_results: Máximo de WOs para processar
        groq_api_key: (não usado nesta versão, reservado)
        progress_callback: Função para reportar progresso
        headless: Modo headless do Playwright
    
    Returns:
        Lista de dicts com dados completos de cada patente
    """
    logger.info(f"🌐 WIPO V3 search: {molecule}")
    
    # Step 1: Search WO numbers
    if progress_callback:
        progress_callback(0, "Searching WIPO...")
    
    wo_numbers = await search_wipo_wo_numbers(molecule, dev_codes, cas, max_results)
    
    if not wo_numbers:
        logger.warning("No WO patents found")
        return []
    
    # Limitar processamento
    wo_numbers = wo_numbers[:max_results]
    total = len(wo_numbers)
    
    logger.info(f"📄 Processing {total} WO patents...")
    
    # Step 2: Process each WO (isolated)
    results = []
    for i, wo_number in enumerate(wo_numbers, 1):
        logger.info(f"[{i}/{total}] Processing {wo_number}...")
        
        if progress_callback:
            progress_pct = int((i / total) * 100)
            progress_callback(progress_pct, f"Processing {wo_number} ({i}/{total})")
        
        # Processar de forma isolada e segura
        data = await process_wo_safe(wo_number, headless=headless)
        
        if data:
            results.append(data)
        
        # Small delay entre WOs (respeito ao servidor)
        if i < total:
            await asyncio.sleep(1)
    
    logger.info(f"✅ WIPO V3 complete: {len(results)}/{total} patents extracted")
    
    return results


# ============================================================================
# STANDALONE TEST
# ============================================================================

async def test_wipo_v3():
    """Teste standalone"""
    print("🧪 Testing WIPO Crawler V3...")
    print("=" * 60)
    
    results = await search_wipo_patents(
        molecule="darolutamide",
        dev_codes=["ODM-201", "BAY-1841788"],
        max_results=5,
        headless=True
    )
    
    print(f"\n✅ Retrieved {len(results)} patents")
    
    if results:
        print("\n📄 First patent sample:")
        import json
        print(json.dumps(results[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(test_wipo_v3())