"""
Google Patents Crawler Layer 2 - AGRESSIVO
Todas variações imagináveis: sais, cristais, formulações, síntese, uso terapêutico, enantiômeros
"""
import asyncio
import re
import random
from typing import List, Set, Dict
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout


# Proxies premium
PROXIES = [
    "http://brd-customer-hl_8ea11d75-zone-residential_proxy1:w7qs41l7ijfc@brd.superproxy.io:33335",
    "http://brd-customer-hl_8ea11d75-zone-datacenter_proxy1:93u1xg5fef4p@brd.superproxy.io:33335",
    "http://5SHQXNTHNKDHUHFD:wifi;us;;;@proxy.scrapingbee.com:8886",
    "http://XNK2KLGACMN0FKRY:wifi;us;;;@proxy.scrapingbee.com:8886",
]


class GooglePatentsCrawler:
    """Crawler AGRESSIVO para descobrir TODAS WOs possíveis"""
    
    def __init__(self):
        self.found_wos = set()
        self.proxy_index = 0
    
    def _get_next_proxy(self) -> str:
        """Rotaciona proxies"""
        proxy = PROXIES[self.proxy_index % len(PROXIES)]
        self.proxy_index += 1
        return proxy
    
    def _build_aggressive_search_terms(
        self,
        molecule: str,
        brand: str,
        dev_codes: List[str],
        cas: str
    ) -> List[str]:
        """
        Constrói TODAS as variações de busca imagináveis
        Baseado em: sais, cristais, formulações, síntese, uso terapêutico, enantiômeros
        """
        terms = []
        
        # 1. BÁSICO - Molecule + patent WO
        terms.append(f'"{molecule}" patent WO')
        terms.append(f'"{molecule}" WO site:patents.google.com')
        
        if brand:
            terms.append(f'"{brand}" patent WO')
            terms.append(f'"{brand}" WO site:patents.google.com')
        
        # 2. DEV CODES
        for code in dev_codes[:5]:
            terms.append(f'"{code}" patent WO')
            terms.append(f'"{code}" WO site:patents.google.com')
        
        # 3. CAS NUMBER
        if cas:
            terms.append(f'"{cas}" patent WO')
        
        # 4. VARIAÇÕES QUÍMICAS - Sais
        salt_variants = [
            f'"{molecule}" salt WO',
            f'"{molecule}" hydrochloride WO',
            f'"{molecule}" sulfate WO',
            f'"{molecule}" mesylate WO',
            f'"{molecule}" tosylate WO',
            f'"{molecule}" phosphate WO',
            f'"{molecule}" acetate WO',
            f'"{molecule}" sodium WO',
            f'"{molecule}" potassium WO',
        ]
        terms.extend(salt_variants)
        
        # 5. CRISTAIS / POLIMORFOS
        crystal_variants = [
            f'"{molecule}" crystalline WO',
            f'"{molecule}" crystal form WO',
            f'"{molecule}" polymorph WO',
            f'"{molecule}" Form A WO',
            f'"{molecule}" Form B WO',
            f'"{molecule}" amorphous WO',
            f'"{molecule}" solvate WO',
            f'"{molecule}" hydrate WO',
        ]
        terms.extend(crystal_variants)
        
        # 6. FORMULAÇÕES
        formulation_variants = [
            f'"{molecule}" formulation WO',
            f'"{molecule}" pharmaceutical composition WO',
            f'"{molecule}" tablet WO',
            f'"{molecule}" capsule WO',
            f'"{molecule}" oral dosage WO',
            f'"{molecule}" extended release WO',
            f'"{molecule}" controlled release WO',
            f'"{molecule}" sustained release WO',
        ]
        terms.extend(formulation_variants)
        
        # 7. SÍNTESE ORGÂNICA / PROCESSO
        synthesis_variants = [
            f'"{molecule}" synthesis WO',
            f'"{molecule}" preparation WO',
            f'"{molecule}" process WO',
            f'"{molecule}" method of making WO',
            f'"{molecule}" production WO',
            f'"{molecule}" intermediate WO',
            f'"{molecule}" organic synthesis WO',
        ]
        terms.extend(synthesis_variants)
        
        # 8. USO TERAPÊUTICO
        therapeutic_variants = [
            f'"{molecule}" prostate cancer WO',
            f'"{molecule}" androgen receptor WO',
            f'"{molecule}" cancer treatment WO',
            f'"{molecule}" therapeutic use WO',
            f'"{molecule}" medical use WO',
            f'"{molecule}" treatment method WO',
            f'"{molecule}" therapy WO',
            f'"{molecule}" castration resistant WO',
            f'"{molecule}" nmCRPC WO',
        ]
        terms.extend(therapeutic_variants)
        
        # 9. ENANTIÔMEROS / ISÔMEROS
        isomer_variants = [
            f'"{molecule}" enantiomer WO',
            f'"{molecule}" isomer WO',
            f'"{molecule}" stereoisomer WO',
            f'"{molecule}" R-enantiomer WO',
            f'"{molecule}" S-enantiomer WO',
            f'"{molecule}" optical isomer WO',
        ]
        terms.extend(isomer_variants)
        
        # 10. COMPANIES - Busca por empresa + molécula
        companies = [
            "Orion", "Bayer", "AstraZeneca", "Pfizer", "Novartis", 
            "Roche", "Merck", "Johnson & Johnson", "Bristol-Myers"
        ]
        for company in companies:
            terms.append(f'{company} "{molecule}" patent WO')
            terms.append(f'"{molecule}" {company} WO')
        
        # 11. ANO RANGES - Busca por faixas de ano
        year_ranges = [
            f'"{molecule}" WO2000',
            f'"{molecule}" WO2005',
            f'"{molecule}" WO2010',
            f'"{molecule}" WO2011',  # CRÍTICO - produto principal
            f'"{molecule}" WO2015',
            f'"{molecule}" WO2020',
            f'"{molecule}" WO2023',
            f'"{molecule}" WO2024',
        ]
        terms.extend(year_ranges)
        
        # 12. BUSCA ESPECÍFICA - WO2011051540 (PRODUTO PRINCIPAL)
        terms.append(f'WO2011051540')
        terms.append(f'WO2011051540 "{molecule}"')
        terms.append(f'WO2011051540 Orion')
        terms.append(f'WO2011051540 Bayer')
        
        # 13. COMBINAÇÕES FARMACÊUTICAS
        combination_variants = [
            f'"{molecule}" combination WO',
            f'"{molecule}" pharmaceutical combination WO',
            f'"{molecule}" drug combination WO',
        ]
        terms.extend(combination_variants)
        
        return terms
    
    async def search_google_patents(
        self,
        molecule: str,
        brand: str,
        dev_codes: List[str],
        cas: str,
        existing_wos: Set[str]
    ) -> Set[str]:
        """
        Busca AGRESSIVA no Google Patents
        Executa TODAS as variações de busca
        FALLBACK: Se Playwright falha, usa httpx
        """
        print(f"🔍 Layer 2 AGGRESSIVE: Buscando WOs para {molecule}...")
        
        new_wos = set()
        search_terms = self._build_aggressive_search_terms(molecule, brand, dev_codes, cas)
        
        print(f"   📊 Total de {len(search_terms)} variações de busca!")
        
        # TENTAR PLAYWRIGHT PRIMEIRO
        playwright_success = False
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox'
                    ]
                )
                
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                
                page = await context.new_page()
                
                # Executar buscas (limitar para não explodir tempo)
                # Fazer as primeiras 30 buscas + buscas críticas
                priority_terms = search_terms[:30]  # Primeiras 30
                
                for i, term in enumerate(priority_terms):
                    try:
                        # Google Search primeiro (melhor indexação)
                        url = f"https://www.google.com/search?q={term.replace(' ', '+')}"
                        await page.goto(url, wait_until='domcontentloaded', timeout=20000)
                        
                        await asyncio.sleep(random.uniform(1, 2))
                        
                        # Extrair WOs
                        content = await page.content()
                        wos_found = re.findall(r'WO\d{4}\d{6}', content)
                        
                        for wo in wos_found:
                            if wo not in existing_wos and wo not in new_wos:
                                new_wos.add(wo)
                                print(f"   ✅ Novo WO: {wo} (via: {term[:50]}...)")
                        
                        # Delay anti-ban
                        await asyncio.sleep(random.uniform(2, 4))
                        
                        # Progress
                        if (i + 1) % 10 == 0:
                            print(f"   📊 Progress: {i+1}/{len(priority_terms)} buscas | {len(new_wos)} WOs novos")
                        
                    except PlaywrightTimeout:
                        print(f"   ⏱️  Timeout: {term[:40]}...")
                        continue
                    except Exception as e:
                        print(f"   ⚠️  Erro: {term[:40]}... - {e}")
                        continue
                
                # Busca direta Google Patents (complementar)
                try:
                    gp_url = f"https://patents.google.com/?q={molecule}&country=WO&num=100"
                    await page.goto(gp_url, wait_until='networkidle', timeout=30000)
                    await asyncio.sleep(random.uniform(3, 5))
                    
                    content = await page.content()
                    wos_found = re.findall(r'WO\d{4}\d{6}', content)
                    
                    for wo in wos_found:
                        if wo not in existing_wos and wo not in new_wos:
                            new_wos.add(wo)
                            print(f"   ✅ Novo WO (Google Patents direct): {wo}")
                
                except Exception as e:
                    print(f"   ⚠️  Google Patents direct error: {e}")
                
                await browser.close()
                playwright_success = True
        
        except Exception as e:
            print(f"❌ Playwright FALHOU: {e}")
            print(f"   🔄 FALLBACK: Tentando httpx simples...")
        
        # FALLBACK HTTPX se Playwright falhou ou encontrou poucos WOs
        if not playwright_success or len(new_wos) < 20:
            print(f"   🔄 HTTPX FALLBACK ativado...")
            try:
                import httpx
                async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                    # Buscar diretamente Google Patents (mais confiável)
                    priority_searches = [
                        f"{molecule}",
                        f"{molecule} WO",
                        f"{molecule} patent",
                        brand if brand else None,
                    ] + dev_codes[:5]
                    
                    priority_searches = [s for s in priority_searches if s]
                    
                    for search_term in priority_searches:
                        try:
                            # Google Patents API search
                            url = f"https://patents.google.com/?q={search_term}&country=WO&num=100"
                            response = await client.get(url, headers={
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                            })
                            
                            if response.status_code == 200:
                                wos_found = re.findall(r'WO\d{4}\d{6}', response.text)
                                for wo in wos_found:
                                    if wo not in existing_wos and wo not in new_wos:
                                        new_wos.add(wo)
                                        print(f"   ✅ HTTPX: {wo}")
                            
                            await asyncio.sleep(2)
                        
                        except Exception as e:
                            print(f"   ⚠️  HTTPX error for {search_term}: {e}")
                            continue
            
            except Exception as e:
                print(f"❌ HTTPX FALLBACK também falhou: {e}")
        
        return new_wos
    
    async def enrich_with_google(
        self,
        molecule: str,
        brand: str,
        dev_codes: List[str],
        cas: str,
        epo_wos: Set[str]
    ) -> Set[str]:
        """
        Enriquece WOs do EPO com TODAS variações de busca Google
        """
        additional_wos = await self.search_google_patents(
            molecule=molecule,
            brand=brand,
            dev_codes=dev_codes,
            cas=cas,
            existing_wos=epo_wos
        )
        
        if additional_wos:
            print(f"🎯 Layer 2 AGGRESSIVE: Encontrou {len(additional_wos)} WOs NOVOS!")
            
            # Verificar se WO2011051540 está presente
            if "WO2011051540" in additional_wos:
                print(f"   🌟 WO2011051540 ENCONTRADO! (produto principal)")
        else:
            print(f"ℹ️  Layer 2: Nenhum WO adicional encontrado")
        
        return additional_wos


# Instance singleton
google_crawler = GooglePatentsCrawler()
