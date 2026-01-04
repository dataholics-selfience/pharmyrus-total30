"""Patent Cliff Calculator - Calcula expiração de patentes"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger("pharmyrus")


def calculate_patent_expiration(filing_date: str, country: str = "BR") -> Optional[str]:
    """
    Calcula data de expiração da patente
    Regra geral: 20 anos da data de depósito
    """
    if not filing_date:
        return None
    
    try:
        # Parse date (YYYY-MM-DD ou YYYYMMDD)
        if len(filing_date) == 8:
            filing_date = f"{filing_date[:4]}-{filing_date[4:6]}-{filing_date[6:8]}"
        
        filing = datetime.strptime(filing_date, "%Y-%m-%d")
        expiration = filing + timedelta(days=20*365)  # 20 anos
        
        return expiration.strftime("%Y-%m-%d")
    except Exception as e:
        logger.debug(f"Error calculating expiration for {filing_date}: {e}")
        return None


def calculate_patent_cliff(patents: List[Dict]) -> Dict:
    """
    Calcula Patent Cliff - quando as patentes expiram
    
    Retorna:
    - Primeira expiração (global)
    - Última expiração (global)
    - Expiração por país
    - Anos até primeira expiração
    - Status (Expirada, < 2 anos, < 5 anos, > 5 anos)
    """
    now = datetime.now()
    expirations = []
    by_country = {}
    
    for patent in patents:
        country = patent.get("country", "")
        filing_date = patent.get("filing_date")
        
        if filing_date:
            expiration_date = calculate_patent_expiration(filing_date, country)
            
            if expiration_date:
                exp_dt = datetime.strptime(expiration_date, "%Y-%m-%d")
                
                expirations.append({
                    "patent_number": patent.get("patent_number"),
                    "country": country,
                    "filing_date": filing_date,
                    "expiration_date": expiration_date,
                    "years_until_expiration": (exp_dt - now).days / 365.25,
                    "expired": exp_dt < now
                })
                
                if country not in by_country:
                    by_country[country] = []
                by_country[country].append(expiration_date)
    
    if not expirations:
        return {
            "first_expiration": None,
            "last_expiration": None,
            "years_until_cliff": None,
            "status": "Unknown",
            "by_country": {},
            "all_expirations": []
        }
    
    # Sort by expiration date
    expirations.sort(key=lambda x: x["expiration_date"])
    
    first_exp = expirations[0]
    last_exp = expirations[-1]
    years_until = first_exp["years_until_expiration"]
    
    # Status
    if first_exp["expired"]:
        status = "Expired"
    elif years_until < 2:
        status = "Critical (<2 years)"
    elif years_until < 5:
        status = "Warning (<5 years)"
    else:
        status = "Safe (>5 years)"
    
    # Earliest by country
    earliest_by_country = {}
    for country, dates in by_country.items():
        earliest_by_country[country] = min(dates)
    
    return {
        "first_expiration": first_exp["expiration_date"],
        "last_expiration": last_exp["expiration_date"],
        "years_until_cliff": round(years_until, 2),
        "status": status,
        "by_country": earliest_by_country,
        "all_expirations": expirations
    }
