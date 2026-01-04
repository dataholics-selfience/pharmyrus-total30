"""Merge inteligente de patentes BR de múltiplas fontes"""
import logging

logger = logging.getLogger("pharmyrus")


def merge_br_patents(epo_brs, inpi_brs):
    """
    Merge inteligente de patentes BR do EPO e INPI
    Remove duplicatas e combina dados complementares
    """
    merged = {}
    
    # Add EPO patents
    for patent in epo_brs:
        pn = patent.get("patent_number", "")
        if pn:
            merged[pn] = {
                **patent,
                "sources": ["EPO"],
                "applicants": patent.get("applicants", []),
                "inventors": patent.get("inventors", []),
                "ipc_codes": patent.get("ipc_codes", []),
            }
    
    # Merge INPI patents
    for patent in inpi_brs:
        pn = patent.get("patent_number", "")
        if not pn:
            continue
        
        if pn in merged:
            # Merge with existing EPO data
            existing = merged[pn]
            
            if "INPI" not in existing["sources"]:
                existing["sources"].append("INPI")
            
            # INPI data takes priority for some fields
            existing["title"] = patent.get("title") or existing.get("title")
            existing["abstract"] = patent.get("abstract") or existing.get("abstract")
            existing["attorney"] = patent.get("attorney")
            existing["national_phase_date"] = patent.get("national_phase_date")
            existing["link_national"] = patent.get("link_national")
            
            # Merge lists
            if patent.get("applicants"):
                existing["applicants"] = list(set(existing.get("applicants", []) + patent["applicants"]))
            if patent.get("inventors"):
                existing["inventors"] = list(set(existing.get("inventors", []) + patent["inventors"]))
            if patent.get("ipc_codes"):
                existing["ipc_codes"] = list(set(existing.get("ipc_codes", []) + patent["ipc_codes"]))
            
            # INPI-exclusive data
            existing["documents"] = patent.get("documents", [])
            existing["despachos"] = patent.get("despachos", [])
            existing["pct_number"] = patent.get("pct_number") or existing.get("pct_number")
            existing["pct_date"] = patent.get("pct_date") or existing.get("pct_date")
            existing["wo_number"] = patent.get("wo_number") or existing.get("wo_number")
            existing["wo_date"] = patent.get("wo_date") or existing.get("wo_date")
            
        else:
            # New patent from INPI only
            merged[pn] = {
                **patent,
                "sources": ["INPI"],
                "applicants": patent.get("applicants", []),
                "inventors": patent.get("inventors", []),
                "ipc_codes": patent.get("ipc_codes", []),
                "documents": patent.get("documents", []),
                "despachos": patent.get("despachos", [])
            }
    
    result = list(merged.values())
    logger.info(f"✅ Merged: {len(epo_brs)} EPO + {len(inpi_brs)} INPI = {len(result)} unique BRs")
    
    return result
