import asyncio
import yaml
import logging
import json
import time
import hashlib
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher

# ==========================================
# 1. UTILITIES & LOGGING
# ==========================================
def normalize_url(url: str) -> str:
    """Removes fragments and trailing slashes for deduplication."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

cfg = load_config()
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
Path(cfg['project']['logs_dir']).mkdir(exist_ok=True)

# Multi-handler Logger (File + Terminal)
logger = logging.getLogger("IngestionEngine")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%H:%M:%S')

fh = logging.FileHandler(Path(cfg['project']['logs_dir']) / f"run_{run_id}.log")
sh = logging.StreamHandler()
fh.setFormatter(formatter)
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)

stats = {}

# ==========================================
# 2. SITE INGESTION WORKER
# ==========================================
async def ingest_site(crawler, site, dispatcher):
    name = site['name']
    base_url = normalize_url(site['base_url'])
    policy = site.get('policy', cfg['project']['global_policy'])
    allowed_domains = site.get('allowed_domains', [])
    output_dir = Path(cfg['project']['output_base_dir']) / name
    output_dir.mkdir(parents=True, exist_ok=True)

    stats[name] = {"success": 0, "failed": 0, "skipped": 0, "bytes": 0}
    processed_urls = set()

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=site['max_depth'], 
            max_pages=site['max_pages'],
            include_external=False 
        ),
        stream=True
    )

    logger.info(f"🚀 STARTING SITE: {name.upper()} (Policy: {policy})")

    async for result in await crawler.arun_many(
        urls=[base_url], 
        config=run_cfg, 
        dispatcher=dispatcher
    ):
        norm_url = normalize_url(result.url)
        
        # URL Deduplication
        if norm_url in processed_urls: continue
        processed_urls.add(norm_url)
        
        # Domain Restriction Logic
        parsed_domain = urlparse(norm_url).netloc
        if allowed_domains and parsed_domain not in allowed_domains:
            logger.info(f"[!] DOMAIN BLOCKED: {norm_url}")
            continue

        # File Naming (MD5 Hash for URL safety)
        file_id = hashlib.md5(norm_url.encode()).hexdigest()[:12]
        file_base = output_dir / f"{file_id}"
        md_path = file_base.with_suffix(".md")
        json_path = file_base.with_suffix(".json")

        # Site-Level Skip Policy
        if policy == "skip" and md_path.exists():
            logger.info(f"[-] SKIPPING: {norm_url}")
            stats[name]["skipped"] += 1
            continue

        if result.success:
            markdown_content = result.markdown
            word_count = len(markdown_content.split())
            
            # Save Raw Content
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            
            # Save Metadata Sidecar
            metadata = {
                "url": norm_url,
                "site_name": name,
                "doc_title": result.metadata.get("title", "Untitled Document"),
                "domain": parsed_domain,
                "crawl_timestamp": datetime.now().isoformat(),
                "word_count": word_count,
                "requires_redaction": site.get("requires_redaction", False) # Governance Tag
            }
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"[OK] SAVED: {norm_url} ({word_count} words)")
            stats[name]["success"] += 1
            stats[name]["bytes"] += len(markdown_content.encode('utf-8'))
        else:
            logger.error(f"[X] FAILED: {norm_url} - Error: {result.error_message[:40]}")
            stats[name]["failed"] += 1

# ==========================================
# 3. ORCHESTRATION & FINAL REPORT
# ==========================================
async def main():
    start_time = time.time()
    
    # Initialize parallel browser environment
    browser_cfg = BrowserConfig(headless=True)
    dispatcher = MemoryAdaptiveDispatcher(
        max_session_permit=cfg['project']['concurrency_limit']
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        # Launch all sites in parallel tasks
        tasks = [ingest_site(crawler, site, dispatcher) for site in cfg['sites']]
        await asyncio.gather(*tasks)

    # Execution Summary
    duration = round(time.time() - start_time, 2)
    logger.info("\n" + "="*65)
    logger.info("FINAL INGESTION SUMMARY")
    logger.info("="*65)
    
    total_files = 0
    total_kb = 0
    
    for site, data in stats.items():
        total_files += data['success']
        total_kb += data['bytes'] / 1024
        logger.info(f"SITE: {site:<15} | OK: {data['success']:>2} | SKIP: {data['skipped']:>2} | FAIL: {data['failed']:>2}")

    logger.info("-" * 65)
    logger.info(f"Total Files Saved: {total_files}")
    logger.info(f"Total Data Volume: {round(total_kb, 2)} KB")
    logger.info(f"Total Runtime:     {duration} seconds")
    logger.info("="*65)

if __name__ == "__main__":
    asyncio.run(main())