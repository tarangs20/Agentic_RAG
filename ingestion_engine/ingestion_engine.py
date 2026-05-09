import asyncio
import yaml
import logging
import json
import time
import hashlib
import sys
import io
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, DefaultMarkdownGenerator
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher

# ==========================================
# 1. HELPERS & ENCODING
# ==========================================
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
BASE_DIR = Path(__file__).parent.resolve()

def normalize_url(url: str) -> str:
    """Includes fragments to ensure sections like #docker are unique files."""
    parsed = urlparse(url)
    fragment = f"#{parsed.fragment}" if parsed.fragment else ""
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}{fragment}".rstrip("/")

def load_config():
    config_path = BASE_DIR / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

cfg = load_config()
data_raw_path = BASE_DIR / cfg['project']['output_base_dir']
logs_path = BASE_DIR / cfg['project']['logs_dir']
logs_path.mkdir(exist_ok=True, parents=True)

# Logger Setup
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = logs_path / f"run_{run_id}.log"
logger = logging.getLogger("IngestionEngine")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%H:%M:%S')

fh = logging.FileHandler(log_file, encoding='utf-8')
sh = logging.StreamHandler()
fh.setFormatter(formatter)
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)

stats = {}

# ==========================================
# 2. HIGH-FIDELITY INGESTION ENGINE
# ==========================================
async def ingest_site(crawler, site, dispatcher):
    name = site['name']
    base_url = normalize_url(site['base_url'])
    path_filter = site.get('path_filter')
    
    site_output_dir = data_raw_path / name
    site_output_dir.mkdir(parents=True, exist_ok=True)

    stats[name] = {"success": 0, "failed": 0, "skipped": 0, "blocked": 0}
    processed_urls = set()

    # Generator settings to preserve tables, lists, and headers
    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": False,
            "ignore_images": True,
            "body_width": 0,
            "skip_internal_links": True
        }
    )

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        markdown_generator=md_generator,
        # SELECTOR: Includes breadcrumbs, headers, and main content areas
        css_selector="div.content, main, article, .breadcrumb, h1, h2, h3, h4",
        # THRESHOLD: 0 ensures short titles and list items are NOT deleted
        word_count_threshold=0,
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=site['max_depth'], 
            max_pages=site['max_pages'],
            include_external=False 
        ),
        stream=True
    )

    logger.info(f"🚀 STARTING HIGH-FIDELITY CRAWL: {name.upper()}")

    async for result in await crawler.arun_many(urls=[base_url], config=run_cfg, dispatcher=dispatcher):
        norm_url = normalize_url(result.url)
        
        if norm_url in processed_urls: continue
        processed_urls.add(norm_url)
        
        if path_filter and path_filter not in norm_url:
            stats[name]["blocked"] += 1
            continue

        file_id = hashlib.md5(norm_url.encode()).hexdigest()[:12]
        file_base = site_output_dir / f"{file_id}"
        md_path = file_base.with_suffix(".md")
        json_path = file_base.with_suffix(".json")

        if site.get('policy') == "skip" and md_path.exists():
            stats[name]["skipped"] += 1
            continue

        if result.success:
            # We use raw_markdown to avoid the "Pruning" that was breaking your tables
            markdown = result.markdown
            
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(markdown)
            
            meta = {
                "url": norm_url,
                "site_name": name,
                "doc_title": result.metadata.get("title", "No Title"),
                "crawl_timestamp": datetime.now().isoformat(),
                "word_count": len(markdown.split())
            }
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)

            logger.info(f"[OK] SAVED: {md_path.name} | {norm_url}")
            stats[name]["success"] += 1
        else:
            logger.error(f"[X] FAIL: {norm_url}")
            stats[name]["failed"] += 1

# ==========================================
# 3. RUNTIME
# ==========================================
async def main():
    start_time = time.time()
    browser_cfg = BrowserConfig(headless=True)
    dispatcher = MemoryAdaptiveDispatcher(max_session_permit=cfg['project']['concurrency_limit'])

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        tasks = [ingest_site(crawler, s, dispatcher) for s in cfg['sites']]
        await asyncio.gather(*tasks)

    duration = round(time.time() - start_time, 2)
    logger.info(f"\nETL COMPLETE | Runtime: {duration}s")
    for s_name, d in stats.items():
        logger.info(f"{s_name}: OK={d['success']}, SKIP={d['skipped']}, BLOCK={d['blocked']}")

if __name__ == "__main__":
    asyncio.run(main())