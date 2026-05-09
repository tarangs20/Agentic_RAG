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
from crawl4ai.deep_crawling.filters import FilterChain, DomainFilter, URLPatternFilter
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

    # FIX 1: Respect global_policy as fallback when site-level policy is absent
    effective_policy = site.get('policy') or cfg['project'].get('global_policy', 'skip')

    # FIX 2: Per-site allowed_domains; fall back to base URL's netloc
    allowed_domains = set(
        site.get('allowed_domains') or [urlparse(base_url).netloc]
    )

    site_output_dir = data_raw_path / name
    site_output_dir.mkdir(parents=True, exist_ok=True)

    stats[name] = {"success": 0, "failed": 0, "skipped": 0, "blocked": 0}
    processed_urls = set()

    # Generator settings — all driven from config with safe defaults
    md_cfg = cfg['project'].get('markdown', {})
    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": md_cfg.get('ignore_links', False),
            "ignore_images": md_cfg.get('ignore_images', True),
            "body_width": md_cfg.get('body_width', 0),
            "skip_internal_links": md_cfg.get('skip_internal_links', True),
        }
    )

    # Per-site css_selector — falls back to project-level default, then bare default
    css_selector = site.get(
        'css_selector',
        cfg['project'].get('default_css_selector', "div.content, main, article, .breadcrumb, h1, h2, h3, h4")
    )

    # Build filter chain from config — prevents BFS from queuing wrong domains
    # and wrong path versions (e.g. /24/, /25/) before they are ever fetched.
    # DomainFilter:     only queue URLs on allowed_domains (blocks cwiki.apache.org etc.)
    # URLPatternFilter: only queue URLs matching path_filter (blocks /24/, /25/ etc.)
    #                   reverse=False means "keep URLs that DO match the pattern"
    active_filters = [DomainFilter(allowed_domains=list(allowed_domains))]
    if path_filter:
        active_filters.append(
            URLPatternFilter(patterns=f"*{path_filter}*", use_glob=True, reverse=False)
        )
    filter_chain = FilterChain(filters=active_filters)

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        markdown_generator=md_generator,
        css_selector=css_selector,
        wait_for=site.get('wait_for'),           # wait for JS hydration before scrape
        word_count_threshold=cfg['project'].get('word_count_threshold', 0),
        page_timeout=cfg['project'].get('page_timeout_ms', 30000),
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=site['max_depth'],
            max_pages=site['max_pages'],
            include_external=False,
            filter_chain=filter_chain,
        ),
        stream=True
    )

    logger.info(f"🚀 STARTING HIGH-FIDELITY CRAWL: {name.upper()} | policy={effective_policy}")

    async for result in await crawler.arun_many(urls=[base_url], config=run_cfg, dispatcher=dispatcher):
        norm_url = normalize_url(result.url)

        # Dedup
        if norm_url in processed_urls:
            continue
        processed_urls.add(norm_url)

        # FIX 2: Enforce allowed_domains filter
        result_domain = urlparse(norm_url).netloc
        if result_domain not in allowed_domains:
            logger.debug(f"[DOMAIN-BLOCK] {norm_url}")
            stats[name]["blocked"] += 1
            continue

        # Path filter
        if path_filter and path_filter not in norm_url:
            stats[name]["blocked"] += 1
            continue

        file_id = hashlib.md5(norm_url.encode()).hexdigest()[:12]
        file_base = site_output_dir / f"{file_id}"
        md_path = file_base.with_suffix(".md")
        json_path = file_base.with_suffix(".json")

        # FIX 1: Use resolved effective_policy
        if effective_policy == "skip" and md_path.exists():
            logger.debug(f"[SKIP] Already exists: {md_path.name}")
            stats[name]["skipped"] += 1
            continue

        if result.success:
            # Use raw_markdown to avoid Crawl4AI's content-pruning
            # result.markdown is the pruned object; .raw_markdown is the full string
            markdown_obj = result.markdown
            markdown = (
                markdown_obj.raw_markdown
                if hasattr(markdown_obj, "raw_markdown")
                else str(markdown_obj)
            )

            word_count = len(markdown.split())
            thin_threshold = cfg['project'].get('thin_page_threshold', 20)

            # Warn on thin pages so selector issues are visible in logs immediately
            if word_count < thin_threshold:
                logger.warning(f"[THIN-SKIP] {norm_url} | {word_count} words — skipping")
                stats[name]["skipped"] += 1
                continue

            with open(md_path, "w", encoding="utf-8") as f:
                f.write(markdown)

            # Persist metadata — feeds Presidio and ChromaDB downstream
            meta = {
                "url": norm_url,
                "site_name": name,
                "doc_title": result.metadata.get("title", "No Title"),
                "crawl_timestamp": datetime.now().isoformat(),
                "word_count": word_count,
                "requires_redaction": site.get('requires_redaction', True),
                "domain": site.get('domain', name),
                "source": site.get('source_type', 'web'),
            }
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)

            logger.info(f"[OK] SAVED: {md_path.name} | words={word_count} | {norm_url}")
            stats[name]["success"] += 1
        else:
            logger.error(f"[X] FAIL: {norm_url} | reason={getattr(result, 'error_message', 'unknown')}")
            stats[name]["failed"] += 1


# ==========================================
# 3. RUNTIME
# ==========================================
async def main():
    start_time = time.time()
    browser_cfg = BrowserConfig(
        headless=cfg['project'].get('headless', True),
        viewport_width=cfg['project'].get('viewport_width', 1280),
        viewport_height=cfg['project'].get('viewport_height', 800),
    )
    dispatcher = MemoryAdaptiveDispatcher(
        max_session_permit=cfg['project'].get('max_session_permit', 3)
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        tasks = [ingest_site(crawler, s, dispatcher) for s in cfg['sites']]

        # FIX 6: return_exceptions=True prevents one failing site from
        # killing all other concurrent site tasks silently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for site, result in zip(cfg['sites'], results):
            if isinstance(result, Exception):
                logger.error(f"[SITE-TASK-FAILED] {site['name']}: {result}")

    duration = round(time.time() - start_time, 2)
    logger.info(f"\n{'='*50}")
    logger.info(f"ETL COMPLETE | Runtime: {duration}s")
    logger.info(f"{'='*50}")
    for s_name, d in stats.items():
        logger.info(
            f"  {s_name}: OK={d['success']} | SKIP={d['skipped']} "
            f"| BLOCK={d['blocked']} | FAIL={d['failed']}"
        )


if __name__ == "__main__":
    asyncio.run(main())