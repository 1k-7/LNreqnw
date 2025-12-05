# -*- coding: utf-8 -*-
import logging
import time
import requests.exceptions
import requests
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

logger = logging.getLogger(__name__)

# --- GLOBAL SIGNAL FOR MANUAL RESTART HALT ---
HALT_403_SIGNAL = "MANUAL_RESTART_HALT_403"
# ---------------------------------------------

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # Reduced max_workers for low RAM usage and stability against 429 errors
        self.init_executor(10) 
        
        # [CRITICAL] Do NOT overwrite self.scraper with requests.Session().
        # We must use the Cloudscraper instance from the parent Crawler class 
        # to bypass Cloudflare protection.

        # Add headers (Safe to add Accept-Language, but DO NOT overwrite User-Agent)
        self.scraper.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
        })

        self.cleaner.bad_css.update({'div[align="center"]'})

        # Standard retries for network glitches
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)
        
    def get_soup_safe(self, url, headers=None):
        """Wrapper to pause on errors during novel info / TOC fetching."""
        while True:
            try:
                # Use self.get_soup which uses the properly configured self.scraper
                # and handles the AJAX headers correctly if passed.
                soup = self.get_soup(url, headers=headers)
                
                if "just a moment" in str(soup.title).lower():
                    # This means auto_refresh_on_403 failed or hit max retries
                    raise Exception("Cloudflare Challenge Detected")
                
                return soup
            except Exception as e:
                msg = str(e).lower()
                if "404" in msg:
                    logger.error(f"Permanent Error (404): {url}")
                    return self.make_soup("<html><body></body></html>")
                
                if "403" in msg or "challenge detected" in msg:
                    logger.critical(f"403 Forbidden/Challenge on {url}. Waiting 60s...")
                    time.sleep(60) 
                    continue 
                
                if "429" in msg:
                    logger.warning("Rate Limit (429). Sleeping 60s...")
                    time.sleep(60)
                    continue
                
                logger.warning(f"Connection Error: {e}. Retrying in 10s...")
                time.sleep(10)
                continue

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # 1. Main Page (Standard Request - Cloudscraper handles headers)
        soup = self.get_soup_safe(self.novel_url)

        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            meta_title = soup.select_one('meta[property="og:title"]')
            self.novel_title = meta_title.get("content").strip() if meta_title else "Unknown Title"

        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        author_tag = soup.select_one('.novel-info .author span[itemprop="author"]')
        self.novel_author = author_tag.text.strip() if author_tag else "Unknown"

        summary_div = soup.select_one(".summary .content")
        self.novel_synopsis = summary_div.get_text("\n\n").strip() if summary_div else ""

        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []
        self.chapter_urls = set()

        # 2. Parse initial chapters
        self.parse_chapter_list(soup)

        # 3. Handle Pagination (Requires specific AJAX header)
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        if pagination_links:
            try:
                last_page = pagination_links[-1]
                href = last_page.get("href")
                common_url = self.absolute_url(href).split("?")[0]
                query = parse_qs(urlparse(href).query)
                
                page_params = query.get("page", ["0"])
                page_count = int(page_params[0]) + 1
                wjm = query.get("wjm", [""])[0]

                # Only add this header for these specific requests
                ajax_headers = {"X-Requested-With": "XMLHttpRequest"}

                for page in range(page_count):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    page_soup = self.get_soup_safe(url, headers=ajax_headers)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        self.chapters.sort(key=lambda x: x["id"] if isinstance(x, dict) else getattr(x, "id", 0))

    def parse_chapter_list(self, soup):
        if not soup: return
        for a in soup.select("ul.chapter-list li a"):
            try:
                url = self.absolute_url(a["href"])
                if url in self.chapter_urls: continue
                self.chapter_urls.add(url)
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=url,
                    title=a.select_one(".chapter-title").text.strip(),
                ))
            except: pass

    def download_chapter_body(self, chapter):
        # Uses standard scraper.get_soup which handles cookies/headers automatically
        soup = self.get_soup_safe(chapter["url"])
        body = soup.select_one("#chapter-article .chapter-content")
        return self.cleaner.extract_contents(body).strip() if body else ""
