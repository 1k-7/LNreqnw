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
        
        # Overwrite default scraper (which uses memory-intensive cloudscraper) 
        # with a standard, lightweight requests session.
        self.scraper = requests.Session()
        
        # Add headers for stability
        self.scraper.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        })

        self.cleaner.bad_css.update({'div[align="center"]'})

        # Standard retries for network glitches
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        # Match pool size to number of workers
        adapter = HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)
        
    def get_soup_safe(self, url):
        """Wrapper to pause on errors during novel info / TOC fetching."""
        while True:
            try:
                response = self.scraper.get(url)
                response.raise_for_status()
                soup = self.make_soup(response)
                
                if "just a moment" in str(soup.title).lower():
                    raise Exception("Cloudflare Challenge Detected")
                
                return soup
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code == 404:
                    # PERMANENT FAILURE: Return empty content immediately
                    logger.error(f"Permanent Error (404) detected during TOC fetch: {url}. Skipping.")
                    return self.make_soup("<html><body></body></html>") # Return empty soup to proceed
                if status_code == 403:
                    raise Exception(f"{HALT_403_SIGNAL}: 403 Forbidden. Manual restart required.")
                if status_code == 429:
                    logger.warning(f"Rate Limit (429) detected during TOC fetch. Sleeping for 60s...")
                    time.sleep(60)
                    continue
                
                logger.warning(f"HTTP Error {status_code} during TOC fetch. Retrying in 15s...")
                time.sleep(15)
                continue
            except Exception as e:
                logger.warning(f"Connection Error during TOC fetch: {e}. Retrying in 10s...")
                time.sleep(10)
                continue

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
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

        # --- PAGINATION LOGIC ---
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        if not pagination_links:
            self.parse_chapter_list(soup)
        else:
            try:
                last_page = pagination_links[-1]
                href = last_page.get("href")
                common_url = self.absolute_url(href).split("?")[0]
                query = parse_qs(urlparse(href).query)
                
                page_params = query.get("page", ["0"])
                page_count = int(page_params[0]) + 1
                
                wjm_params = query.get("wjm", [""])
                wjm = wjm_params[0]

                for page in range(page_count):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    page_soup = self.get_soup_safe(url)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}. Parsing current page.")
                self.parse_chapter_list(soup)

        self.chapters.sort(key=lambda x: x["id"] if isinstance(x, dict) else getattr(x, "id", 0))

    def parse_chapter_list(self, soup):
        if not soup: return
        for a in soup.select("ul.chapter-list li a"):
            try:
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=self.absolute_url(a["href"]),
                    title=a.select_one(".chapter-title").text.strip(),
                ))
            except: pass

    def download_chapter_body(self, chapter):
        empty_retry_count = 0 
        
        while True:
            try:
                response = self.scraper.get(chapter["url"])
                response.raise_for_status() 
                
                soup = self.make_soup(response)
                body = soup.select_one("#chapter-article .chapter-content")
                
                content = self.cleaner.extract_contents(body).strip() if body else ""
                
                # 1. SUCCESS CHECK: Content found. Return it.
                if content:
                    return content
                
                # --- HANDLE EMPTY CONTENT / SOFT FAILURE ---
                
                # 2. CHECK MAX RETRIES (3 attempts total)
                if empty_retry_count >= 2: 
                    logger.warning(
                        f"Chapter body for {chapter['title']} is confirmed empty after 3 attempts. "
                        "Marking as successful and proceeding."
                    )
                    # Return placeholder to mark success=True for the integrity check
                    return "<p><i>[Chapter content unavailable from source]</i></p>"

                # 3. SOFT FAILURE: Wait and retry.
                empty_retry_count += 1
                logger.warning(
                    f"Empty content detected for {chapter['title']} (Attempt {empty_retry_count}/3). "
                    "Waiting 3 seconds and retrying..."
                )
                time.sleep(3)
                continue 
                
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                
                if status_code == 404:
                    # FIX: Permanent error. Mark as broken and move on.
                    logger.error(f"Permanent Error (404) detected on chapter: {chapter['title']}. Skipping.")
                    return "<p><i>[Chapter link is broken (Error 404)]</i></p>" # Mark as success=True and proceed

                if status_code == 403:
                    # HALT SIGNAL: Stops the entire process
                    logger.critical(f"Permanent Ban (403) detected on chapter: {chapter['title']}")
                    raise Exception(f"{HALT_403_SIGNAL}: 403 Forbidden. Manual restart required.")
                
                if status_code == 429:
                    # WAIT & RETRY: Pauses for 60s and keeps trying this chapter
                    logger.warning(f"Rate Limit (429) detected for {chapter['title']}. Sleeping for 60 seconds and retrying...")
                    time.sleep(60)
                    continue
                    
                # Other HTTP errors (500, 502, 503, 504 are handled by Retry adapter, or catch here)
                logger.warning(f"HTTP Error {status_code} for {chapter['title']}. Retrying in 15 seconds...")
                time.sleep(15)
                continue

            except Exception as e:
                # Catch connection errors, timeouts, etc.
                logger.warning(f"Connection Error for {chapter['title']}: {e}. Retrying in 10 seconds...")
                time.sleep(10)
                continue
