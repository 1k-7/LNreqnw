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
        # Reduced max_workers for low RAM usage
        self.init_executor(10) 
        
        # Standard Session
        self.scraper = requests.Session()
        
        # Standard Browser Headers (NO AJAX HEADERS HERE)
        self.scraper.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        })

        self.cleaner.bad_css.update({'div[align="center"]'})

        # Retry logic
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)
        
    def get_soup_safe(self, url, headers=None):
        """
        Wrapper to fetch soup with optional specific headers.
        """
        while True:
            try:
                # Use session headers by default, or merge/override with specific headers
                response = self.scraper.get(url, headers=headers)
                response.raise_for_status()
                soup = self.make_soup(response)
                
                if "just a moment" in str(soup.title).lower():
                    raise Exception("Cloudflare Challenge Detected")
                
                return soup
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code == 404:
                    logger.error(f"Permanent Error (404) fetching {url}")
                    return self.make_soup("<html><body></body></html>")
                if status_code == 403:
                    raise Exception(f"{HALT_403_SIGNAL}: 403 Forbidden.")
                if status_code == 429:
                    logger.warning(f"Rate Limit (429). Sleeping 60s...")
                    time.sleep(60)
                    continue
                
                logger.warning(f"HTTP Error {status_code}. Retrying in 15s...")
                time.sleep(15)
                continue
            except Exception as e:
                logger.warning(f"Connection Error: {e}. Retrying in 10s...")
                time.sleep(10)
                continue

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # 1. Fetch Main Page (Standard Browser Request - No AJAX Header)
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

        # 2. Parse Chapters from Main Page (Chapters 1-100 usually)
        self.parse_chapter_list(soup)

        # 3. Handle Pagination (AJAX Requests)
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        if pagination_links:
            try:
                last_page = pagination_links[-1]
                href = last_page.get("href")
                common_url = self.absolute_url(href).split("?")[0]
                query = parse_qs(urlparse(href).query)
                
                page_params = query.get("page", ["0"])
                page_count = int(page_params[0]) + 1
                
                wjm_params = query.get("wjm", [""])
                wjm = wjm_params[0]

                # Header specifically for pagination scripts (fy.php / fy1.php)
                ajax_headers = {"X-Requested-With": "XMLHttpRequest"}

                for page in range(page_count):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    # Fetch with AJAX header
                    page_soup = self.get_soup_safe(url, headers=ajax_headers)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination logic failed: {e}. Proceeding with extracted chapters.")

        self.chapters.sort(key=lambda x: x["id"] if isinstance(x, dict) else getattr(x, "id", 0))

    def parse_chapter_list(self, soup):
        if not soup: return
        for a in soup.select("ul.chapter-list li a"):
            try:
                url = self.absolute_url(a["href"])
                if url in self.chapter_urls:
                    continue
                
                self.chapter_urls.add(url)
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=url,
                    title=a.select_one(".chapter-title").text.strip(),
                ))
            except: pass

    def download_chapter_body(self, chapter):
        empty_retry_count = 0 
        
        while True:
            try:
                # Standard request for chapter body (No AJAX header)
                response = self.scraper.get(chapter["url"])
                response.raise_for_status() 
                
                soup = self.make_soup(response)
                body = soup.select_one("#chapter-article .chapter-content")
                
                content = self.cleaner.extract_contents(body).strip() if body else ""
                
                if content:
                    return content
                
                if empty_retry_count >= 2: 
                    return "<p><i>[Chapter content unavailable from source]</i></p>"

                empty_retry_count += 1
                time.sleep(2)
                continue 
                
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code == 404:
                    return "<p><i>[Chapter link is broken (Error 404)]</i></p>"
                if status_code == 403:
                    raise Exception(f"{HALT_403_SIGNAL}: 403 Forbidden.")
                if status_code == 429:
                    time.sleep(60)
                    continue
                time.sleep(15)
                continue
            except Exception as e:
                time.sleep(10)
                continue
