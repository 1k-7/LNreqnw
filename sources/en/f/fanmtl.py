# -*- coding: utf-8 -*-
import logging
import time
import random
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

# Try to import curl_cffi for the bypass
try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        if not HAS_CFFI:
            raise Exception("Please install 'curl_cffi' to use this source: pip install curl_cffi")

        # [TURBO] 60-80 threads is safe because we are impersonating a real browser
        # and not using a slow proxy tunnel.
        self.init_executor(60) 
        
        # 1. Replace the default scraper with a High-Performance Browser Session
        # 'impersonate="chrome"' makes the TLS handshake look identical to a real browser.
        self.scraper = cffi_requests.Session(impersonate="chrome")
        
        # 2. Add standard headers (User-Agent is handled automatically by impersonate)
        self.scraper.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # 3. Remove all proxies (Use Direct Connection for Max Speed)
        self.scraper.proxies = {}
        
        logger.info(f"FanMTL NATIVE: TLS Impersonation Active (Chrome) | Proxy: Direct (Fast)")
        self.cleaner.bad_css.update({'div[align="center"]'})
        
    def get_soup_safe(self, url):
        """
        Wrapper to handle fetching with TLS Impersonation.
        """
        retries = 0
        while True:
            try:
                # Use the cffi scraper directly
                response = self.scraper.get(url, timeout=10)
                
                # Handle Cloudflare specific responses
                if response.status_code in [403, 503]:
                    if "just a moment" in response.text.lower():
                        logger.warning(f"Cloudflare Challenge on {url}. Retrying...")
                        time.sleep(2)
                        retries += 1
                        if retries > 5: raise Exception("Cloudflare Loop")
                        continue

                response.raise_for_status()
                
                # Use the crawler's soup maker
                return self.make_soup(response)

            except Exception as e:
                msg = str(e).lower()
                if "404" in msg:
                    logger.error(f"Permanent Error (404): {url}")
                    return self.make_soup("<html></html>")
                
                # Connection errors or temporary blocks
                logger.warning(f"Request Error: {e}. Retrying in 5s...")
                time.sleep(5)
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
        
        # Parse Current Page
        self.parse_chapter_list(soup)

        # Handle Pagination
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
                
                # AJAX Header needed for pagination scripts
                ajax_headers = {"X-Requested-With": "XMLHttpRequest"}

                for page in range(page_count):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    # We must use self.scraper.get directly to pass specific headers here
                    try:
                        resp = self.scraper.get(url, headers=ajax_headers, timeout=10)
                        page_soup = self.make_soup(resp)
                        self.parse_chapter_list(page_soup)
                    except Exception as e:
                        logger.error(f"Pagination fetch failed: {e}")

            except Exception as e:
                logger.error(f"Pagination logic error: {e}")

        self.chapters.sort(key=lambda x: x["id"] if isinstance(x, dict) else getattr(x, "id", 0))

    def parse_chapter_list(self, soup):
        if not soup: return
        for a in soup.select("ul.chapter-list li a"):
            try:
                url = self.absolute_url(a["href"])
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=url,
                    title=a.select_one(".chapter-title").text.strip(),
                ))
            except: pass

    def download_chapter_body(self, chapter):
        """
        Downloads chapter content using the high-speed TLS-impersonating session.
        """
        retries = 0
        while True:
            try:
                # Direct high-speed request
                response = self.scraper.get(chapter["url"], timeout=10)
                
                # Basic error checking
                if response.status_code == 404:
                    return "<p><i>[Chapter link is broken (Error 404)]</i></p>"
                
                response.raise_for_status()
                soup = self.make_soup(response)
                
                body = soup.select_one("#chapter-article .chapter-content")
                content = self.cleaner.extract_contents(body).strip() if body else ""
                
                if content:
                    return content
                
                if retries >= 2:
                    return "<p><i>[Chapter content unavailable]</i></p>"

                retries += 1
                time.sleep(1) # Tiny sleep on soft fail
                
            except Exception as e:
                if "403" in str(e) or "challenge" in str(e).lower():
                    logger.warning(f"Blocked on {chapter['title']}. Retrying...")
                    time.sleep(5) # Wait a bit if blocked
                    continue
                    
                logger.warning(f"Error on {chapter['title']}: {e}")
                time.sleep(2)
                continue
