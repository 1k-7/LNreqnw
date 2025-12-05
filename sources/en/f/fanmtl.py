# -*- coding: utf-8 -*-
import logging
import time
import random
import requests.exceptions
import requests
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler
from lncrawl.assets.user_agents import user_agents

logger = logging.getLogger(__name__)

HALT_403_SIGNAL = "MANUAL_RESTART_HALT_403"

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] Increase threads for raw speed
        self.init_executor(60) 
        
        self.random_ua = random.choice(user_agents)
        
        # 1. Setup the SAFE Scraper (Uses WARP Proxy + Cloudscraper)
        # This is slow but can bypass anything.
        self.scraper.headers.update({
            "User-Agent": self.random_ua,
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        
        self.warp_proxies = {
            'http': 'socks5://127.0.0.1:40000',
            'https': 'socks5://127.0.0.1:40000',
        }
        self.scraper.proxies.update(self.warp_proxies)

        # 2. Setup the FAST Scraper (Direct Connection + Standard Requests)
        # This bypasses WARP completely for maximum throughput.
        self.fast_scraper = requests.Session()
        self.fast_scraper.headers.update(self.scraper.headers)
        
        # IMPORTANT: NO PROXIES for the fast scraper
        self.fast_scraper.proxies.clear() 
        
        # Optimize connection pooling for speed
        adapter = requests.adapters.HTTPAdapter(pool_connections=60, pool_maxsize=60)
        self.fast_scraper.mount("https://", adapter)
        self.fast_scraper.mount("http://", adapter)
        
        logger.info(f"FanMTL HYBRID: UA -> {self.random_ua[:30]}... | Strategy -> Direct First, WARP Fallback")
        self.cleaner.bad_css.update({'div[align="center"]'})
        
    def get_soup_safe(self, url, headers=None):
        """
        Always use the SAFE scraper (WARP) for sensitive pages like 
        Table of Contents and Pagination to avoid getting the main IP banned.
        """
        while True:
            try:
                soup = self.get_soup(url, headers=headers)
                if "just a moment" in str(soup.title).lower():
                    raise Exception("Cloudflare Challenge Detected")
                return soup
            except Exception as e:
                msg = str(e).lower()
                if "404" in msg:
                    logger.error(f"Permanent Error (404): {url}")
                    return self.make_soup("<html><body></body></html>")
                if "403" in msg or "challenge" in msg:
                    logger.warning(f"403/Challenge on info page. Retrying via WARP in 10s...")
                    time.sleep(10) 
                    continue 
                time.sleep(2)
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

        self.parse_chapter_list(soup)

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
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=url,
                    title=a.select_one(".chapter-title").text.strip(),
                ))
            except: pass

    def download_chapter_body(self, chapter):
        """
        HYBRID STRATEGY:
        1. Try DIRECT connection (Fastest).
        2. If 403/Block -> Fallback to WARP (Slow but Safe).
        """
        url = chapter["url"]
        
        # --- ATTEMPT 1: FAST (Direct Connection) ---
        try:
            # Timeout is short (5s) because we want to fail fast and switch to WARP if blocked
            response = self.fast_scraper.get(url, timeout=5)
            
            # Check if Cloudflare blocked us
            if response.status_code == 403 or "just a moment" in response.text.lower():
                raise requests.exceptions.RequestException("Direct Access Blocked")
            
            response.raise_for_status()
            soup = self.make_soup(response)
            body = soup.select_one("#chapter-article .chapter-content")
            content = self.cleaner.extract_contents(body).strip() if body else ""
            
            if content:
                return content
                
        except Exception:
            # Silently ignore direct failure and proceed to WARP fallback
            pass

        # --- ATTEMPT 2: SAFE (WARP Proxy) ---
        try:
            # This uses the 'scraper' object which has the WARP proxy configured
            soup = self.get_soup_safe(url)
            body = soup.select_one("#chapter-article .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            return ""
