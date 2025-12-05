# -*- coding: utf-8 -*-
import logging
import time
import requests
from urllib.parse import urlparse, parse_qs
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

# Import the solver (Cloudscraper) and the speed engine (curl_cffi)
from lncrawl.cloudscraper import create_scraper
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

        self.init_executor(60) 
        
        # 1. Setup the SOLVER (Cloudscraper)
        # Its only job is to solve the initial JS challenge and get cookies.
        self.solver = create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True},
            delay=10
        )

        # 2. Setup the RUNNER (Curl_CFFI)
        # This does the actual downloading at high speed.
        self.runner = cffi_requests.Session(impersonate="chrome120")
        
        # Sync headers
        self.runner.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
        })

        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: Cookie Handover (Solver -> Turbo Runner)")

    def refresh_cookies(self, url):
        """
        Uses the slow Solver to pass Cloudflare, then hands cookies to the fast Runner.
        """
        logger.warning("🔒 Encountered Cloudflare. Launching Solver...")
        
        # 1. Solve with Cloudscraper
        # This request might take 5-10 seconds while it solves JS
        resp = self.solver.get(url)
        
        if resp.status_code == 403:
            logger.critical("❌ Solver failed. IP might be strictly banned or Captcha required.")
            raise Exception("Manual Intervention Required: Cloudflare Blocked Solver")
            
        # 2. Extract the 'Golden Ticket' (cf_clearance)
        cf_cookie = self.solver.cookies.get('cf_clearance')
        ua = self.solver.headers.get('User-Agent')
        
        if not cf_cookie:
            logger.warning("⚠️ No cf_clearance cookie found, but access seemed OK.")
            
        # 3. Inject into Fast Runner
        if cf_cookie:
            self.runner.cookies.set('cf_clearance', cf_cookie, domain='.fanmtl.com')
        
        # Sync User-Agent to match the solver exactly to avoid mismatch detection
        if ua:
            self.runner.headers['User-Agent'] = ua
            
        self.cookies_synced = True
        logger.info("✅ Cookies Handed Over! Switching to Turbo Mode.")
        return resp

    def get_soup_safe(self, url):
        """
        Smart wrapper that handles the handover automatically.
        """
        retries = 0
        while True:
            try:
                # STEP 1: Try Fast Runner
                response = self.runner.get(url, timeout=10)
                
                # Check if we hit a challenge
                if response.status_code in [403, 503] and "just a moment" in response.text.lower():
                    logger.warning("⛔ Turbo session expired. Refreshing cookies...")
                    self.refresh_cookies(url)
                    continue # Retry loop with new cookies

                response.raise_for_status()
                return self.make_soup(response)

            except Exception as e:
                # If it's a 403/503 error, try to refresh cookies once
                if "403" in str(e) or "503" in str(e):
                    if retries == 0:
                        try:
                            self.refresh_cookies(url)
                            retries += 1
                            continue
                        except: pass

                if "404" in str(e):
                    logger.error(f"Permanent Error (404): {url}")
                    return self.make_soup("<html></html>")

                logger.warning(f"Request Error: {e}. Retrying in 5s...")
                time.sleep(5)
                continue

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # Initial request will likely trigger the Solver immediately
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
                    # Pagination requests also use the safe wrapper
                    page_soup = self.get_soup_safe(url) 
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
        # This uses the runner which stays valid as long as the cookie is alive.
        # If it expires, get_soup_safe will auto-refresh it.
        try:
            soup = self.get_soup_safe(chapter["url"])
            body = soup.select_one("#chapter-article .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception:
            return ""
