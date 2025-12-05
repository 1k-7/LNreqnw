# -*- coding: utf-8 -*-
import logging
import time
import requests
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

# Import Selenium Creator from your project
from lncrawl.webdriver.local import create_local
from selenium.webdriver import ChromeOptions

# Import Turbo Runner
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

        # [TURBO] 60 threads for downloading
        self.init_executor(60) 
        
        # 1. Setup the RUNNER (Curl_CFFI)
        # This executes the high-speed downloads once we have the 'key'
        self.runner = cffi_requests.Session(impersonate="chrome120")
        
        self.runner.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
        })
        
        # Force traffic through WARP to match the IP used by the solver
        self.proxy_url = "socks5://127.0.0.1:40000"
        self.runner.proxies = {
            "http": self.proxy_url,
            "https": self.proxy_url
        }

        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: Browser Solver (Selenium) -> Turbo Runner (Curl_CFFI)")

    def refresh_cookies(self, url):
        """
        Launches a REAL headless Chrome browser to solve the Cloudflare Challenge.
        """
        logger.warning("🔒 Encountered Cloudflare. Launching Real Browser Solver...")
        driver = None
        try:
            # 1. Configure Chrome to use WARP
            options = ChromeOptions()
            options.add_argument(f'--proxy-server={self.proxy_url}')
            
            # 2. Start Browser (Headless)
            driver = create_local(headless=True, options=options)
            
            # 3. Visit Page
            logger.info("Browser: Navigating to page...")
            driver.get(url)
            
            # 4. Wait for Challenge (Just a moment...)
            # We wait 15s to ensure the JS challenge completes
            time.sleep(15)
            
            if "Just a moment" in driver.title:
                logger.warning("Browser: Challenge still active. Waiting 10s more...")
                time.sleep(10)

            # 5. Extract the Golden Ticket (cf_clearance)
            cookies = driver.get_cookies()
            ua = driver.execute_script("return navigator.userAgent")
            
            found_cf = False
            for cookie in cookies:
                # Inject into our Turbo Runner
                self.runner.cookies.set(
                    cookie['name'], 
                    cookie['value'], 
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/')
                )
                if cookie['name'] == 'cf_clearance':
                    found_cf = True
            
            # Sync User-Agent to match the browser exactly
            self.runner.headers['User-Agent'] = ua
            
            if found_cf:
                logger.info("✅ Solver Success! 'cf_clearance' secured. Switching to Turbo Mode.")
                self.cookies_synced = True
            else:
                logger.error("❌ Browser finished, but no 'cf_clearance' cookie found. Logic might fail.")
            
        except Exception as e:
            logger.critical(f"❌ Browser Solver Crashed: {e}")
            raise e
        finally:
            if driver:
                driver.quit() # Clean up the heavy browser

    def get_soup_safe(self, url):
        """
        Smart wrapper that handles the handover automatically.
        """
        retries = 0
        while True:
            try:
                # STEP 1: Try Fast Runner
                response = self.runner.get(url, timeout=20)
                
                # Check for Challenge Page (Status 403/503 + Specific Text)
                if response.status_code in [403, 503] and "just a moment" in response.text.lower():
                    if retries == 0:
                        logger.warning("⛔ Turbo session expired/blocked. Refreshing cookies...")
                        self.refresh_cookies(url)
                        retries += 1
                        continue
                    else:
                        raise Exception("Cloudflare Loop (Solver failed to bypass)")

                response.raise_for_status()
                return self.make_soup(response)

            except Exception as e:
                msg = str(e).lower()
                if "404" in msg:
                    logger.error(f"Permanent Error (404): {url}")
                    return self.make_soup("<html></html>")

                # Network errors or timeout
                logger.warning(f"Request Error: {e}. Retrying in 5s...")
                time.sleep(5)
                continue

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # Initial request triggers the Solver
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
                
                # AJAX Header is required for pagination
                ajax_headers = {"X-Requested-With": "XMLHttpRequest"}

                for page in range(page_count):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    # We pass headers manually to the runner here
                    resp = self.runner.get(url, headers=ajax_headers)
                    page_soup = self.make_soup(resp)
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
        try:
            soup = self.get_soup_safe(chapter["url"])
            body = soup.select_one("#chapter-article .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception:
            return ""
