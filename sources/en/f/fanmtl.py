# -*- coding: utf-8 -*-
import logging
import time
import random
import requests
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler
from lncrawl.assets.user_agents import user_agents

# Import Selenium
from lncrawl.webdriver.local import create_local
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 50 threads for stability
        self.init_executor(50) 
        
        self.session_ua = random.choice(user_agents)
        
        self.runner = requests.Session()
        self.runner.headers.update({
            "User-Agent": self.session_ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # Force traffic through WARP
        self.proxy_url = "socks5h://127.0.0.1:40000"
        self.runner.proxies = {
            "http": self.proxy_url,
            "https": self.proxy_url
        }

        adapter = requests.adapters.HTTPAdapter(pool_connections=60, pool_maxsize=60)
        self.runner.mount("https://", adapter)
        self.runner.mount("http://", adapter)

        self.scraper = self.runner
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info(f"FanMTL Init: UA -> {self.session_ua[:30]}... | Proxy -> WARP")

    def click_shadow_checkbox(self, driver):
        """Attempts to click Cloudflare checkbox inside Shadow DOM"""
        try:
            driver.execute_script("""
                function clickShadow() {
                    let shadowHosts = document.querySelectorAll('*');
                    for (let host of shadowHosts) {
                        if (host.shadowRoot) {
                            let checkbox = host.shadowRoot.querySelector('input[type="checkbox"]');
                            if (checkbox) { checkbox.click(); return true; }
                            let button = host.shadowRoot.querySelector('button'); # Sometimes it's a button
                            if (button) { button.click(); return true; }
                        }
                    }
                    return false;
                }
                clickShadow();
            """)
        except:
            pass

    def open_browser_and_solve(self, url, return_html=False):
        """
        Launches a REAL browser and waits specifically for the CHAPTER LIST.
        """
        logger.warning(f"🔒 Launching Browser Solver for: {url}")
        driver = None
        try:
            options = ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument(f'--proxy-server={self.proxy_url}')
            options.add_argument(f'--user-agent={self.session_ua}')
            
            driver = create_local(headless=True, options=options)
            driver.set_page_load_timeout(60)
            driver.get(url)
            
            # --- ROBUST SOLVER LOOP ---
            # Try for up to 60 seconds to get the real content
            success = False
            for attempt in range(6):
                try:
                    # 1. Check if we are already good (Look for chapter list)
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "ul.chapter-list"))
                    )
                    logger.info("Browser: Chapter list detected! Challenge solved.")
                    success = True
                    break
                except TimeoutException:
                    # 2. Not found yet. Check for Cloudflare frames/checkboxes
                    logger.info(f"Browser: Content missing. Checking for challenge... ({attempt+1}/6)")
                    
                    # Try standard frame search
                    try:
                        iframes = driver.find_elements(By.TAG_NAME, "iframe")
                        for frame in iframes:
                            try:
                                driver.switch_to.frame(frame)
                                # Try checking for checkbox
                                checkbox = driver.find_elements(By.XPATH, "//input[@type='checkbox']")
                                if checkbox:
                                    checkbox[0].click()
                                    logger.info("Browser: Clicked standard checkbox.")
                                driver.switch_to.default_content()
                            except:
                                driver.switch_to.default_content()
                    except: pass

                    # Try Shadow DOM clicker
                    self.click_shadow_checkbox(driver)
                    
                    # Wait a bit for reload
                    time.sleep(5)

            if not success:
                logger.error("❌ Browser failed to reach novel content after 60s.")
                # Fallback: Return whatever we have, maybe requests can handle it or it's a 404
                # But mostly this means the solver failed.

            # --- SYNC COOKIES ---
            cookies = driver.get_cookies()
            for cookie in cookies:
                self.runner.cookies.set(
                    cookie['name'], 
                    cookie['value'], 
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/')
                )
            
            if return_html:
                return driver.page_source
            
        except Exception as e:
            logger.critical(f"❌ Browser Solver Crashed: {e}")
            if return_html: return None
        finally:
            if driver:
                try: driver.quit()
                except: pass

    def get_soup_safe(self, url, headers=None):
        retries = 0
        while True:
            try:
                req_headers = self.runner.headers.copy()
                if headers: req_headers.update(headers)

                response = self.runner.get(url, headers=req_headers, timeout=20)
                
                # [CRITICAL] Check CONTENT for challenge
                page_text = response.text.lower()
                if response.status_code in [403, 503] or "just a moment" in page_text or "verify you are human" in page_text:
                    if retries == 0:
                        logger.warning("⛔ Cloudflare Blocked Request. Refreshing cookies...")
                        self.open_browser_and_solve(url)
                        retries += 1
                        continue
                    else:
                        raise Exception("Cloudflare Loop")

                response.raise_for_status()
                return self.make_soup(response)

            except Exception as e:
                msg = str(e).lower()
                if "404" in msg:
                    return self.make_soup("<html></html>")

                if retries < 3:
                    logger.warning(f"Request Error: {e}. Retrying...")
                    time.sleep(2)
                    retries += 1
                    continue
                
                logger.error(f"Failed to fetch {url}")
                return self.make_soup("<html></html>")

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # [CRITICAL] Use Browser to fetch the Info Page + Chapters
        # We expect the browser to wait until ul.chapter-list is present
        html_content = self.open_browser_and_solve(self.novel_url, return_html=True)
        
        if not html_content:
            logger.error("Failed to get HTML from browser. Fallback to requests...")
            soup = self.get_soup_safe(self.novel_url)
        else:
            soup = self.make_soup(html_content)

        # --- PARSE INFO ---
        possible_title = soup.select_one("h1.novel-title")
        if not possible_title:
            # If browser returned junk, force a raw request retry
            logger.warning("⚠️ Title not found. Trying raw request...")
            soup = self.get_soup_safe(self.novel_url)
            possible_title = soup.select_one("h1.novel-title")

        self.novel_title = possible_title.text.strip() if possible_title else "Unknown"
        
        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        author_tag = soup.select_one('.novel-info .author span[itemprop="author"]')
        self.novel_author = author_tag.text.strip() if author_tag else "Unknown"

        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        # Parse chapters from the page we just got
        self.parse_chapter_list(soup)

        # --- PAGINATION ---
        # Now that we (hopefully) have valid cookies, use Requests for pagination
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        if pagination_links:
            try:
                last_page = pagination_links[-1]
                common_url = self.absolute_url(last_page.get("href")).split("?")[0]
                query = parse_qs(urlparse(last_page.get("href")).query)
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
        
        if not self.chapters:
            logger.error(f"❌ Critical: No chapters found for {self.novel_url}.")

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
