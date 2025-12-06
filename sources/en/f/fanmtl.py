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

# Import Selenium Components
from lncrawl.webdriver.local import create_local
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 40 threads is the stability sweet spot for WARP
        self.init_executor(40) 
        
        self.session_ua = random.choice(user_agents)
        
        # 1. Setup the RUNNER (Standard Requests)
        self.runner = requests.Session()
        
        self.runner.headers.update({
            "User-Agent": self.session_ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # Force traffic through WARP (socks5h for remote DNS)
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
                        }
                    }
                    return false;
                }
                clickShadow();
            """)
        except: pass

    def open_browser_and_solve(self, url, return_html=False):
        """
        Launches a REAL browser and waits specifically for the CHAPTER LIST.
        Fails hard if the content is not found (prevents 'No chapters found' error).
        """
        logger.warning(f"🔒 Launching Browser Solver for: {url}")
        driver = None
        try:
            options = ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            # Stealth flags
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-infobars")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            options.add_argument(f'--proxy-server={self.proxy_url}')
            options.add_argument(f'--user-agent={self.session_ua}')
            
            driver = create_local(headless=True, options=options)
            driver.set_page_load_timeout(60)
            driver.get(url)
            
            # --- ROBUST SOLVER LOOP ---
            success = False
            
            # We try for 40 seconds to pass the challenge
            for attempt in range(8):
                try:
                    # 1. Check if we are already good (Look for unique novel element)
                    # 'ul.chapter-list' is the best indicator of success
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "ul.chapter-list"))
                    )
                    logger.info("Browser: Chapter list detected! Challenge solved.")
                    success = True
                    break
                except TimeoutException:
                    # 2. Not found yet. Check for Cloudflare
                    logger.info(f"Browser: Content missing. Checking for challenge... ({attempt+1}/8)")
                    
                    # Try standard frame search & click
                    try:
                        iframes = driver.find_elements(By.TAG_NAME, "iframe")
                        for frame in iframes:
                            try:
                                driver.switch_to.frame(frame)
                                checkbox = driver.find_elements(By.XPATH, "//input[@type='checkbox']")
                                if checkbox:
                                    logger.info("Browser: Found checkbox. Moving mouse and clicking...")
                                    # Use ActionChains for human-like click
                                    actions = ActionChains(driver)
                                    actions.move_to_element(checkbox[0]).pause(0.5).click().perform()
                                driver.switch_to.default_content()
                            except:
                                driver.switch_to.default_content()
                    except: pass

                    # Try Shadow DOM clicker (Fallback)
                    self.click_shadow_checkbox(driver)
                    
                    time.sleep(5)

            if not success:
                # [CRITICAL] If we didn't find the chapter list, DO NOT return HTML.
                # Raise exception so the bot knows it failed instead of saying "No chapters".
                page_src = driver.page_source.lower()
                if "access denied" in page_src or "error 1020" in page_src:
                     raise Exception("Cloudflare IP Ban (Access Denied)")
                
                raise Exception("Solver Timeout: Could not bypass Cloudflare Challenge")

            # --- SYNC COOKIES ---
            cookies = driver.get_cookies()
            found_cf = False
            for cookie in cookies:
                self.runner.cookies.set(
                    cookie['name'], 
                    cookie['value'], 
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/')
                )
                if cookie['name'] == 'cf_clearance':
                    found_cf = True
            
            if found_cf:
                logger.info("✅ Cookies Synced. Turbo Runner is READY.")
            
            if return_html:
                return driver.page_source
            
        except Exception as e:
            logger.critical(f"❌ Browser Solver Failed: {e}")
            # Propagate error to trigger retry logic in caller
            raise e
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
                
                # Check content for challenge (even if status 200)
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
                    # Return empty soup for 404s
                    return self.make_soup("<html></html>")
                
                # For blocks, we must re-raise or retry differently
                if "cloudflare" in msg or "ban" in msg:
                    raise e 

                if retries < 3:
                    logger.warning(f"Request Error: {e}. Retrying...")
                    time.sleep(2)
                    retries += 1
                    continue
                
                logger.error(f"Failed to fetch {url}")
                return self.make_soup("<html></html>")

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # Use Browser to get initial content (Guarantees we see the chapter list)
        try:
            html_content = self.open_browser_and_solve(self.novel_url, return_html=True)
            soup = self.make_soup(html_content)
        except Exception as e:
            logger.error(f"Failed to load novel info via browser: {e}")
            return # Stop here, don't try to parse empty stuff

        possible_title = soup.select_one("h1.novel-title")
        if not possible_title:
            # If title missing despite solver success, something is wrong
            logger.error(f"❌ Page loaded but Title not found. Content might be invalid.")
            return

        self.novel_title = possible_title.text.strip()
        
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

        # Pagination
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
            logger.error(f"❌ No chapters found. Scraper failed to parse content.")

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
