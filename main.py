import asyncio
import json
import logging
import os
import re
import time
import argparse
import random
from pathlib import Path
from typing import List, Dict, Any, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

# --- CONFIGURATION ---
TARGET_MAKES = [
    'acura', 'alfa-romeo', 'aston-martin', 'audi', 'bentley', 'bmw', 'bugatti', 'buick',
    'cadillac', 'chevrolet', 'chrysler', 'dodge', 'ferrari', 'ford', 'genesis', 'gmc',
    'honda', 'hummer', 'hyundai', 'infiniti', 'jaguar', 'jeep', 'kia', 'lamborghini',
    'land-rover', 'lexus', 'lincoln', 'lotus', 'maserati', 'maybach', 'mazda', 'mclaren',
    'mercedes-benz', 'mercury', 'mini', 'mitsubishi', 'nissan', 'oldsmobile', 'pontiac',
    'porsche', 'ram', 'rolls-royce', 'saab', 'saturn', 'scion', 'subaru', 'suzuki',
    'tesla', 'toyota', 'volkswagen', 'volvo'
]
TARGET_YEARS = list(range(2000, 2026))  # Scrapes years 2000 through 2025
RESULTS_DIR = Path("results")
LOG_FILE = "scraper.log"
DONE_MARKER = ".done"

# --- PERFORMANCE & ERROR HANDLING ---
MAX_RETRIES = 5  # Number of times to retry a failed page load
INITIAL_BACKOFF_DELAY_SECONDS = 30 # Initial delay after a failure, doubles each time

# --- ANTI-DETECTION CONFIGURATION ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1600, "height": 900},
    {"width": 1280, "height": 720},
    {"width": 1024, "height": 768}
]

# --- LOGGING SETUP ---
def setup_logging():
    """Configures logging to both a file and the console."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# --- ANTI-DETECTION FUNCTIONS ---
async def setup_stealth_page(browser_instance):
    """Sets up a page with comprehensive anti-detection measures."""
    context = await browser_instance.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport=random.choice(VIEWPORTS),
        locale="en-US",
        timezone_id="America/New_York",
        permissions=["geolocation"],
        geolocation={"latitude": 40.7128, "longitude": -74.0060},  # NYC coordinates
        extra_http_headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"'
        }
    )
    
    page = await context.new_page()
    
    # Add comprehensive stealth scripts
    await page.add_init_script("""
        // Remove webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });
        
        // Mock languages and plugins
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });
        
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });
        
        // Mock screen properties
        Object.defineProperty(screen, 'colorDepth', {
            get: () => 24,
        });
        
        Object.defineProperty(screen, 'pixelDepth', {
            get: () => 24,
        });
        
        // Mock permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
        );
        
        // Mock chrome runtime
        window.chrome = {
            runtime: {},
        };
        
        // Mock WebGL
        const getParameter = WebGLRenderingContext.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) {
                return 'Intel Inc.';
            }
            if (parameter === 37446) {
                return 'Intel Iris OpenGL Engine';
            }
            return getParameter(parameter);
        };
        
        // Mock hardware concurrency
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 4,
        });
        
        // Mock device memory
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => 8,
        });
        
        // Mock battery API
        Object.defineProperty(navigator, 'getBattery', {
            get: () => () => Promise.resolve({
                charging: true,
                chargingTime: 0,
                dischargingTime: Infinity,
                level: 1,
            }),
        });
        
        // Mock connection
        Object.defineProperty(navigator, 'connection', {
            get: () => ({
                effectiveType: '4g',
                rtt: 150,
                downlink: 10,
                saveData: false,
            }),
        });
        
        // Override Date.now to avoid time-based detection
        const originalDateNow = Date.now;
        Date.now = function() {
            return originalDateNow() + Math.floor(Math.random() * 1000);
        };
        
        // Mock mouse movements
        let mouseEvents = [];
        document.addEventListener('mousemove', (e) => {
            mouseEvents.push({x: e.clientX, y: e.clientY, time: Date.now()});
            if (mouseEvents.length > 10) mouseEvents.shift();
        });
        
        // Add entropy to Math.random
        const originalRandom = Math.random;
        Math.random = function() {
            return (originalRandom() + performance.now() * 0.000001) % 1;
        };
    """)
    
    return page

async def human_like_delay(min_delay: float = 0.5, max_delay: float = 2.0):
    """Introduces human-like delays with random variations."""
    delay = random.uniform(min_delay, max_delay)
    await asyncio.sleep(delay)

async def simulate_human_behavior(page):
    """Simulates human-like behavior on the page."""
    try:
        # Random mouse movements
        viewport = await page.viewport_size()
        if viewport:
            for _ in range(random.randint(1, 3)):
                x = random.randint(0, viewport['width'])
                y = random.randint(0, viewport['height'])
                await page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.3))
        
        # Random scrolling
        scroll_distance = random.randint(100, 500)
        await page.evaluate(f"window.scrollBy(0, {scroll_distance})")
        await asyncio.sleep(random.uniform(0.2, 0.5))
        
        # Scroll back
        await page.evaluate(f"window.scrollBy(0, -{scroll_distance})")
        await asyncio.sleep(random.uniform(0.1, 0.3))
        
    except Exception as e:
        logging.debug(f"Human behavior simulation failed: {e}")

async def check_for_detection(page) -> bool:
    """Checks if the page shows signs of bot detection."""
    try:
        # Check for common anti-bot messages
        content = await page.content()
        detection_indicators = [
            "access denied",
            "blocked",
            "captcha",
            "security check",
            "suspicious activity",
            "automated requests",
            "bot detected",
            "rate limit",
            "too many requests",
            "cloudflare",
            "please verify you are human"
        ]
        
        content_lower = content.lower()
        for indicator in detection_indicators:
            if indicator in content_lower:
                return False
        
        # Check for unusual redirects
        current_url = page.url
        if "challenge" in current_url or "verify" in current_url or "captcha" in current_url:
            return False
            
        return False
    except Exception:
        return False

# --- UTILITY & PARSING FUNCTIONS ---
def format_number(num_str):
    """Formats a number string, removing .0 from whole numbers."""
    if num_str is None: return None
    try:
        f = float(num_str)
        return str(int(f)) if f.is_integer() else num_str
    except (ValueError, TypeError): return num_str

def get_clean_text(element):
    """Extracts and cleans text from a BeautifulSoup element."""
    if not element: return None
    element_copy = BeautifulSoup(str(element), 'html.parser')
    for tag in element_copy.find_all(['i', 'img', 'span'], class_=['badge', 'tire_load_index', 'd-block', 'fa-li']):
        tag.decompose()
    return ' '.join(element_copy.get_text(strip=True).split())

def get_staggered_data(cell, is_imperial=False):
    """Parses a table cell to extract potentially staggered (front/rear) data."""
    target = cell
    if is_imperial and cell.find('span', class_='imperial'):
        target = cell.find('span', 'imperial')
    rear_data_span = target.find('span', class_='rear-tire-data')
    if rear_data_span:
        rear_value = get_clean_text(rear_data_span)
        front_target_copy = BeautifulSoup(str(target), 'html.parser')
        front_target_copy.find('span', class_='rear-tire-data').decompose()
        front_value = get_clean_text(front_target_copy)
        return front_value, rear_value
    html_string = str(target)
    parts = re.split(r'<br\s*/?>', html_string, maxsplit=1)
    if len(parts) == 2:
        return get_clean_text(parts[0]), get_clean_text(parts[1])
    return get_clean_text(target), get_clean_text(target)

def parse_vehicle_data(html_content):
    """Parses the HTML content to extract wheel and tire data for the USA market."""
    soup = BeautifulSoup(html_content, 'html.parser')
    h1 = soup.find('h1', id='title-header')
    if not h1: return []
    make, model, year = h1.get('data-make-name'), h1.get('data-model-name'), int(h1.get('data-year'))
    trims_list_div = soup.find('div', class_='trims-list')
    source_html = str(trims_list_div) if trims_list_div else ""
    results = []
    for panel in soup.find_all('div', class_='panel', id=lambda x: x and x.startswith('trim-')):
        if 'region-trim-usdm' not in panel.get('class', []): continue
        trim_info = {"html_output": source_html, "make": make, "model": model, "year": year, "tires": []}
        panel_hdr = panel.find('div', class_='panel-hdr')
        trim_name_span = panel_hdr.find('span', class_='panel-hdr-trim-name')
        trim_info['engine'] = trim_name_span['data-trim-name'] if trim_name_span and trim_name_span.get('data-trim-name') else get_clean_text(trim_name_span)
        power_span = panel_hdr.find(lambda tag: 'hp' in tag.text and tag.name == 'span')
        if power_span and (hp_match := re.search(r'(\d+)\s*hp', power_span.text)):
            trim_info['hp'] = int(hp_match.group(1))
        for item in panel.find_all('li', class_='element-parameter'):
            param_name_span = item.find('span', class_='parameter-name')
            if not param_name_span: continue
            param_name = get_clean_text(param_name_span).lower()
            if 'wheel tightening torque' in param_name:
                if torque_span := item.find('span', class_='imperial'):
                    trim_info['wheel_tightening_torque'] = get_clean_text(torque_span).lower().replace('lbf⋅ft', 'lbf ft')
                continue
            full_text = ' '.join(item.get_text(strip=True).split())
            if ':' in full_text:
                value = full_text.split(':', 1)[1].strip()
                if 'generation' in param_name: trim_info['generation'] = value
                elif 'production' in param_name: trim_info['production'] = value
                elif 'center bore' in param_name: trim_info['centerbore'] = value
                elif 'bolt pattern' in param_name: trim_info['bolt_pattern'] = value
                elif 'wheel fasteners' in param_name: trim_info['wheel_fasterns'] = value
                elif 'thread size' in param_name: trim_info['thread_size'] = value
        table = panel.find('table', class_='table-ws')
        if not table or not table.find('tbody'): continue
        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 6: continue
            tire_data = {'original_equipment': 'stock' in row.get('class', []), 'recommended_for_winter': bool(row.find('i', class_='fa-snowflake'))}
            tire_data['front_size'], tire_data['rear_size'] = get_staggered_data(cells[0])
            tire_data['front_rim'], tire_data['rear_rim'] = get_staggered_data(cells[1])
            tire_data['front_offset'], tire_data['rear_offset'] = get_staggered_data(cells[2])
            tire_data['front_backspacing'], tire_data['rear_backspacing'] = [format_number(v) for v in get_staggered_data(cells[3], is_imperial=True)]
            tire_data['front_tire_weight'], tire_data['rear_tire_weight'] = [format_number(v) for v in get_staggered_data(cells[4], is_imperial=True)]
            tire_data['front_max_psi'], tire_data['rear_max_psi'] = [format_number(v) for v in get_staggered_data(cells[5], is_imperial=True)]
            trim_info['tires'].append(tire_data)
        results.append(trim_info)
    return results

def save_vehicle_data(data, make, model, year):
    """Saves data to a nested directory and returns True on success."""
    make_dir_name = make.lower()
    model_file_name = model.lower().replace('/', '_') # Sanitize model name for filename
    year_dir_name = str(year)

    directory_path = RESULTS_DIR / make_dir_name / year_dir_name
    directory_path.mkdir(parents=True, exist_ok=True)
    
    filename = f"{make_dir_name}__{model_file_name}__{year_dir_name}.json"
    filepath = directory_path / filename
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logging.error(f"Failed to save data for {make}/{model}/{year}: {e}")
        return False

# --- ASYNCHRONOUS SCRAPING LOGIC ---
async def get_models_for_make_year(page, make, year):
    """Fetches the list of models for a given make and year using Playwright."""
    logging.info(f"Discovering models for {make.upper()} {year}...")
    
    # Navigate with human-like behavior
    await page.goto("https://www.wheel-size.com", wait_until='domcontentloaded')
    await human_like_delay(1.0, 2.0)
    
    # Check for detection
    if await check_for_detection(page):
        logging.warning(f"Detection detected on main page for {make} {year}")
        raise Exception("Bot detection detected")
    
    # Simulate human behavior
    await simulate_human_behavior(page)
    
    await page.wait_for_selector('#vehicle_form')
    await human_like_delay(0.5, 1.0)
    
    # Select make with human-like interaction
    await page.select_option('select#auto_vendor', make)
    await human_like_delay(0.8, 1.5)
    
    await page.wait_for_selector('select#auto_year:not([disabled])')
    await page.select_option('select#auto_year', str(year))
    await human_like_delay(0.8, 1.5)
    
    model_dropdown = 'span[aria-labelledby="select2-auto_model-container"]'
    await page.wait_for_selector(f'select#auto_model:not([disabled])')
    
    # Simulate mouse hover before clicking
    await page.hover(model_dropdown)
    await human_like_delay(0.2, 0.5)
    
    await page.click(model_dropdown)
    await page.wait_for_selector('ul.select2-results__options')
    await human_like_delay(0.5, 1.0)
    
    model_texts = await page.eval_on_selector_all(
        'ul.select2-results__options li.select2-results__option--selectable',
        'nodes => nodes.map(node => node.textContent).filter(text => text !== "Model")'
    )
    models = [text.strip().lower().replace(' ', '-') for text in model_texts]
    logging.info(f"Found {len(models)} models for {make.upper()} {year}.")
    return models

async def scrape_vehicle_page(page, make, model, year):
    """Scrapes a single vehicle page with automatic retry and backoff logic."""
    url = f"https://www.wheel-size.com/size/{make}/{model}/{year}/"
    
    for attempt in range(MAX_RETRIES):
        try:
            # Human-like navigation
            await human_like_delay(0.5, 1.5)
            await page.goto(url, wait_until='networkidle')
            
            # Check for detection immediately after page load
            if await check_for_detection(page):
                logging.warning(f"Detection detected on {url}, attempt {attempt + 1}")
                if attempt + 1 == MAX_RETRIES:
                    raise Exception("Bot detection detected")
                delay = INITIAL_BACKOFF_DELAY_SECONDS * (2 ** attempt)
                await asyncio.sleep(delay)
                continue
            
            # Simulate human behavior
            await simulate_human_behavior(page)
            
            await page.wait_for_selector('.trims-list .panel', timeout=20000)
            await human_like_delay(0.5, 1.0)
            
            html_content = await page.content()
            data = parse_vehicle_data(html_content)
            
            if data:
                if save_vehicle_data(data, make, model, year):
                    logging.info(f"✓ Scraped and saved {make}/{model}/{year}")
            else:
                logging.warning(f"✗ No US market data found for {make}/{model}/{year}")
            return # Success, exit the retry loop
            
        except PlaywrightTimeoutError:
            if attempt + 1 == MAX_RETRIES:
                logging.error(f"✗ Final attempt failed for {url}. Giving up.")
                break
            delay = INITIAL_BACKOFF_DELAY_SECONDS * (2 ** attempt)
            logging.warning(f"Timeout for {url}. Attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
        except Exception as e:
            if "detection" in str(e).lower():
                if attempt + 1 == MAX_RETRIES:
                    logging.error(f"✗ Bot detection persisted for {url}. Giving up.")
                    break
                delay = INITIAL_BACKOFF_DELAY_SECONDS * (2 ** attempt)
                logging.warning(f"Bot detection for {url}. Attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
            else:
                logging.error(f"An unexpected error occurred for {url}: {e}", exc_info=True)
                break # Don't retry on unexpected errors
    
    logging.error(f"✗ Failed to scrape {url} after all attempts.")

async def process_make_year(task_queue, pbar):
    """A worker that pulls tasks from a queue and processes them."""
    while not task_queue.empty():
        make, year = await task_queue.get()
        playwright_instance = None
        browser_instance = None
        page = None
        
        try:
            playwright_instance = await async_playwright().start()
            
            # Launch browser with anti-detection settings
            browser_instance = await playwright_instance.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--single-process',
                    '--disable-gpu',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-features=TranslateUI',
                    '--disable-ipc-flooding-protection',
                    '--disable-hang-monitor',
                    '--disable-client-side-phishing-detection',
                    '--disable-popup-blocking',
                    '--disable-default-apps',
                    '--disable-extensions',
                    '--disable-component-extensions-with-background-pages',
                    '--disable-background-networking',
                    '--no-default-browser-check',
                    '--autoplay-policy=user-gesture-required',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-field-trial-config',
                    '--disable-back-forward-cache',
                    '--disable-backing-store-limit',
                    '--disable-blink-features=AutomationControlled',
                    '--excludes-switches=enable-automation',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            page = await setup_stealth_page(browser_instance)
            
            # Add random delay before starting
            await human_like_delay(1.0, 3.0)
            
            models = await get_models_for_make_year(page, make, year)
            
            for model in models:
                make_dir_name = make.lower()
                model_file_name = model.lower().replace('/', '_')
                year_dir_name = str(year)
                filename = f"{make_dir_name}__{model_file_name}__{year_dir_name}.json"
                filepath = RESULTS_DIR / make_dir_name / year_dir_name / filename
                
                if filepath.exists():
                    logging.info(f"⏭️  Skipping existing file: {filepath.relative_to(RESULTS_DIR)}")
                    continue
                
                await scrape_vehicle_page(page, make, model, year)
                
                # Variable delay between requests
                await human_like_delay(1.0, 3.0)

            # Mark this make/year as complete
            done_path = RESULTS_DIR / make / str(year) / DONE_MARKER
            done_path.touch()
            logging.info(f"🏁 Marked {make.upper()} {year} as complete.")

        except PlaywrightTimeoutError:
            logging.error(f"Critical timeout discovering models for {make} {year}. Skipping.")
        except Exception as e:
            logging.critical(f"A worker failed processing {make} {year}: {e}", exc_info=True)
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass
            if browser_instance:
                try:
                    await browser_instance.close()
                except:
                    pass
            if playwright_instance:
                try:
                    await playwright_instance.stop()
                except:
                    pass
            task_queue.task_done()
            pbar.update(1)

async def main():
    """Main function to orchestrate the entire concurrent scraping process."""
    parser = argparse.ArgumentParser(description="Production-grade web scraper for wheel-size.com with anti-detection.")
    parser.add_argument(
        '-w', '--workers', type=int, default=4,
        help='Number of concurrent browser workers to run.'
    )
    args = parser.parse_args()
    
    setup_logging()
    logging.info(f"--- Starting Enhanced Anti-Detection Scraper with {args.workers} workers ---")
    
    # Generate all potential tasks
    all_tasks = [(make, year) for make in TARGET_MAKES for year in reversed(TARGET_YEARS)]
    
    # Filter out tasks that are already completed by checking for the .done marker
    tasks_to_do = []
    for make, year in all_tasks:
        done_path = RESULTS_DIR / make / str(year) / DONE_MARKER
        if not done_path.exists():
            tasks_to_do.append((make, year))

    logging.info(f"Found {len(all_tasks)} total potential tasks.")
    logging.info(f"Skipping {len(all_tasks) - len(tasks_to_do)} already completed tasks.")
    
    if not tasks_to_do:
        logging.info("All tasks are already complete. Exiting.")
        return
        
    logging.info(f"Queuing {len(tasks_to_do)} tasks to be processed.")

    # Create and populate the queue
    task_queue = asyncio.Queue()
    for task in tasks_to_do:
        await task_queue.put(task)

    # Start the progress bar and create worker tasks
    pbar = tqdm(total=len(tasks_to_do), desc="Processing Make/Year combinations")
    workers = [
        asyncio.create_task(process_make_year(task_queue, pbar))
        for _ in range(args.workers)
    ]

    # Wait for the queue to be fully processed
    await task_queue.join()

    # Clean up and close
    for worker in workers:
        worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    pbar.close()

    logging.info("--- Enhanced Anti-Detection Scraper Finished All Queued Tasks ---")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("\n--- Scraper Interrupted by User ---")