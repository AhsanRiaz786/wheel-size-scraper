import json
import re
import time
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def format_number(num_str):
    """
    Formats a number string to remove trailing .0 if it's a whole number.
    e.g., "25.0" -> "25", but "22.4" -> "22.4"
    """
    if num_str is None:
        return None
    try:
        f = float(num_str)
        if f.is_integer():
            return str(int(f))
        return num_str
    except (ValueError, TypeError):
        return num_str

def get_clean_text(element):
    """
    Extracts and cleans text from a BeautifulSoup element.
    Removes unwanted nested tags to get pure text content.
    """
    if not element:
        return None
    element_copy = BeautifulSoup(str(element), 'html.parser')
    for tag in element_copy.find_all(['i', 'img', 'span'], class_=['badge', 'tire_load_index', 'd-block', 'fa-li']):
        tag.decompose()
    return ' '.join(element_copy.get_text(strip=True).split())

def get_staggered_data(cell, is_imperial=False):
    """
    Parses a table cell (<td>) to extract potentially staggered (front/rear) data.
    """
    target = cell
    if is_imperial and cell.find('span', class_='imperial'):
        target = cell.find('span', class_='imperial')

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
        front_value = get_clean_text(parts[0])
        rear_value = get_clean_text(parts[1])
        return front_value, rear_value

    value = get_clean_text(target)
    return value, value

def parse_vehicle_data(html_content):
    """
    Parses the HTML content of a vehicle page to extract wheel and tire data.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    h1 = soup.find('h1', id='title-header')
    if not h1:
        print("Could not find the main header. The page structure may have changed.")
        return []
        
    make = h1.get('data-make-name')
    model = h1.get('data-model-name')
    year = int(h1.get('data-year'))

    results = []
    trim_panels = soup.find_all('div', class_='panel', id=lambda x: x and x.startswith('trim-'))

    for panel in trim_panels:
        trim_info = { "make": make, "model": model, "year": year, "tires": [] }
        panel_hdr = panel.find('div', class_='panel-hdr')
        
        # --- FIXED ENGINE/TRIM NAME EXTRACTION ---
        trim_name_span = panel_hdr.find('span', class_='panel-hdr-trim-name')
        if trim_name_span and trim_name_span.get('data-trim-name'):
            # This is the most reliable source for the engine/trim name.
            trim_info['engine'] = trim_name_span['data-trim-name']
        else:
            # Fallback to parsing text content if the attribute is missing.
            trim_info['engine'] = get_clean_text(trim_name_span)
        # --- END OF FIX ---

        power_span = panel_hdr.find(lambda tag: 'hp' in tag.text and tag.name == 'span')
        if power_span:
            hp_match = re.search(r'(\d+)\s*hp', power_span.text)
            if hp_match:
                trim_info['hp'] = int(hp_match.group(1))

        param_lists = panel.find_all('ul', class_='parameter-list')
        for param_list in param_lists:
            for item in param_list.find_all('li', class_='element-parameter'):
                param_name_span = item.find('span', class_='parameter-name')
                if not param_name_span: continue
                param_name = get_clean_text(param_name_span).lower()
                
                if 'wheel tightening torque' in param_name:
                    torque_span = item.find('span', class_='imperial')
                    if torque_span:
                        torque_value = get_clean_text(torque_span).lower().replace('lbf⋅ft', 'lbf ft')
                        trim_info['wheel_tightening_torque'] = torque_value
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

            tire_data = {
                'original_equipment': 'stock' in row.get('class', []),
                'recommended_for_winter': bool(row.find('i', class_='fa-snowflake'))
            }
            
            tire_data['front_size'], tire_data['rear_size'] = get_staggered_data(cells[0])
            tire_data['front_rim'], tire_data['rear_rim'] = get_staggered_data(cells[1])
            tire_data['front_offset'], tire_data['rear_offset'] = get_staggered_data(cells[2])
            front_bs, rear_bs = get_staggered_data(cells[3], is_imperial=True)
            tire_data['front_backspacing'] = format_number(front_bs)
            tire_data['rear_backspacing'] = format_number(rear_bs)
            front_weight, rear_weight = get_staggered_data(cells[4], is_imperial=True)
            tire_data['front_tire_weight'] = format_number(front_weight)
            tire_data['rear_tire_weight'] = format_number(rear_weight)
            front_psi, rear_psi = get_staggered_data(cells[5], is_imperial=True)
            tire_data['front_max_psi'] = format_number(front_psi)
            tire_data['rear_max_psi'] = format_number(rear_psi)
            
            trim_info['tires'].append(tire_data)
        
        results.append(trim_info)
    return results

def scrape_and_parse():
    """
    Launches a browser with anti-scraping measures, navigates to the URL, 
    waits for dynamic content, scrapes it, and then parses the data.
    """
    url = "https://www.wheel-size.com/size/audi/a4/2024/"
    html_content = None

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,  # Set to False to see browser actions, True for production
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )
        page = browser.new_page(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page.set_extra_http_headers({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        try:
            print(f"Opening URL: {url}")
            page.goto(url, wait_until='networkidle', timeout=60000)
            print("Page loaded, waiting for dynamic data tables...")
            page.wait_for_selector('.trims-list .panel', timeout=30000)
            time.sleep(3)
            print("Dynamic content loaded. Extracting HTML...")
            html_content = page.content()

        except Exception as e:
            print(f"An error occurred during scraping: {e}")
        finally:
            print("Closing browser.")
            browser.close()

    if html_content:
        print("Parsing data...")
        data = parse_vehicle_data(html_content)
        if data:
            print("\n--- SCRAPED DATA ---\n")
            print(json.dumps(data, indent=2))
        else:
            print("Parsing failed. No data was extracted.")
    else:
        print("Could not retrieve HTML content to parse.")

if __name__ == "__main__":
    scrape_and_parse()