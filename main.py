import json
import re
import time
import os
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
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
    Parses the HTML content to extract wheel and tire data only for the USA market.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    h1 = soup.find('h1', id='title-header')
    if not h1:
        print("Could not find the main header. The page structure may have changed.")
        return []
        
    make = h1.get('data-make-name')
    model = h1.get('data-model-name')
    year = int(h1.get('data-year'))

    # Get the source HTML for the entire modifications block
    trims_list_div = soup.find('div', class_='trims-list')
    source_html = str(trims_list_div) if trims_list_div else ""

    results = []
    trim_panels = soup.find_all('div', class_='panel', id=lambda x: x and x.startswith('trim-'))

    for panel in trim_panels:
        # Filter to only include panels for the USA market (USDM)
        panel_classes = panel.get('class', [])
        if 'region-trim-usdm' not in panel_classes:
            continue # Skip this panel if it's not for the US market

        trim_info = { 
            "html_output": source_html,
            "make": make, 
            "model": model, 
            "year": year, 
            "tires": [] 
        }

        panel_hdr = panel.find('div', class_='panel-hdr')
        trim_name_span = panel_hdr.find('span', class_='panel-hdr-trim-name')
        if trim_name_span and trim_name_span.get('data-trim-name'):
            trim_info['engine'] = trim_name_span['data-trim-name']
        else:
            trim_info['engine'] = get_clean_text(trim_name_span)

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

# Target car makes to scrape
TARGET_MAKES = [
    'acura', 'alfa-romeo', 'aston-martin', 'audi', 'bentley', 'bmw', 'bugatti', 'buick',
    'cadillac', 'chevrolet', 'chrysler', 'dodge', 'ferrari', 'ford', 'genesis', 
    'gmc', 'honda', 'hummer', 'hyundai', 'infiniti', 'jaguar', 'jeep', 'kia',
    'lamborghini', 'land-rover', 'lexus', 'lincoln', 'lotus', 'maserati', 'maybach', 'mazda', 
    'mclaren', 'mercedes-benz', 'mercury', 'mini', 'mitsubishi', 'nissan', 'oldsmobile',
    'pontiac', 'porsche', 'ram', 'rolls-royce', 'saab', 'saturn', 'scion', 'subaru',
    'suzuki', 'tesla', 'toyota', 'volkswagen', 'volvo'
]

# Years to scrape
TARGET_YEARS = list(range(2000, 2026))  # 2000-2025

def get_models_for_make_year(page, make, year):
    """
    Navigate to wheel-size.com, select make and year, and get available models.
    This version correctly interacts with the Select2 JavaScript widgets.
    """
    try:
        print(f"Getting models for {make} {year}")
        page.goto("https://www.wheel-size.com", wait_until='domcontentloaded', timeout=60000)
        
        page.wait_for_selector('#vehicle_form', timeout=30000)

        page.select_option('select#auto_vendor', make)
        
        page.wait_for_selector('select#auto_year:not([disabled])', timeout=15000)
        page.select_option('select#auto_year', str(year))

        model_dropdown_selector = 'span[aria-labelledby="select2-auto_model-container"]'
        page.wait_for_selector(f'select#auto_model:not([disabled])', timeout=15000)
        page.click(model_dropdown_selector)

        results_list_selector = 'ul.select2-results__options'
        page.wait_for_selector(results_list_selector, timeout=10000)

        model_texts = page.eval_on_selector_all(
            'ul.select2-results__options li.select2-results__option--selectable',
            'nodes => nodes.map(node => node.textContent).filter(text => text !== "Model")'
        )
        
        models = [text.strip().lower().replace(' ', '-') for text in model_texts]

        if models:
            print(f"Found {len(models)} models for {make} {year}")
        else:
            print(f"No models found for {make} {year} (this may be expected).")
            
        return models
        
    except PlaywrightTimeoutError:
        print(f"Timed out waiting for models for {make} {year}. It's likely none exist for this combination.")
        return []
    except Exception as e:
        print(f"An error occurred getting models for {make} {year}: {e}")
        return []

def scrape_vehicle_data(page, make, model, year):
    """
    Scrape data for a specific make/model/year combination.
    Returns parsed data or None if failed.
    """
    try:
        url = f"https://www.wheel-size.com/size/{make}/{model}/{year}/"
        print(f"Scraping: {url}")
        
        page.goto(url, wait_until='networkidle', timeout=60000)
        page.wait_for_selector('.trims-list .panel', timeout=30000)
        
        html_content = page.content()
        data = parse_vehicle_data(html_content)
        
        if data:
            print(f"✓ Successfully scraped {make} {model} {year} - {len(data)} trim(s)")
            return data
        else:
            print(f"✗ No US market data found for {make} {model} {year}")
            return None
            
    except PlaywrightTimeoutError:
        print(f"✗ Timed out or no content found for {make} {model} {year}. The page might not exist.")
        return None
    except Exception as e:
        print(f"✗ Error scraping {make} {model} {year}: {e}")
        return None

# --- UPDATED FUNCTION ---
def save_vehicle_data(data, make, model, year):
    """
    Save vehicle data to a JSON file in a nested directory structure:
    results/make/year/make__model__year.json
    """
    if not data:
        return False
        
    try:
        # Create clean names for directories and the file
        make_dir_name = make.lower().replace('-', '_').replace(' ', '_')
        model_file_name = model.lower().replace('-', '_').replace(' ', '_')
        year_dir_name = str(year)
        
        # Define the nested directory path
        directory_path = os.path.join('results', make_dir_name, year_dir_name)
        
        # Create the nested directories if they don't exist
        os.makedirs(directory_path, exist_ok=True)
        
        # Create the final filename and the full path to it
        filename = f"{make_dir_name}__{model_file_name}__{year_dir_name}.json"
        filepath = os.path.join(directory_path, filename)
        
        # Save the data
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Saved to: {filepath}")
        return True
        
    except Exception as e:
        print(f"✗ Error saving data for {make} {model} {year}: {e}")
        return False

# --- UPDATED FUNCTION ---
def scrape_all_vehicles():
    """
    Main function to scrape all target makes, years, and models, saving them
    into a nested directory structure.
    """
    print("Starting comprehensive vehicle data scraping...")
    print(f"Target makes: {len(TARGET_MAKES)}")
    print(f"Target years: {TARGET_YEARS[0]}-{TARGET_YEARS[-1]}")
    
    total_scraped = 0
    total_saved = 0
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--disable-dev-shm-usage', '--no-sandbox']
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        page.set_extra_http_headers({
            'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive', 'Upgrade-Insecure-Requests': '1',
        })
        
        try:
            for make in TARGET_MAKES:
                print(f"\n{'='*60}")
                print(f"Processing make: {make.upper()}")
                print(f"{'='*60}")
                
                for year in reversed(TARGET_YEARS): # Scrape newest years first
                    print(f"\n--- {make.upper()} {year} ---")
                    
                    models = get_models_for_make_year(page, make, year)
                    
                    if not models:
                        print(f"No models found to scrape for {make} {year}")
                        continue
                    
                    for model in models:
                        # --- MODIFIED LOGIC TO CHECK FOR FILE IN NESTED DIRECTORY ---
                        make_dir_name = make.lower().replace('-', '_').replace(' ', '_')
                        model_file_name = model.lower().replace('-', '_').replace(' ', '_')
                        year_dir_name = str(year)
                        
                        filename = f"{make_dir_name}__{model_file_name}__{year_dir_name}.json"
                        directory_path = os.path.join('results', make_dir_name, year_dir_name)
                        filepath = os.path.join(directory_path, filename)
                        
                        if os.path.exists(filepath):
                            print(f"⏭️  Skipping {make} {model} {year} - file already exists")
                            continue
                        
                        data = scrape_vehicle_data(page, make, model, year)
                        total_scraped += 1
                        
                        if save_vehicle_data(data, make, model, year):
                            total_saved += 1
                        
                        time.sleep(1) # Small delay to be respectful
                
                print(f"\nCompleted {make.upper()}")
                
        except KeyboardInterrupt:
            print(f"\n\n⚠️  Scraping interrupted by user")
        except Exception as e:
            print(f"\n\n❌ Fatal error: {e}")
        finally:
            browser.close()
    
    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Total vehicle pages attempted: {total_scraped}")
    print(f"Total files saved: {total_saved}")
    print(f"Results saved in 'results/' directory")

def scrape_single_vehicle(make, model, year):
    """
    Scrape a single vehicle for testing purposes.
    """
    print(f"Scraping single vehicle: {make} {model} {year}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False) # Use headless=False for debugging
        context = browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        page = context.new_page()
        
        try:
            models = get_models_for_make_year(page, make, year)
            print(f"Available models for {make} {year}: {models}")
            if model in models:
                 data = scrape_vehicle_data(page, make, model, year)
                 if data:
                     save_vehicle_data(data, make, model, year)
                     print("\n--- SAMPLE OUTPUT ---")
                     print(json.dumps(data[0], indent=2))
            else:
                print(f"Model '{model}' not found in the list for {make} {year}.")

        finally:
            browser.close()

if __name__ == "__main__":
    import sys
    
    print("🚗 Wheel Size Data Scraper")
    print("=" * 50)
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        print("Running in TEST mode.")
        scrape_single_vehicle("ford", "mustang", 2024)
    else:
        print("Running in FULL mode. This will scrape all target vehicles.")
        print("This may take several hours. Press Ctrl+C to stop.")
        try:
            if input("Continue? (y/N): ").strip().lower() in ['y', 'yes']:
                scrape_all_vehicles()
            else:
                print("Scraping cancelled by user.")
        except KeyboardInterrupt:
            print("\nScraping cancelled by user.")