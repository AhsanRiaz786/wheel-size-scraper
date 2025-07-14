import time
from playwright.sync_api import sync_playwright

def scrape_bmw_m3_page():
    """
    Scrape the BMW M3 2002 wheel size page using Playwright sync API
    """
    url = "https://www.wheel-size.com/size/bmw/m3/2002/"
    
    with sync_playwright() as p:
        # Launch browser in headed mode (visible)
        browser = p.chromium.launch(headless=False)
        
        # Create a new page
        page = browser.new_page()
        
        print(f"Opening URL: {url}")
        
        # Navigate to the page
        page.goto(url)
        
        print("Page loaded, waiting 10 seconds...")
        
        # Wait for 10 seconds
        time.sleep(10)
        
        # Get the HTML content
        html_content = page.content()
        
        # Save to HTML file
        output_file = "bmw_m3_2002_wheel_size.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML content saved to: {output_file}")
        
        # Close the browser
        browser.close()

if __name__ == "__main__":
    scrape_bmw_m3_page() 