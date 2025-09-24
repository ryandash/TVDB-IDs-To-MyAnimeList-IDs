# get_total_pages.py
from playwright.sync_api import sync_playwright
import re
import os

BASE_URL = "https://www.thetvdb.com/genres/anime?page=1"

def get_total_pages():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(BASE_URL, timeout=30000)  # 30s timeout
            page.wait_for_selector('xpath=//*[@id="app"]/div[3]/div[3]/div[1]/ul', timeout=10000)

            li_elements = page.query_selector_all(
                'xpath=//*[@id="app"]/div[3]/div[3]/div[1]/ul/li'
            )

            if len(li_elements) < 2:
                total = 1
            else:
                last_li = li_elements[-2]
                a_tag = last_li.query_selector("a")
                href = a_tag.get_attribute("href")
                total = int(re.search(r'page=(\d+)', href).group(1))
        finally:
            browser.close()

    # Output for GitHub Actions
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
            print(f"total={total}", file=fh)
    return total

if __name__ == "__main__":
    total_pages = get_total_pages()
    print(f"Total pages: {total_pages}")
