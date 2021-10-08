import urllib.parse as p
import time
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
# from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from random import random
# from msedge.selenium_tools import Edge, EdgeOptions

def get_random_sec():
    """
    get random time range from 4 sec to 8 sec
    """
    return random() * 4 + 4

def get_list_page(browser, param_dict, page):
    url = "http://app1.nmpa.gov.cn/datasearchcnda/face3/search.jsp?tableId={}&State=1&bcId={}&State=1&curstart={}" \
          "&State=1&tableName={}&State=1&viewtitleName={}&State=1&viewsubTitleName={}&State=1&tableView={}" \
          "&State=1&cid=0&State=1&ytableId=0&State=1&searchType=search&State=1".format(
        param_dict["table_id"],
        param_dict["bc_id"],
        page,
        param_dict["table_name"],
        param_dict["view_title_name"],
        param_dict["view_subtitle_name"],
        p.quote(param_dict["title"])
    )
    try:
        browser.get(url)
        print(page)
    except:
        print("超时")

    time.sleep(get_random_sec())
    return browser


def main_browser(param_dict):
    # firefox driver
    # browser = webdriver.Firefox(executable_path="geckodriver.exe")
    # edge driver
    # options = EdgeOptions()
    # options.use_chromium = True
    # browser = Edge(options=options)

    # chrome driver
    browser = webdriver.Chrome(executable_path="chromedriver.exe")

    # remove webdriver navigator flag to avoid anti-crawler check
    browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
        Object.defineProperty(navigator, 'webdriver', {
          get: () => undefined
        })
      """
    })

    url = "https://www.nmpa.gov.cn/yaopin/index.html"
    browser.get(url)
    time.sleep(get_random_sec())

    # action = ActionChains(browser)
    # click_name = browser.find_element_by_link_text('药品')
    # action.move_to_element(click_name)
    # time.sleep(get_random_sec())
    # click_name.click()
    # time.sleep(get_random_sec())

    click_name = browser.find_element_by_link_text(param_dict["title"])
    click_name.click()
    time.sleep(get_random_sec())

    for page in range(param_dict["start_num"], param_dict["end_num"]):
        browser = get_list_page(browser, param_dict, page)
        with open("{}{}.txt".format(param_dict["title"], page), 'w+', encoding="utf-8") as f:
            f.write(browser.page_source)

        with open("page.txt", 'w+', encoding='utf-8') as f:
            f.write(str([page + 1, param_dict["end_num"]]))


if __name__ == "__main__":
    param_dict = {
        "start_num": 1,
        "end_num": 11002,
        "table_id": "25",
        "bc_id": "152904713761213296322795806604",
        "table_name": "TABLE25",
        "view_title_name": "COLUMN167",
        "view_subtitle_name": "COLUMN821,COLUMN170,COLUMN166",
        "title": "国产药品"
    }

    main_browser(param_dict)
