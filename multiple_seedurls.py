from urllib.parse import urljoin
import time
import random
import logging
from playwright.sync_api import sync_playwright, TimeoutError, Error as PageError
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import re
import threading

from database import Database

# Configure logging
logging.basicConfig(
    level=logging.INFO,     
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',   
    handlers=[
        logging.FileHandler("playwright.log"),    
       
    ]
)
logger = logging.getLogger(__name__)     

# Initialize database connection
db_instance = Database(logger)
db_instance.create_connection()
url_ids = db_instance.fetch_url_ids()
print(url_ids)

# Initialize counters for each URL ID
counters = {url_id: 0 for url_id in url_ids}


def main(url_id):

    """
    Main function to extract article URLs from a given seed URL using Playwright.

    This function navigates to the provided seed URL, identifies anchor tags 
    with child URLs, and extracts URLs that match specific patterns. It then 
    initiates a multithreading process to parse these URLs concurrently. The 
    function uses the Playwright library to interact with the web page and a 
    database instance to manage extracted data.

    Args:
        url_id (int): The identifier for the URL configuration in the database.

    Returns:
        None

    Raises:
        TimeoutError: If navigating to the seed URL times out.
        PageError: If an error occurs with Playwright during navigation.
    """

    db_instance = Database(logger)
    db_instance.create_connection()
    
    # Fetch configuration record from the database based on URL ID
    config_record = db_instance.fetch_record_by_url_id(url_id)
    if config_record:
        seed_url = config_record[1]        
        max_threads = config_record[2]
        maximum_urls = config_record[3]
        count = config_record[4]
        child_url_xpath = config_record[5]
        article_title_xpth = config_record[6]
        article_content_xpth = config_record[7]
        seed_url_re_str = config_record[8]
        child_url_re_str = config_record[9]
        delay = config_record[10]

    article_set = set()  

    # Use Playwright to navigate to the seed URL and extract child URLs
    with sync_playwright() as p:
       
        browser = p.chromium.launch(headless=False)

        context = browser.new_context()

        
        page = context.new_page()

        try:
            
            page.goto(seed_url, timeout=60000)
            logger.info(f"Navigated to seed url {seed_url}")

            
            page.wait_for_load_state('domcontentloaded')
            logger.info(f"loaded the url {seed_url}")
        
            # Compile regular expressions for URL matching
            child_re_str = re.compile(child_url_re_str)
            seed_re_str = re.compile(seed_url_re_str)

            # Locate and process anchor tags
            anchor_tags = page.locator(child_url_xpath)
            anchor_elements = anchor_tags.element_handles()   
            

            for anchor in anchor_elements:
               
                href = anchor.get_attribute('href')
                if href and seed_re_str.match(href):
                    
                    article_set.add(href)
                   
                if href and child_re_str.match(href):
                    
                    full_url = urljoin(seed_url, href)  
                    article_set.add(full_url)
                   
            if article_set:
                logger.info(f'Getting the child urls  from the seed url {seed_url}')
            else:
                logger.info(f'No child urls  from the seed url {seed_url}')

        except TimeoutError:
            logger.error(f"Timeout error occurred while navigating to seed url {seed_url}")
        
        except PageError as e:
            logger.error(f"An error occurred with Playwright for seed_url {seed_url}: {e.name} and {e.message}")
        
        finally:
            page.close()
            context.close()
            browser.close()
            logger.info(f"Browser closed for seed url {seed_url}")

    # Synchronize access to shared resources
    with lock:
        global counters
        counters[url_id] = count
        print('starting count : ', counters[url_id])
        logger.info(f'initialized starting count is {counters[url_id]}')

        max_urls = maximum_urls
        print('maximum number of urls : ', max_urls)
        logger.info(f'initialized maximum number of urls are {max_urls}')

        global max_workers
        max_workers = max_threads
        print('initialized max_workers for mutlithreading : ', max_workers)
        logger.info(f'initialized max_workers for mutlithreading are {max_workers}')

        depth_0_urls_list = []
        
        if len(article_set) > 0:
            for child_url in article_set:
                depth_0_urls_list.append(child_url)

        visited_child_urls_set = article_set
        
        delay = delay      
        time.sleep(60 + random.uniform(5, 15))

        print(f'total child urls from seed url: {seed_url} ', len(depth_0_urls_list))
        logger.info(f"Found {len(article_set)} child urls from the seed url {depth_0_urls_list}")
       
    if len(depth_0_urls_list) > 0 and counters[url_id] < max_urls: 
        logger.info('Multithreading is started')
        create_and_start_threads(depth_0_urls_list, delay, max_workers, article_title_xpth, article_content_xpth, url_id, max_urls, visited_child_urls_set)
    
    if len(depth_0_urls_list)>0 and counters[url_id] < max_urls:
        child_urls(depth_0_urls_list, delay, child_url_xpath,  article_title_xpth, article_content_xpth, seed_url_re_str, child_url_re_str, url_id, max_urls, visited_child_urls_set)

# Create a global lock to synchronize access to shared resources
lock = threading.Lock()


def get_child_urls(url, delay, child_url_xpath, article_title_xpth, article_content_xpth, seed_url_re_str, child_url_re_str, url_id, visited_child_urls_set):

    """
    Extracts article URLs from the given seed URL using Playwright.

    This function navigates to the provided seed URL, identifies anchor tags 
    with child URLs, and extracts URLs that match specific patterns. It uses 
    the Playwright library to interact with the web page.

    Args:
        url (str): The seed URL from which to extract article URLs.
        delay (int): Delay between requests in seconds.
        child_url_xpath (str): XPath to locate child URLs in the web page.
        article_title_xpth (str): XPath to locate the article title in the web page.
        article_content_xpth (str): XPath to locate the article content in the web page.
        seed_url_re_str (str): Regular expression string to match seed URLs.
        child_url_re_str (str): Regular expression string to match child URLs.
        url_id (int): Identifier for the URL configuration in the database.
        visited_child_urls_set (set): Set of already visited child URLs to avoid duplicates.

    Returns:
        list: A list of extracted article URLs.

    Raises:
        TimeoutError: If navigating to the seed URL times out.
        PageError: If an error occurs with Playwright during navigation.
    """

    with lock:

        article_set = set()  
        seed_url = url       
    
        with sync_playwright() as p:
            
            browser = p.chromium.launch(headless=False)

            context = browser.new_context()

            page = context.new_page()

            try:
                
                page.goto(seed_url, timeout=60000)
                logger.info(f"Navigated to seed url {seed_url}")
    
                page.wait_for_load_state('domcontentloaded')
                logger.info(f"loaded the url {seed_url}")
            
                # Compile regular expressions for URL matching
                child_re_str = re.compile(child_url_re_str)
                seed_re_str= re.compile(seed_url_re_str)
                
                # Locate and process anchor tags
                anchor_tags = page.locator(child_url_xpath)
                anchor_elements = anchor_tags.element_handles()   
                
                for anchor in anchor_elements:
                    
                    href = anchor.get_attribute('href')
                    if href and seed_re_str.match(href):
                        
                        article_set.add(href)
                
                    if href and child_re_str.match(href):
                       
                        full_url = urljoin(seed_url, href)  
                        article_set.add(full_url)
                        
                if article_set:
                    logger.info(f'Getting the child urls  from the seed url {seed_url}')
                else:
                    logger.info(f'No child urls  from the seed url {seed_url}')

            except TimeoutError:
                logger.error(f"Timeout error occurred while navigating to seed url {seed_url}")

            except PageError as e:
                logger.error(f"An error occurred with Playwright for seed_url {seed_url}: {e.name} and {e.message}")
            
            finally:
                page.close()
                context.close()
                browser.close()
                logger.info(f"Browser closed for seed url {seed_url}")

        delay = delay
        # Process and filter child URLs
        depth_urls = []
        child_urls_list = list(article_set)
        print('length :', len(child_urls_list))

        child_urls= []
        if len(child_urls_list) > 0:
            for child_url in child_urls_list:
                print('==========first mutithread=======')
                if child_url not in visited_child_urls_set:
                        visited_child_urls_set.add(child_url)
                        child_urls.append(child_url)
                        depth_urls.append(child_url)
                
            if len(child_urls)>0:
                print('============== length of child urls : ', len(child_urls))
                print('======== length of depth urls ===== ', len(depth_urls))
                create_and_start_threads(child_urls, delay, article_title_xpth, article_content_xpth, url_id)

        time.sleep(30)
        return depth_urls


def parse_url(url, article_title_xpth, article_content_xpth, url_id, max_urls, visited_child_urls_set):

    """
    Parses the given URL to extract article information and child URLs.

    This function navigates to the provided URL, extracts the article title and content,
    identifies child URLs, and stores the extracted information in a database. It
    uses the Playwright library for web scraping and threading for concurrent processing.

    Args:
        url (str): The URL to parse.
        article_title_xpth (str): XPath to locate the article title.
        article_content_xpth (str): XPath to locate the article content.
        url_id (int): Identifier for the URL configuration in the database.
        max_urls (int): Maximum number of URLs to be processed.
        visited_child_urls_set (set): Set of already visited child URLs to avoid duplicates.

    Returns:
        None
    """
     
    article_details = []     

    if counters[url_id] >= max_urls+1:
        return
    
    # Set up the Playwright environment
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            current_datetime = datetime.now()
            formatted_datetime = current_datetime.strftime('%Y:%m:%d %H:%M:%S')

            page.goto(url, timeout=60000)
            logger.info(f"Navigated to url {url}")

            page.wait_for_load_state('domcontentloaded', timeout=60000)
            logger.info(f"loaded the url {url}")

            title_tag = page.locator(article_title_xpth)

            if title_tag:
                title = title_tag.text_content()
                logger.info(f'article title from the url {url}')
            else:
                logger.warning(f"No title found for {url}")
                title = 'No title found'

            article_details.append(url)
            print(url)
            article_details.append(title)
            
            content = ''
            art_content = page.locator(article_content_xpth)
            article_content = art_content.all_text_contents()
            content = " ".join(article_content)
            if content != '':
                logger.info(f'article content from the url {url}')
            else:
                logger.info(f'No article content from the url {url}')

            article_details.append(content)
            article_details.append(formatted_datetime)
        
        except TimeoutError:
           
            logger.error(f"Timeout error occurred while navigating to url {url}")

        except PageError as e:
            
            logger.error(f"An error occurred with Playwright for url {url}: {e.name} and {e.message}")
        finally:
            page.close()
            context.close()
            browser.close()
            logger.info(f"Browser closed for url {url}")

        time.sleep(2)

    print('lenth of visited set before : ', len(visited_child_urls_set))
    print('article details : ', article_details)

    if article_details:
            
            if len(article_details)>3 and article_details[1] != 'No title found' and article_details[2] != '' and not article_details[2].isspace():
                if '\n' in article_details[2]:
                    article_details[2]=article_details[2].replace('\n', '')

                if db_instance.store_db(article_details):
                    print(f"Counter: {counters[url_id]}")
                    counters[url_id] += 1
        
            if counters[url_id] >= max_urls+1: 
                print("Threshold reached!")
                

def create_and_start_threads(child_urls_list, delay, max_workers, article_title_xpth, article_content_xpth, url_id, max_urls, visited_child_urls_set):

    """
    Creates and starts threads to parse URLs concurrently using ThreadPoolExecutor.

    This function initiates a pool of threads to process URLs from the provided `child_urls_list`.
    It stops creating new threads if the maximum URL limit is reached and logs the status.

    Args:
        child_urls_list (list): List of child URLs to be processed.
        delay (int): Delay in seconds between processing each URL.
        max_workers (int): Maximum number of worker threads.
        article_title_xpth (str): XPath to locate the article title.
        article_content_xpth (str): XPath to locate the article content.
        url_id (int): Identifier for the URL configuration in the database.
        max_urls (int): Maximum number of URLs to be processed.
        visited_child_urls_set (set): Set of already visited child URLs to avoid duplicates.

    Returns:
        None
    """
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []    

        for url in child_urls_list:
            if counters[url_id] < max_urls+1:
                time.sleep(delay)
                futures.append(executor.submit(parse_url, url, article_title_xpth, article_content_xpth, url_id, max_urls, visited_child_urls_set))
            else:
                executor.shutdown(wait=False, cancel_futures=True)
                logger.info('Multithreading is stopped')  
                break


def child_urls(child_urls_list, delay, child_url_xpath, article_title_xpth, article_content_xpth, seed_url_re_str, child_url_re_str, url_id, max_urls, visited_child_urls_set):
    """
    Recursively extracts and processes child URLs using multithreading.
    
    This function takes a list of initial child URLs and uses a thread pool to process each URL concurrently. It extracts further child URLs from each processed URL, adding them to a depth list for further processing. The function runs recursively until the maximum URL limit is reached or there are no more child URLs to process.
    
    Args:
        child_urls_list (list): List of initial child URLs to process.
        delay (int): Delay in seconds between processing each URL.
        child_url_xpath (str): XPath to locate child URLs within a page.
        article_title_xpth (str): XPath to locate the article title.
        article_content_xpth (str): XPath to locate the article content.
        seed_url_re_str (str): Regular expression for validating seed URLs.
        child_url_re_str (str): Regular expression for validating child URLs.
        url_id (int): Identifier for the URL configuration in the database.
        max_urls (int): Maximum number of URLs to process.
        visited_child_urls_set (set): Set of already visited child URLs to avoid duplicates.
    """
        
    depth_urls = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for url in child_urls_list:
            if counters[url_id] < max_urls+1:
                time.sleep(delay)
                futures.append(executor.submit(get_child_urls, url, delay, child_url_xpath, article_title_xpth, article_content_xpth, seed_url_re_str, child_url_re_str, url_id, visited_child_urls_set))
            else:
                executor.shutdown(wait=False, cancel_futures=True)
                logger.info('Multithreading is stopped')  
                break

        for future in as_completed(futures):
            try:
                values = future.result()
                depth_urls += values
                print('-------------------------------------------------')
            except Exception as e:
                print(f"Error storing data: {e}")


    print('length of depth urls  ---->>>>> ', len(depth_urls))
    if counters[url_id] < max_urls+1:
        child_urls(depth_urls, delay, child_url_xpath, article_title_xpth, article_content_xpth, seed_url_re_str, child_url_re_str, url_id, max_urls, visited_child_urls_set)
    

with ThreadPoolExecutor(max_workers=2) as executor:
    executor.map(main, url_ids)

db_instance.close_database()