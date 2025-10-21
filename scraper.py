import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
from datetime import datetime
import random

# --- configuration ---
CONCURRENT_DOWNLOADS = 3  # reduced from 5 to be less aggressive
CHUNK_SIZE = 8192
TIMEOUT_SECONDS = 300
MAX_RETRIES = 3

# rate limiting
MIN_DELAY_BETWEEN_REQUESTS = 1.0  # min seconds between requests
MAX_DELAY_BETWEEN_REQUESTS = 3.0  # max seconds between requests
MIN_DELAY_BETWEEN_DOWNLOADS = 2.0  # min seconds between starting downloads
MAX_DELAY_BETWEEN_DOWNLOADS = 5.0  # max seconds between starting downloads

# user agent rotation - mix of common browsers
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
]

# common pagination selectors
PAGINATION_SELECTORS = {
    '1': {'name': 'Generic pagination (default)', 'selector': 'a[href*="page"], .pagination a, a[rel="next"]'},
    '2': {'name': 'Numbered page links', 'selector': 'a.page-link, a.page-number'},
    '3': {'name': 'Next button only', 'selector': 'a[rel="next"], a.next, .next-page'},
    '4': {'name': 'WordPress style', 'selector': '.nav-links a, .pagination a'},
    '5': {'name': 'Shopify/e-commerce', 'selector': '.pagination__item a, a[aria-label*="page"]'},
    '6': {'name': 'Bootstrap pagination', 'selector': '.pagination li a, ul.pagination a'},
}


def setup_logging(download_dir):
    """
    sets up logging to both console and file.
    """
    log_file = download_dir / f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


def get_random_headers():
    """
    returns headers with a random user agent to avoid detection.
    """
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }


async def random_delay(min_delay, max_delay):
    """
    adds a random delay to appear more human-like.
    """
    delay = random.uniform(min_delay, max_delay)
    await asyncio.sleep(delay)


async def get_best_download_link(session, video_page_url, logger):
    """
    visits a video page and finds the best quality h264 download link.
    """
    # random delay before request
    await random_delay(MIN_DELAY_BETWEEN_REQUESTS, MAX_DELAY_BETWEEN_REQUESTS)
    
    logger.info(f"finding link on: {video_page_url}")
    try:
        async with session.get(video_page_url, headers=get_random_headers()) as response:
            if response.status == 429:  # too many requests
                logger.warning(f"rate limited on {video_page_url}, backing off...")
                await asyncio.sleep(30)  # wait 30 seconds
                return None, None
            
            if response.status != 200:
                logger.error(f"failed to fetch {video_page_url} - status: {response.status}")
                return None, None

            soup = BeautifulSoup(await response.text(), "html.parser")
            
            highest_quality = 0
            best_link = None
            
            # find all h264 download links
            links = soup.select('div.dloaddivcol span.download-h264 a')

            for link in links:
                href = link.get('href')
                # extract quality from the url, e.g., /dload/.../720/...
                parts = href.split('/')
                if len(parts) > 3 and parts[3].isdigit():
                    quality = int(parts[3])
                    if quality > highest_quality:
                        highest_quality = quality
                        best_link = href
            
            if best_link:
                filename = best_link.split('/')[-1]
                logger.info(f"found {highest_quality}p link for {filename}")
                return best_link, filename

            logger.warning(f"no h264 download links found on {video_page_url}")
            return None, None

    except Exception as e:
        logger.error(f"error processing {video_page_url}: {e}")
        return None, None


async def download_file(session, base_url, url, filename, download_dir, semaphore, logger):
    """
    downloads a file with resume capability and retry logic.
    """
    # stagger download starts
    await random_delay(MIN_DELAY_BETWEEN_DOWNLOADS, MAX_DELAY_BETWEEN_DOWNLOADS)
    
    async with semaphore:
        filepath = download_dir / filename
        temp_filepath = download_dir / f"{filename}.part"
        
        # check if file is already complete
        if filepath.exists():
            logger.info(f"skipping, already exists: {filename}")
            return True
        
        # check for partial download
        resume_from = 0
        if temp_filepath.exists():
            resume_from = temp_filepath.stat().st_size
            logger.info(f"resuming {filename} from {resume_from} bytes")
        
        full_url = urljoin(base_url, url)
        
        for attempt in range(MAX_RETRIES):
            try:
                headers = get_random_headers()
                if resume_from > 0:
                    headers['Range'] = f'bytes={resume_from}-'
                
                timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
                async with session.get(full_url, headers=headers, timeout=timeout) as response:
                    # handle rate limiting
                    if response.status == 429:
                        wait_time = 60 * (attempt + 1)  # increase wait time with each retry
                        logger.warning(f"rate limited on {filename}, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    # check if server supports resume
                    if resume_from > 0 and response.status not in [206, 200]:
                        logger.warning(f"server doesn't support resume for {filename}, starting over")
                        resume_from = 0
                        temp_filepath.unlink(missing_ok=True)
                        continue
                    
                    if response.status not in [200, 206]:
                        raise Exception(f"http error: {response.status}")
                    
                    # get total size
                    content_range = response.headers.get('Content-Range')
                    if content_range:
                        total_size = int(content_range.split('/')[-1])
                    else:
                        total_size = int(response.headers.get('content-length', 0)) + resume_from
                    
                    # setup progress bar
                    mode = 'ab' if resume_from > 0 else 'wb'
                    with tqdm(
                        total=total_size,
                        initial=resume_from,
                        unit='B',
                        unit_scale=True,
                        desc=filename,
                        position=0,
                        leave=True
                    ) as pbar:
                        async with aiofiles.open(temp_filepath, mode) as f:
                            async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                                await f.write(chunk)
                                pbar.update(len(chunk))
                                
                                # small random delays during download to throttle ourselves
                                if random.random() < 0.1:  # 10% chance per chunk
                                    await asyncio.sleep(random.uniform(0.01, 0.05))
                
                # download complete, rename temp file
                temp_filepath.rename(filepath)
                logger.info(f"completed: {filename}")
                return True
                
            except asyncio.TimeoutError:
                logger.warning(f"timeout on {filename}, attempt {attempt + 1}/{MAX_RETRIES}")
                if temp_filepath.exists():
                    resume_from = temp_filepath.stat().st_size
                if attempt < MAX_RETRIES - 1:
                    backoff = (2 ** attempt) + random.uniform(0, 1)
                    await asyncio.sleep(backoff)
                    
            except aiohttp.ClientError as e:
                logger.error(f"network error on {filename}: {e}, attempt {attempt + 1}/{MAX_RETRIES}")
                if temp_filepath.exists():
                    resume_from = temp_filepath.stat().st_size
                if attempt < MAX_RETRIES - 1:
                    backoff = (2 ** attempt) + random.uniform(0, 1)
                    await asyncio.sleep(backoff)
                    
            except Exception as e:
                logger.error(f"unexpected error on {filename}: {e}")
                break
        
        logger.error(f"failed to download {filename} after {MAX_RETRIES} attempts")
        return False


async def get_all_video_pages(session, base_url, start_url, pagination_selector, logger):
    """
    gets all unique video page urls, including pagination.
    """
    all_video_links = set()
    pages_to_visit = {start_url}
    visited_pages = set()
    
    logger.info("scanning for video links across all pages...")
    
    while pages_to_visit:
        current_page = pages_to_visit.pop()
        if current_page in visited_pages:
            continue
        
        # add delay between page requests
        await random_delay(MIN_DELAY_BETWEEN_REQUESTS, MAX_DELAY_BETWEEN_REQUESTS)
        
        visited_pages.add(current_page)
        logger.info(f"checking page: {current_page}")
        
        try:
            async with session.get(current_page, headers=get_random_headers()) as response:
                if response.status == 429:
                    logger.warning(f"rate limited, pausing for 30s...")
                    await asyncio.sleep(30)
                    pages_to_visit.add(current_page)  # re-add to try again
                    continue
                
                if response.status != 200:
                    logger.warning(f"couldn't fetch {current_page}")
                    continue
                    
                soup = BeautifulSoup(await response.text(), 'html.parser')
                
                # find all video links
                video_links = soup.select('a[href^="/video"]')
                for a in video_links:
                    full_url = urljoin(base_url, a['href'])
                    all_video_links.add(full_url)
                
                # look for pagination links
                pagination_links = soup.select(pagination_selector)
                for link in pagination_links:
                    href = link.get('href')
                    if href:
                        full_pagination_url = urljoin(base_url, href)
                        if full_pagination_url not in visited_pages:
                            pages_to_visit.add(full_pagination_url)
        
        except Exception as e:
            logger.error(f"error scanning {current_page}: {e}")
    
    return all_video_links


def check_vpn_status():
    """
    prompts user to confirm vpn is enabled before downloading.
    """
    print("\n" + "="*60)
    print("⚠️  VPN CHECK REMINDER")
    print("="*60)
    print("before downloading, ensure your vpn is enabled and connected.")
    print("="*60 + "\n")
    
    while True:
        response = input("is your vpn enabled? (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            print("great! proceeding with downloads...\n")
            return True
        elif response in ['n', 'no']:
            print("\nplease enable your vpn before continuing.")
            retry = input("do you want to try again? (y/n): ").strip().lower()
            if retry not in ['y', 'yes']:
                print("exiting. run the script again when your vpn is enabled.")
                return False
        else:
            print("please answer 'y' or 'n'")


def select_pagination_selector():
    """
    prompts user to select a pagination selector or provide custom one.
    """
    print("\n" + "="*60)
    print("PAGINATION SELECTOR OPTIONS")
    print("="*60)
    
    for key, info in PAGINATION_SELECTORS.items():
        print(f"{key}. {info['name']}")
    
    print("c. custom css selector")
    print("="*60 + "\n")
    
    while True:
        choice = input("select pagination style (press enter for default): ").strip().lower()
        
        if choice == '' or choice == '1':
            selector = PAGINATION_SELECTORS['1']['selector']
            print(f"using: {PAGINATION_SELECTORS['1']['name']}\n")
            return selector
        elif choice in PAGINATION_SELECTORS:
            selector = PAGINATION_SELECTORS[choice]['selector']
            print(f"using: {PAGINATION_SELECTORS[choice]['name']}\n")
            return selector
        elif choice == 'c':
            custom = input("enter custom css selector: ").strip()
            if custom:
                print(f"using custom selector: {custom}\n")
                return custom
            else:
                print("invalid selector, using default.\n")
                return PAGINATION_SELECTORS['1']['selector']
        else:
            print("invalid choice. please select 1-6 or 'c' for custom.")


async def main():
    start_url = input("enter the url of the page with video links: ").strip()
    
    # extract base url from the provided url
    parsed = urlparse(start_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    print(f"using base url: {base_url}\n")
    
    # get download directory
    download_input = input("enter download directory (press enter for './downloads'): ").strip()
    download_dir = Path(download_input) if download_input else Path("downloads")
    download_dir.mkdir(exist_ok=True)
    print(f"downloading to: {download_dir.absolute()}\n")
    
    # setup logging
    logger = setup_logging(download_dir)
    logger.info("="*60)
    logger.info("starting new download session")
    logger.info("="*60)
    
    # select pagination selector
    pagination_selector = select_pagination_selector()
    
    # create a semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
    
    # configure session with timeout and connection limits
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=3)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    
    async with aiohttp.ClientSession(
        timeout=timeout, 
        connector=connector,
        headers=get_random_headers()
    ) as session:
        # 1. get all unique video page urls
        logger.info("step 1: finding all video page urls...")
        video_page_links = await get_all_video_pages(
            session, base_url, start_url, pagination_selector, logger
        )
        
        if not video_page_links:
            logger.error("no links starting with '/video' found. exiting.")
            return

        logger.info(f"found {len(video_page_links)} unique video pages.")
        
        # 2. visit each page to get the best download link
        logger.info("step 2: getting download links from each page...")
        download_tasks_info = await asyncio.gather(
            *[get_best_download_link(session, page, logger) for page in video_page_links]
        )
        
        # filter out any pages where a link wasn't found
        valid_downloads = [info for info in download_tasks_info if info[0]]
        
        if not valid_downloads:
            logger.error("no valid download links found. exiting.")
            return
        
        # 3. vpn check before starting downloads
        if not check_vpn_status():
            return
        
        logger.info(f"step 3: starting download of {len(valid_downloads)} videos...")
        logger.info(f"concurrent downloads: {CONCURRENT_DOWNLOADS}")
        logger.info(f"timeout per request: {TIMEOUT_SECONDS}s")
        logger.info(f"max retries: {MAX_RETRIES}")
        logger.info(f"request delays: {MIN_DELAY_BETWEEN_REQUESTS}-{MAX_DELAY_BETWEEN_REQUESTS}s")
        logger.info(f"download delays: {MIN_DELAY_BETWEEN_DOWNLOADS}-{MAX_DELAY_BETWEEN_DOWNLOADS}s")
        
        # 4. download all files with resume capability
        download_coroutines = [
            download_file(session, base_url, url, filename, download_dir, semaphore, logger)
            for url, filename in valid_downloads
        ]
        
        results = await asyncio.gather(*download_coroutines)
        
        # summary
        successful = sum(results)
        failed = len(results) - successful
        logger.info("="*60)
        logger.info(f"download session complete!")
        logger.info(f"successful: {successful}/{len(results)}")
        logger.info(f"failed: {failed}/{len(results)}")
        logger.info("="*60)
        
        if failed > 0:
            print("\nsome downloads failed. check the log file for details.")
            print("you can re-run the script to resume incomplete downloads.")


if __name__ == "__main__":
    asyncio.run(main())