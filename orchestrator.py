#!/usr/bin/env python3
"""
JPX Automated Data Pipeline
Description: This script orchestrates the downloading and processing of JPX monthly
             investor trading data for ETFs and REITs.
             1. Reads a target year and month from 'config.ini'.
             2. Downloads the relevant Excel files using a web scraper.
             3. Processes the downloaded files to extract data.
             4. Generates final Data, Metadata, and ZIP archive files.
Version: 1.0
Date: 2025-07-17
"""

import time
import requests
import json
import logging
import sys
import os
import re
import zipfile
import configparser
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# --- SELENIUM & WEB SCRAPING IMPORTS ---
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

# --- DATA PROCESSING IMPORTS ---
import pandas as pd
import numpy as np

# --- INITIAL SETUP FOR LOGGING AND UNICODE ---

# Function to handle Unicode printing issues, especially on Windows
def safe_print(message):
    try:
        print(message)
    except UnicodeEncodeError:
        safe_message = message.encode('ascii', 'replace').decode('ascii')
        print(safe_message)

# Configure logging with UTF-8 encoding for file and console
class UTF8StreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super().__init__(stream)
        if hasattr(self.stream, 'reconfigure'):
            self.stream.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jpx_pipeline.log', 'w', encoding='utf-8'),
        UTF8StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- PART 1: JPX FILE DOWNLOADER CLASS ---

class JPXDownloader:
    """
    Handles the downloading of specific monthly reports from the JPX website.
    """
    BASE_URL = "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html"
    
    # Maps month number to the text abbreviation found on the website
    MONTH_ABBREVIATIONS = {
        "01": "Jan.", "02": "Feb.", "03": "Mar.", "04": "Apr.",
        "05": "May",  "06": "Jun.", "07": "Jul.", "08": "Aug.",
        "09": "Sep.", "10": "Oct.", "11": "Nov.", "12": "Dec."
    }
    
    def __init__(self, config):
        """Initializes the downloader with configuration."""
        self.target_year = config['year']
        self.target_month_num = config['month']
        self.target_month_abbr = self.MONTH_ABBREVIATIONS.get(self.target_month_num)
        
        self.download_dir = Path(config['download_directory'])
        self.headless = config['headless_mode'].lower() == 'true'
        self.log_file_path = self.download_dir / "jpx_download_log.json"
        
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.downloaded_files_log = self._load_download_log()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
    def _load_download_log(self):
        try:
            with open(self.log_file_path, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except FileNotFoundError:
            logger.info("Download log not found. A new one will be created.")
            return set()

    def _save_download_log(self):
        logger.info(f"Saving updated download log to '{self.log_file_path.name}'...")
        with open(self.log_file_path, 'w', encoding='utf-8') as f:
            json.dump(sorted(list(self.downloaded_files_log)), f, indent=4)

    def _download_file(self, url, file_path):
        try:
            response = self.session.get(url, timeout=45)
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(response.content)
            return True
        except requests.RequestException as e:
            logger.error(f"Network error downloading {url}. Error: {e}")
            return False

    def _setup_chrome_options(self):
        """Prepares Chrome options for undetected_chromedriver."""
        options = uc.ChromeOptions()
        if self.headless:
            logger.info("Running downloader in HEADLESS mode.")
            options.add_argument('--headless=new')
        else:
            logger.info("Running downloader in VISIBLE mode.")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--log-level=3')
        return options

    def run_download(self):
        """
        Main method to orchestrate the download process for the configured month and year.
        Returns True if new files were downloaded, False otherwise.
        """
        logger.info("="*60)
        logger.info(f"🚀 Starting JPX Downloader for {self.target_month_abbr} {self.target_year}")
        logger.info("="*60)

        if not self.target_month_abbr:
            logger.error(f"Invalid month configured: {self.target_month_num}. Please use a two-digit format (01-12).")
            return False

        driver = None
        files_downloaded_this_run = 0
        try:
            options = self._setup_chrome_options()
            driver = uc.Chrome(options=options)
            wait = WebDriverWait(driver, 20)
            
            logger.info(f"Navigating to JPX website: {self.BASE_URL}")
            driver.get(self.BASE_URL)
            
            # Handle cookie banner
            try:
                cookie_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Agree')]")))
                cookie_button.click()
                logger.info("Accepted cookie banner.")
                time.sleep(1)
            except (TimeoutException, NoSuchElementException):
                logger.info("Cookie banner not found or already accepted.")

            categories_to_process = {"ETFs": "etf_m", "REITs": "reit_m"}

            for category_name, file_filter in categories_to_process.items():
                logger.info(f"\n--- Processing Category: {category_name} ---")
                
                # Navigate to the correct category and tab
                driver.get(self.BASE_URL) # Reset to main page for each category
                wait.until(EC.element_to_be_clickable((By.LINK_TEXT, category_name))).click()
                time.sleep(1)
                wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Monthly"))).click()
                logger.info(f"Navigated to '{category_name}' > 'Monthly' tab.")
                time.sleep(2)

                # Select the target year from the dropdown
                logger.info(f"Attempting to select year '{self.target_year}' from dropdown...")
                archive_dropdown = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "backnumber")))
                select = Select(archive_dropdown)
                select.select_by_visible_text(self.target_year)
                logger.info(f"Successfully selected year '{self.target_year}'. Loading data...")
                time.sleep(3) # Wait for page to reload with year data

                # Parse the page to find the file link
                soup = BeautifulSoup(driver.page_source, 'lxml')
                
                # Find the main data table (where month headers like 'Jan.' are)
                thead = soup.find('thead')
                if not (thead and self.target_month_abbr in thead.text):
                     logger.warning(f"Could not find a valid data table for {category_name} in {self.target_year}.")
                     continue
                
                monthly_data_table = thead.find_parent('table')
                month_headers = [th.text.strip() for th in thead.find_all('th')]
                
                try:
                    target_month_index = month_headers.index(self.target_month_abbr)
                except ValueError:
                    logger.warning(f"Month '{self.target_month_abbr}' not found in table headers for {category_name} {self.target_year}.")
                    continue
                
                # The Excel file link is usually in the second data row (tr)
                excel_row = monthly_data_table.find('tbody').find_all('tr')[1]
                data_cell = excel_row.find_all('td')[target_month_index]
                
                link = data_cell.find('a', href=lambda h: h and file_filter in h and (h.endswith('.xls') or h.endswith('.xlsx')))
                
                if not link:
                    logger.warning(f"No downloadable file found for {category_name} for {self.target_month_abbr} {self.target_year}.")
                    continue
                    
                file_url = urljoin(driver.current_url, link['href'])
                file_name = file_url.split('/')[-1]

                if file_name in self.downloaded_files_log:
                    logger.info(f"[SKIP] '{file_name}' has already been downloaded.")
                    # Ensure file exists for extractor, otherwise re-download
                    if not (self.download_dir / file_name).exists():
                         logger.warning(f"[RE-DOWNLOAD] File '{file_name}' missing from disk. Downloading again.")
                    else:
                         files_downloaded_this_run += 1 # Count existing file as "available" for processing
                         continue
                
                logger.info(f"[NEW] Downloading '{file_name}'...")
                file_path = self.download_dir / file_name
                
                if self._download_file(file_url, file_path):
                    logger.info(f"[SUCCESS] Saved to: {file_path}")
                    self.downloaded_files_log.add(file_name)
                    files_downloaded_this_run += 1
                else:
                    logger.error(f"[FAIL] Download failed for '{file_name}'.")

        except Exception as e:
            logger.critical(f"An unexpected error occurred during download: {e}", exc_info=True)
            return False
        finally:
            if driver:
                driver.quit()
                logger.info("Browser closed.")
            self._save_download_log()

        if files_downloaded_this_run > 0:
            logger.info(f"\n✅ Download process complete. {files_downloaded_this_run} file(s) are ready for processing.")
            return True
        else:
            logger.warning("\n⚠️ Download process finished, but no new or required files were found for the specified period.")
            return False

# --- PART 2: ADAPTIVE DATA EXTRACTOR CLASS (from jtmd_scraper5.py) ---

class AdaptiveJTMDExtractor:
    """
    Adaptive JTMD Excel Data Extractor with Dynamic Balance Detection.
    (This class is a direct integration of your proven 'jtmd_scraper5.py' logic)
    """
    
    def __init__(self, input_folder: str, output_folder: str):
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(exist_ok=True)
        self.column_mapping = self._initialize_column_mapping()
        self.category_structure = self._initialize_category_structure()
        self.fixed_mappings = self._initialize_fixed_mappings()
        logger.info(f"Adaptive JTMD Extractor initialized. Input: '{self.input_folder}', Output: '{self.output_folder}'")
        
    def _initialize_column_mapping(self):
        # This extensive mapping is kept exactly as in your original script
        return {
            # ETF Balance columns (14 columns)
            'JTMD.ETF.BAL.PROP.M': 'ETF, Balance, Proprietary', 'JTMD.ETF.BAL.BROKER.M': 'ETF, Balance, Brokerage', 
            'JTMD.ETF.BAL.TOT.M': 'ETF, Balance, Total', 'JTMD.ETF.BAL.INST.M': 'ETF, Balance, Institutions',
            'JTMD.ETF.BAL.INDIV.M': 'ETF, Balance, Individuals', 'JTMD.ETF.BAL.FOR.M': 'ETF, Balance, Foreigners',
            'JTMD.ETF.BAL.SECCOS.M': 'ETF, Balance, Securities Cos.', 'JTMD.ETF.BAL.INVTRUST.M': 'ETF, Balance, Investment Trusts',
            'JTMD.ETF.BAL.BUSCORP.M': 'ETF, Balance, Business Cos.', 'JTMD.ETF.BAL.OTHINST.M': 'ETF, Balance, Other Cos.',
            'JTMD.ETF.BAL.INSTFIN.M': 'ETF, Balance, Financial Institutions', 'JTMD.ETF.BAL.INS.M': 'ETF, Balance, Life/Non-Life Insurance',
            'JTMD.ETF.BAL.BK.M': 'ETF, Balance, Banks', 'JTMD.ETF.BAL.OTHERFIN.M': 'ETF, Balance, Other Financials',
            # ETF Sales columns (14 columns)
            'JTMD.ETF.SAL.PROP.M': 'ETF, Sales, Proprietary', 'JTMD.ETF.SAL.BROKER.M': 'ETF, Sales, Brokerage',
            'JTMD.ETF.SAL.TOT.M': 'ETF, Sales, Total', 'JTMD.ETF.SAL.INST.M': 'ETF, Sales, Institutions',
            'JTMD.ETF.SAL.INDIV.M': 'ETF, Sales, Individuals', 'JTMD.ETF.SAL.FOR.M': 'ETF, Sales, Foreigners',
            'JTMD.ETF.SAL.SECCOS.M': 'ETF, Sales, Securities Cos.', 'JTMD.ETF.SAL.INVTRUST.M': 'ETF, Sales, Investment Trusts',
            'JTMD.ETF.SAL.BUSCORP.M': 'ETF, Sales, Business Cos.', 'JTMD.ETF.SAL.OTHINST.M': 'ETF, Sales, Other Cos.',
            'JTMD.ETF.SAL.INSTFIN.M': 'ETF, Sales, Financial Institutions', 'JTMD.ETF.SAL.INS.M': 'ETF, Sales, Life/Non-Life Insurance',
            'JTMD.ETF.SAL.BK.M': 'ETF, Sales, Banks', 'JTMD.ETF.SAL.OTHERFIN.M': 'ETF, Sales, Other Financials',
            # ETF Purchases columns (14 columns)
            'JTMD.ETF.PURCH.PROP.M': 'ETF, Purchases, Proprietary', 'JTMD.ETF.PURCH.BROKER.M': 'ETF, Purchases, Brokerage',
            'JTMD.ETF.PURCH.TOT.M': 'ETF, Purchases, Total', 'JTMD.ETF.PURCH.INST.M': 'ETF, Purchases, Institutions',
            'JTMD.ETF.PURCH.INDIV.M': 'ETF, Purchases, Individuals', 'JTMD.ETF.PURCH.FOR.M': 'ETF, Purchases, Foreigners',
            'JTMD.ETF.PURCH.SECCOS.M': 'ETF, Purchases, Securities Cos.', 'JTMD.ETF.PURCH.INVTRUST.M': 'ETF, Purchases, Investment Trusts',
            'JTMD.ETF.PURCH.BUSCORP.M': 'ETF, Purchases, Business Cos.', 'JTMD.ETF.PURCH.OTHINST.M': 'ETF, Purchases, Other Cos.',
            'JTMD.ETF.PURCH.INSTFIN.M': 'ETF, Purchases, Financial Institutions', 'JTMD.ETF.PURCH.INS.M': 'ETF, Purchases, Life/Non-Life Insurance',
            'JTMD.ETF.PURCH.BK.M': 'ETF, Purchases, Banks', 'JTMD.ETF.PURCH.OTHERFIN.M': 'ETF, Purchases, Other Financials',
            # REIT columns
            'JTMD.REIT.BAL.PROP.M': 'REIT, Balance, Proprietary', 'JTMD.REIT.BAL.BROKER.M': 'REIT, Balance, Brokerage',
            'JTMD.REIT.BAL.TOT.M': 'REIT, Balance, Total', 'JTMD.REIT.BAL.INST.M': 'REIT, Balance, Institutions',
            'JTMD.REIT.BAL.INDIV.M': 'REIT, Balance, Individuals', 'JTMD.REIT.BAL.FOR.M': 'REIT, Balance, Foreigners',
            'JTMD.REIT.BAL.SECCOS.M': 'REIT, Balance, Securities Cos.', 'JTMD.REIT.BAL.INVTRUST.M': 'REIT, Balance, Investment Trusts',
            'JTMD.REIT.BAL.BUSCORP.M': 'REIT, Balance, Business Cos.', 'JTMD.REIT.BAL.OTHINST.M': 'REIT, Balance, Other Cos.',
            'JTMD.REIT.BAL.INSTFIN.M': 'REIT, Balance, Financial Institutions', 'JTMD.REIT.BAL.INS.M': 'REIT, Balance, Life/Non-Life Insurance',
            'JTMD.REIT.BAL.BK.M': 'REIT, Balance, Banks', 'JTMD.REIT.BAL.OTHERFIN.M': 'REIT, Balance, Other Financials',
            'JTMD.REIT.SAL.PROP.M': 'REIT, Sales, Proprietary', 'JTMD.REIT.SAL.BROKER.M': 'REIT, Sales, Brokerage',
            'JTMD.REIT.SAL.TOT.M': 'REIT, Sales, Total', 'JTMD.REIT.SAL.INST.M': 'REIT, Sales, Institutions',
            'JTMD.REIT.SAL.INDIV.M': 'REIT, Sales, Individuals', 'JTMD.REIT.SAL.FOR.M': 'REIT, Sales, Foreigners',
            'JTMD.REIT.SAL.SECCOS.M': 'REIT, Sales, Securities Cos.', 'JTMD.REIT.SAL.INVTRUST.M': 'REIT, Sales, Investment Trusts',
            'JTMD.REIT.SAL.BUSCORP.M': 'REIT, Sales, Business Cos.', 'JTMD.REIT.SAL.OTHINST.M': 'REIT, Sales, Other Cos.',
            'JTMD.REIT.SAL.INSTFIN.M': 'REIT, Sales, Financial Institutions', 'JTMD.REIT.SAL.INS.M': 'REIT, Sales, Life/Non-Life Insurance',
            'JTMD.REIT.SAL.BK.M': 'REIT, Sales, Banks', 'JTMD.REIT.SAL.OTHERFIN.M': 'REIT, Sales, Other Financials',
            'JTMD.REIT.PURCH.PROP.M': 'REIT, Purchases, Proprietary', 'JTMD.REIT.PURCH.BROKER.M': 'REIT, Purchases, Brokerage',
            'JTMD.REIT.PURCH.TOT.M': 'REIT, Purchases, Total', 'JTMD.REIT.PURCH.INST.M': 'REIT, Purchases, Institutions',
            'JTMD.REIT.PURCH.INDIV.M': 'REIT, Purchases, Individuals', 'JTMD.REIT.PURCH.FOR.M': 'REIT, Purchases, Foreigners',
            'JTMD.REIT.PURCH.SECCOS.M': 'REIT, Purchases, Securities Cos.', 'JTMD.REIT.PURCH.INVTRUST.M': 'REIT, Purchases, Investment Trusts',
            'JTMD.REIT.PURCH.BUSCORP.M': 'REIT, Purchases, Business Cos.', 'JTMD.REIT.PURCH.OTHINST.M': 'REIT, Purchases, Other Cos.',
            'JTMD.REIT.PURCH.INSTFIN.M': 'REIT, Purchases, Financial Institutions', 'JTMD.REIT.PURCH.INS.M': 'REIT, Purchases, Life/Non-Life Insurance',
            'JTMD.REIT.PURCH.BK.M': 'REIT, Purchases, Banks', 'JTMD.REIT.PURCH.OTHERFIN.M': 'REIT, Purchases, Other Financials'
        }
    
    def _initialize_category_structure(self):
        return {
            'PROP': {'base_row': 13, 'search_offsets': [0, 1, 2]}, 'BROKER': {'base_row': 16, 'search_offsets': [0, 1, 2]},    
            'TOT': {'base_row': 19, 'search_offsets': [0, 1, 2]}, 'INST': {'base_row': 24, 'search_offsets': [0, 1, 2]},      
            'INDIV': {'base_row': 27, 'search_offsets': [0, 1, 2]}, 'FOR': {'base_row': 30, 'search_offsets': [0, 1, 2]},
            'SECCOS': {'base_row': 33, 'search_offsets': [0, 1, 2]}, 'INVTRUST': {'base_row': 38, 'search_offsets': [0, 1, 2]},
            'BUSCORP': {'base_row': 41, 'search_offsets': [0, 1, 2]}, 'OTHINST': {'base_row': 44, 'search_offsets': [0, 1, 2]},
            'INSTFIN': {'base_row': 47, 'search_offsets': [0, 1, 2]}, 'INS': {'base_row': 52, 'search_offsets': [0, 1, 2]},
            'BK': {'base_row': 55, 'search_offsets': [0, 1, 2]}, 'OTHERFIN': {'base_row': 58, 'search_offsets': [0, 1, 2]}
        }
    
    def _initialize_fixed_mappings(self):
        # This mapping is also kept exactly as is from your script
        return {
            # ETF Sales & Purchases
            'JTMD.ETF.SAL.PROP.M': {'row': 13, 'col': 4}, 'JTMD.ETF.PURCH.PROP.M': {'row': 14, 'col': 4},
            'JTMD.ETF.SAL.BROKER.M': {'row': 16, 'col': 4}, 'JTMD.ETF.PURCH.BROKER.M': {'row': 17, 'col': 4},
            'JTMD.ETF.SAL.TOT.M': {'row': 19, 'col': 4}, 'JTMD.ETF.PURCH.TOT.M': {'row': 20, 'col': 4},
            'JTMD.ETF.SAL.INST.M': {'row': 24, 'col': 4}, 'JTMD.ETF.PURCH.INST.M': {'row': 25, 'col': 4},
            'JTMD.ETF.SAL.INDIV.M': {'row': 27, 'col': 4}, 'JTMD.ETF.PURCH.INDIV.M': {'row': 28, 'col': 4},
            'JTMD.ETF.SAL.FOR.M': {'row': 30, 'col': 4}, 'JTMD.ETF.PURCH.FOR.M': {'row': 31, 'col': 4},
            'JTMD.ETF.SAL.SECCOS.M': {'row': 33, 'col': 4}, 'JTMD.ETF.PURCH.SECCOS.M': {'row': 34, 'col': 4},
            'JTMD.ETF.SAL.INVTRUST.M': {'row': 38, 'col': 4}, 'JTMD.ETF.PURCH.INVTRUST.M': {'row': 39, 'col': 4},
            'JTMD.ETF.SAL.BUSCORP.M': {'row': 41, 'col': 4}, 'JTMD.ETF.PURCH.BUSCORP.M': {'row': 42, 'col': 4},
            'JTMD.ETF.SAL.OTHINST.M': {'row': 44, 'col': 4}, 'JTMD.ETF.PURCH.OTHINST.M': {'row': 45, 'col': 4},
            'JTMD.ETF.SAL.INSTFIN.M': {'row': 47, 'col': 4}, 'JTMD.ETF.PURCH.INSTFIN.M': {'row': 48, 'col': 4},
            'JTMD.ETF.SAL.INS.M': {'row': 52, 'col': 4}, 'JTMD.ETF.PURCH.INS.M': {'row': 53, 'col': 4},
            'JTMD.ETF.SAL.BK.M': {'row': 55, 'col': 4}, 'JTMD.ETF.PURCH.BK.M': {'row': 56, 'col': 4},
            'JTMD.ETF.SAL.OTHERFIN.M': {'row': 58, 'col': 4}, 'JTMD.ETF.PURCH.OTHERFIN.M': {'row': 59, 'col': 4},
            # REIT Sales & Purchases
            'JTMD.REIT.SAL.PROP.M': {'row': 13, 'col': 4}, 'JTMD.REIT.PURCH.PROP.M': {'row': 14, 'col': 4},
            'JTMD.REIT.SAL.BROKER.M': {'row': 16, 'col': 4}, 'JTMD.REIT.PURCH.BROKER.M': {'row': 17, 'col': 4},
            'JTMD.REIT.SAL.TOT.M': {'row': 19, 'col': 4}, 'JTMD.REIT.PURCH.TOT.M': {'row': 20, 'col': 4},
            'JTMD.REIT.SAL.INST.M': {'row': 24, 'col': 4}, 'JTMD.REIT.PURCH.INST.M': {'row': 25, 'col': 4},
            'JTMD.REIT.SAL.INDIV.M': {'row': 27, 'col': 4}, 'JTMD.REIT.PURCH.INDIV.M': {'row': 28, 'col': 4},
            'JTMD.REIT.SAL.FOR.M': {'row': 30, 'col': 4}, 'JTMD.REIT.PURCH.FOR.M': {'row': 31, 'col': 4},
            'JTMD.REIT.SAL.SECCOS.M': {'row': 33, 'col': 4}, 'JTMD.REIT.PURCH.SECCOS.M': {'row': 34, 'col': 4},
            'JTMD.REIT.SAL.INVTRUST.M': {'row': 38, 'col': 4}, 'JTMD.REIT.PURCH.INVTRUST.M': {'row': 39, 'col': 4},
            'JTMD.REIT.SAL.BUSCORP.M': {'row': 41, 'col': 4}, 'JTMD.REIT.PURCH.BUSCORP.M': {'row': 42, 'col': 4},
            'JTMD.REIT.SAL.OTHINST.M': {'row': 44, 'col': 4}, 'JTMD.REIT.PURCH.OTHINST.M': {'row': 45, 'col': 4},
            'JTMD.REIT.SAL.INSTFIN.M': {'row': 47, 'col': 4}, 'JTMD.REIT.PURCH.INSTFIN.M': {'row': 48, 'col': 4},
            'JTMD.REIT.SAL.INS.M': {'row': 52, 'col': 4}, 'JTMD.REIT.PURCH.INS.M': {'row': 53, 'col': 4},
            'JTMD.REIT.SAL.BK.M': {'row': 55, 'col': 4}, 'JTMD.REIT.PURCH.BK.M': {'row': 56, 'col': 4},
            'JTMD.REIT.SAL.OTHERFIN.M': {'row': 58, 'col': 4}, 'JTMD.REIT.PURCH.OTHERFIN.M': {'row': 59, 'col': 4}
        }

    def _extract_period_from_filename(self, filename: str) -> str:
        match = re.search(r'm(\d{2})(\d{2})', filename.lower())
        if match:
            year_suffix, month = match.groups()
            return f"20{year_suffix}-{month}"
        logger.warning(f"Could not extract period from '{filename}'. Using current date as fallback.")
        return datetime.now().strftime('%Y-%m')
        
    def _preserve_number_formatting(self, value):
        if pd.isna(value) or value in [None, '']: return ''
        str_value = str(value).strip()
        return str_value
    
    def find_balance_in_category(self, data_rows, category_key, sheet_type):
        category_info = self.category_structure[category_key]
        balance_column_code = f"JTMD.{sheet_type.upper()}.BAL.{category_key}.M"
        for offset in category_info['search_offsets']:
            test_row = category_info['base_row'] + offset
            try:
                if test_row < len(data_rows) and len(data_rows[test_row]) > 6:
                    raw_value = data_rows[test_row][6]  # Column G
                    formatted_value = self._preserve_number_formatting(raw_value)
                    if formatted_value:
                        return (balance_column_code, formatted_value)
            except IndexError:
                continue
        return None
        
    def _extract_from_sheet(self, df: pd.DataFrame, sheet_type: str) -> Dict[str, str]:
        """Extracts all data (fixed and dynamic) from a given sheet DataFrame."""
        data_rows = df.values.tolist()
        extracted_data = {}
        
        # --- Fixed coordinates (Sales & Purchases) ---
        prefix = f"JTMD.{sheet_type.upper()}"
        relevant_mappings = {k: v for k, v in self.fixed_mappings.items() 
                           if k.startswith(prefix)}
        for code, coords in relevant_mappings.items():
            try:
                raw_value = data_rows[coords['row']][coords['col']]
                formatted_value = self._preserve_number_formatting(raw_value)
                if formatted_value: extracted_data[code] = formatted_value
            except IndexError:
                continue

        # --- Dynamic coordinates (Balance) ---
        for category_key in self.category_structure.keys():
            result = self.find_balance_in_category(data_rows, category_key, sheet_type)
            if result:
                extracted_data[result[0]] = result[1]
                
        logger.info(f"Extracted {len(extracted_data)}/{len(relevant_mappings) + len(self.category_structure)} data points for {sheet_type}.")
        return extracted_data
    
    def process_excel_file(self, file_path: Path):
        logger.info(f"📁 Processing: {file_path.name}")
        period = self._extract_period_from_filename(file_path.name)
        
        try:
            excel_file = pd.ExcelFile(file_path)
            if 'Value' not in excel_file.sheet_names:
                logger.error(f"No 'Value' sheet found in {file_path.name}")
                return period, {}
            
            df_value = pd.read_excel(file_path, sheet_name='Value', header=None)
            sheet_type = 'ETF' if 'etf' in file_path.name.lower() else 'REIT'
            
            data = self._extract_from_sheet(df_value, sheet_type)
            return period, data
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")
            return period, {}
    
    def _create_output_csv(self, all_data: Dict[str, Dict[str, str]], timestamp: str):
        columns = [''] + list(self.column_mapping.keys())
        df_data = []
        for period, period_data in sorted(all_data.items()):
            row = {'': period}
            row.update({col: period_data.get(col, '') for col in columns[1:]})
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        # Reorder columns to match the required format
        df = df[[''] + list(self.column_mapping.keys())]

        # Create a header DataFrame
        header_df = pd.DataFrame([self.column_mapping.keys(), self.column_mapping.values()], columns=df.columns[1:])
        header_df.insert(0, '', ['', ''])

        output_file = self.output_folder / f"JTMD_DATA_{timestamp}.csv"
        # Write headers without pandas index/header, then append data
        header_df.to_csv(output_file, index=False, header=False, encoding='utf-8')
        df.to_csv(output_file, mode='a', index=False, header=False, encoding='utf-8')

        logger.info(f"✅ Created data CSV: {output_file}")
        return output_file
        
    def _create_metadata_csv(self, timestamp: str):
        metadata_rows = []
        for code, desc in self.column_mapping.items():
            metadata_rows.append({
                'CODE': code, 'DESCRIPTION': desc, 'UNIT': 'Thousand JPY',
                'FREQUENCY': 'Monthly', 'SOURCE': 'Japan Exchange Group (JPX)',
                'DATASET': 'JTMD', 'LAST_UPDATE': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                'NEXT_RELEASE_DATE': '', 'DATA_TYPE': 'Numeric', 'CATEGORY': 'Trading Statistics'
            })
        df = pd.DataFrame(metadata_rows)
        metadata_file = self.output_folder / f"JTMD_META_{timestamp}.csv"
        df.to_csv(metadata_file, index=False, encoding='utf-8')
        logger.info(f"✅ Created metadata CSV: {metadata_file}")
        return metadata_file

    def _create_zip_archive(self, data_file, metadata_file, timestamp):
        zip_path = self.output_folder / f"JTMD_{timestamp}.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(data_file, data_file.name)
            zf.write(metadata_file, metadata_file.name)
        logger.info(f"📦 Created ZIP archive: {zip_path}")
        return zip_path
        
    def run(self):
        logger.info("="*60)
        logger.info("🧠 Starting Adaptive Data Extractor")
        logger.info("="*60)
        
        excel_files = list(self.input_folder.glob('*.xls*'))
        if not excel_files:
            logger.error(f"No Excel files found in the input directory: '{self.input_folder}'")
            return
            
        all_data = {}
        for file_path in excel_files:
            period, data = self.process_excel_file(file_path)
            if period not in all_data:
                all_data[period] = {}
            all_data[period].update(data)
            
        if not all_data:
            logger.error("Data extraction failed. No data was extracted from any file.")
            return

        timestamp = datetime.now().strftime('%Y%m%d')
        data_file = self._create_output_csv(all_data, timestamp)
        metadata_file = self._create_metadata_csv(timestamp)
        self._create_zip_archive(data_file, metadata_file, timestamp)
        
        logger.info("🎉 Adaptive extraction process completed successfully!")


# --- PART 3: MAIN ORCHESTRATION ---

def load_config():
    """Loads settings from config.ini."""
    parser = configparser.ConfigParser()
    config_path = Path('config.ini')
    if not config_path.exists():
        logger.critical(f"FATAL: Configuration file '{config_path}' not found.")
        sys.exit(1)
    
    parser.read(config_path)
    return {
        'year': parser.get('SETTINGS', 'year'),
        'month': parser.get('SETTINGS', 'month'),
        'download_directory': parser.get('PATHS', 'download_directory'),
        'output_directory': parser.get('PATHS', 'output_directory'),
        'headless_mode': parser.get('SCRAPER', 'headless_mode')
    }

def main():
    """Main pipeline execution function."""
    start_time = time.time()
    logger.info("==========================================================")
    logger.info("          STARTING JPX AUTOMATED DATA PIPELINE")
    logger.info("==========================================================")
    
    try:
        # 1. Load configuration
        config = load_config()
        logger.info(f"Configuration loaded for Target: {config['month']}-{config['year']}.")
        logger.info(f"Download Path: '{config['download_directory']}' | Output Path: '{config['output_directory']}'")

        # 2. Run the downloader
        downloader = JPXDownloader(config)
        files_are_ready = downloader.run_download()
        
        # 3. If downloads were successful (or files already exist), run the extractor
        if files_are_ready:
            extractor = AdaptiveJTMDExtractor(
                input_folder=config['download_directory'],
                output_folder=config['output_directory']
            )
            extractor.run()
        else:
            logger.warning("Extractor step skipped because no files were downloaded or found for the target period.")

        total_time = time.time() - start_time
        logger.info("==========================================================")
        logger.info(f"✅ PIPELINE FINISHED IN {total_time:.2f} SECONDS")
        logger.info("==========================================================")
        
    except Exception as e:
        logger.critical(f"A critical error occurred in the main pipeline: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()