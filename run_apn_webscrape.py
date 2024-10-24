import undetected_chromedriver as uc
from random import randint
from time import sleep, time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import pickle
import os
import multiprocessing as mp

# Configuration
NUM_PROCESSES = 20
SAMPLE_SIZE = None  # Set to a number for test runs, or None for full data run
CSV_FILES_TO_RUN = [1]  # Specify which CSV files to process

def process_apn(apn, df_dict, driver):
    """Fetch and process data for a given APN."""
    driver.get('https://payments.sccgov.org/PropertyTax/Secured')

    try:
        # Enter APN in input field and submit form
        apn_input = driver.find_element(By.XPATH, 
            '/html/body/div[2]/div/div[2]/div[1]/div[3]/div/div/form/div/div/div/div[1]/input')
        ActionChains(driver).move_to_element(apn_input).click().perform()
        apn_input.send_keys(apn)

        submit_btn = driver.find_element(By.XPATH, 
            '/html/body/div[2]/div/div[2]/div[1]/div[3]/div/div/form/div/div/div/div[2]/button')
        submit_btn.send_keys(Keys.ENTER)

        # Extract table data using BeautifulSoup
        df_dict[apn] = pd.read_html(BeautifulSoup(driver.page_source, 'html.parser').prettify())

    except Exception:
        # If there's no data available, store the error message
        df_dict[apn] = driver.find_element(By.XPATH, 
            '/html/body/div[2]/div/div[2]/div[1]/div[3]/div/div/form/div/div/span').text

class WebProcess(mp.Process):
    """Multiprocessing class for handling parallel APN data extraction."""
    
    def __init__(self, apn_queue, df_dict):
        super().__init__()
        self.apn_queue = apn_queue
        self.df_dict = df_dict
        self.df_dict['processes'] = str(int(self.df_dict.get('processes', 0)) + 1)

    def run(self):
        """Main loop for processing APNs."""
        driver = uc.Chrome()

        try:
            while True:
                apn = self.apn_queue.get()
                if apn is None:  # Poison pill to signal process termination
                    print(f'{self.name}: Exiting')
                    break
                process_apn(apn, self.df_dict, driver)
        except Exception as e:
            print(f'Error in {self.name}: {e}')
        finally:
            driver.quit()
            self.shutdown()

    def shutdown(self):
        """Decrement the active process count."""
        self.df_dict['processes'] = str(int(self.df_dict['processes']) - 1)

def main():
    """Main function to coordinate multiprocessing and data collection."""
    for csv_num in CSV_FILES_TO_RUN:
        start_time = time()

        with mp.Manager() as manager:
            df_dict = manager.dict()
            apn_queue = mp.Queue()
            df_dict['processes'] = '0'

            # Start worker processes
            processes = [WebProcess(apn_queue, df_dict) for _ in range(NUM_PROCESSES)]
            for p in processes:
                p.start()

            # Load parcel data and filter out rows with missing APNs
            parcels_df = pd.read_csv(f"csvs/Parcels_{csv_num}.csv", dtype=str)
            parcels_df = parcels_df[parcels_df.NOTES != 'No APN, data reviewer correction.']
            print(f'{parcels_df[parcels_df.NOTES == "No APN, data reviewer correction."].shape[0]} lines skipped')

            # Add APNs to the processing queue
            apns = parcels_df['APN'].sample(n=SAMPLE_SIZE, random_state=201) if SAMPLE_SIZE else parcels_df['APN']
            for apn in apns:
                apn_queue.put(apn)

            # Add poison pills to terminate processes
            for _ in range(NUM_PROCESSES):
                apn_queue.put(None)

            # Write initial pickle file
            write_pickle(df_dict, f'county_data_{csv_num}.pkl')

            # Monitor and periodically save progress
            monitor_progress(df_dict, csv_num)

            # Wait for all processes to finish
            for p in processes:
                p.join()

            print(f'CSV {csv_num} processing completed in {time() - start_time:.2f} seconds')

def write_pickle(df_dict, filename):
    """Save the current state of the data to a pickle file."""
    with open(filename, 'wb') as file:
        pickle.dump(dict(df_dict), file)

def monitor_progress(df_dict, csv_num):
    """Monitor the progress and save interim results every 5 minutes."""
    current_pickle = f'county_data_{csv_num}.pkl'
    old_pickle = f'county_data_{csv_num}_old.pkl'

    while df_dict['processes'] != '0':
        sleep(300)  # Wait for 5 minutes before next save
        print('Writing progress to temporary pickle file...')

        # Rotate old pickle files
        if os.path.isfile(current_pickle):
            if Path(current_pickle).stat().st_size < 3:
                print('Pickle file is empty, exiting...')
                exit()
            if os.path.isfile(old_pickle):
                os.remove(old_pickle)
            os.rename(current_pickle, old_pickle)

        write_pickle(df_dict, current_pickle)

if __name__ == '__main__':
    main()
