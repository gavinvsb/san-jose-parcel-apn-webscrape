import undetected_chromedriver as uc
from random import randint
from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import json
import time
import pickle
import os
import datetime

import multiprocessing as mp

num_processes = 20
# sample_size = 20 # TEST RUN
sample_size = None # REAL RUN

# select which of the subset of csvs to compute
csvs_to_run = [1]

def process_apn(APN, df_dict, driver):
    driver.get('https://payments.sccgov.org/PropertyTax/Secured')
    apn_input = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div[1]/div[3]/div/div/form/div/div/div/div[1]/input')
    actions = ActionChains(driver)
    actions.move_to_element(apn_input).click().perform()
    apn_input.send_keys(APN)
    form_submit_input = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div[1]/div[3]/div/div/form/div/div/div/div[2]/button')
    form_submit_input.send_keys(Keys.ENTER)
    try:
        df_dict[APN] = pd.read_html(BeautifulSoup(driver.page_source, 'html.parser').prettify())
    except:
        df_dict[APN] = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div[1]/div[3]/div/div/form/div/div/span').text
        return


class WebProcess(mp.Process):

    def __init__(self, apn_queue, df_dict):
        mp.Process.__init__(self)
        self.apn_queue = apn_queue
        self.df_dict = df_dict
        self.df_dict['processes'] = str(int(self.df_dict['processes']) + 1)
        
    def run(self):
        try:
            proc_name = self.name
            driver = uc.Chrome()
            while True:
                next_apn = self.apn_queue.get()
                if next_apn is None:
                    # Poison pill means we should exit
                    print('%s: Exiting' % proc_name)
                    driver.quit()
                    self.shutdown()
                    break
                process_apn(next_apn, self.df_dict, driver)
        except Exception as e:
            print(e)
            self.shutdown()
        return

    def shutdown(self):
        self.df_dict['processes'] = str(int(self.df_dict['processes']) - 1)

if __name__ == '__main__':
    # parcels_df = pd.read_csv("Parcels.csv", dtype=str)

    # for i in range(9):
    #     parcels_df[i * 25000 : (i + 1) * 25000 ].to_csv('csvs/Parcels_%d.csv' % i)
    # parcels_df[9 * 25000 : ].to_csv('csvs/Parcels_%d.csv' % 9)
    
    t1 = time.time()

    with mp.Manager() as manager:

        for n in csvs_to_run:
            df_dict = manager.dict()
            apn_queue = mp.Queue()
            df_dict['processes'] = '0'

            processes = [ WebProcess(apn_queue, df_dict) for i in range(num_processes)]
            try:
                for p in processes:
                    p.start()
            except Exception as e:
                print(e)

            parcels_df = pd.read_csv("csvs/Parcels_%d.csv" % n, dtype=str)
            print('no apn found on %d lines' % parcels_df[parcels_df.NOTES =='No APN, data reviewer correction.'].shape[0])
            parcels_df = parcels_df[parcels_df.NOTES !='No APN, data reviewer correction.']
            if sample_size != None:
                for APN in parcels_df['APN'].sample(n=sample_size, random_state=201):
                    # print('adding apn')
                    apn_queue.put(APN)
            else:
                for APN in parcels_df['APN']:
                    # print('adding apn')
                    apn_queue.put(APN)

            for i in range(num_processes):
                apn_queue.put(None)

            time.sleep(5)
            print('initial file write')
            current_pickle_fname = 'county_data_%d.pkl' % n
            old_pickle_fname = 'county_data_%d_old.pkl' % n
            with open(current_pickle_fname,'wb') as file:
                pickl_dict = {}
                pickl_dict.update(df_dict)
                pickle.dump(pickl_dict, file)

            while df_dict['processes'] != '0':
                # print(df_dict['processes'])
                time.sleep(300)
                print('writing to temp pkl file')
                if os.path.isfile(current_pickle_fname):
                    size = Path(current_pickle_fname).stat().st_size
                    if size < 3:
                        print('pkl file empty')
                        print('last entry in object: ' + df_dict.tail(10))
                        exit()
                    if os.path.isfile(old_pickle_fname):
                        os.remove(old_pickle_fname)
                    os.rename(current_pickle_fname, old_pickle_fname)
                with open(current_pickle_fname,'wb') as file:
                    pickl_dict = {}
                    pickl_dict.update(df_dict)
                    pickle.dump(pickl_dict, file)

            print(df_dict)

            t2 = time.time()
            print(f'csv {n} elapsed time: {t2-t1} seconds')
            pickle_fname = 'county_data_%d.pkl' % n
            if not os.path.isfile(pickle_fname):
                with open(pickle_fname,'wb') as file:
                    pickl_dict = {}
                    pickl_dict.update(df_dict)
                    pickle.dump(pickl_dict, file)
            print(f"collection done!")
