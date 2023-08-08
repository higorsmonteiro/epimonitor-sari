import os
import csv
import time
import numpy as np
import pandas as pd
import datetime as dt 
from collections import defaultdict

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

class SivepPipe:
    '''
        Selenium application for automatic download of DBF files from the SIVEP-Gripe
        information system. 
        
        Using an authorized certification (login and password), the application sends
        a download request to the system and waits until there is an available file
        for its requisition number. In case there is a large delay between the requisition 
        and the time the file is available, the application can be closed and opened again
        to download the file with the proper requisition file.

        Disclaimer: Files are available only for the day in the system. Therefore, a new
        requisition must be done to download the file.

        Args:
        -----
            certification:
                dictionary. Contains only two keys, 'username' and 'password'. It should be
                an authorized certification for the SIVEP-Gripe system.
            requisition_number:
                String. If a requisition was already made, the requisition number can be 
                provided at the beginning.
            download_folder:
                String. Absolute path to store the download file. A raw string must be parsed
                following the format 'C:/path/to/folder'.
            headless:
                Bool. Whether the browser should explicit or run in the background.
            verbose:
                Bool. Signal to provide logs in the output.
    
    '''
    def __init__(self, certification, download_folder,
                 requisition_number=None, headless=False):
        
        # -- input variables
        self._username, self._password = certification['username'], certification['password']
        self.download_folder = download_folder
        self.requisition_number = requisition_number
        self.headless = headless
        
        self.query = ''
        self.requisition_table = None
        self.downloaded = False
        
        self.driver = None
        self.url = "https://sivepgripe.saude.gov.br/sivepgripe/login.html?0"
        self.browser_options = webdriver.ChromeOptions()
        if self.headless:
            self.browser_options.add_argument("--headless=new")
        self.browser_options.add_experimental_option("prefs", 
            {
                "download.default_directory": self.download_folder,
                "download.prompt_for_download": False,
                'profile.default_content_setting_values.automatic_downloads': 1,
            })
            
        
    # ------------------ properties ------------------

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, x):
        raise Exception('Cannot change username.')
    
    # ------------------ navigation ------------------
    
    def login(self):
        '''
            Go to the initial login page of SIVEP-Gripe. Usually used only once, but it 
            can be used again when the access timeout. 

            Obs: we might consider in adding a few extra error handling conditions.
        '''
        if self.driver is None:
            self.driver = webdriver.Chrome(options=self.browser_options)
        
        # -- locate the login fields and clear any text in them (if the case)
        self.driver.get(self.url)
        usernm_form =  WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.ID, 'BoxAreaLogin_campo_email'))).find_element(By.TAG_NAME, 'input')
        passwd_form =  WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.ID, 'BoxAreaLogin_campo_senha'))).find_element(By.TAG_NAME, 'input')
        usernm_form.clear()
        passwd_form.clear()

        # -- send certification
        usernm_form.send_keys(self._username)
        passwd_form.send_keys(self._password)
        enter_field =  WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.NAME, 'ENTRAR')))
        self.driver.execute_script('arguments[0].click()', enter_field)
        
        # -- checker whether there is a dialog box to close before interacting with the page.
        try:
            dialog_div = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located(( By.XPATH, './/div[@class = "ui-dialog-buttonset"]' )))
            dialog_button = dialog_div.find_element(By.TAG_NAME, 'button')
            self.driver.execute_script('arguments[0].click()', dialog_button)
        except:
            pass
        
        # -- allow chaining
        return self
        
    def homepage(self):
        '''
            Return to the homepage from any page in the website.
        '''
        elements = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located(( By.XPATH, './/div[@id = "BoxConteudoPrincipal_migalha"]' ))).find_elements(By.TAG_NAME, 'a')
        for elem in elements:
            if 'principal' in elem.get_attribute('text').lower():
                self.driver.execute_script('arguments[0].click()', elem)
                break
                
        # -- checker whether there is a dialog box to close before interacting with the page.
        try:
            dialog_div = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located(( By.XPATH, './/div[@class = "ui-dialog-buttonset"]' )))
            dialog_button = dialog_div.find_element(By.TAG_NAME, 'button')
            self.driver.execute_script('arguments[0].click()', dialog_button)
        except:
            pass

        # -- allow chaining
        return self
    
    def locate_page(self):
        '''
            Return the absolute path of the current page from the homepage.
        '''
        elements = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located(( By.XPATH, './/div[@id = "BoxConteudoPrincipal_migalha"]' ))).find_elements(By.TAG_NAME, 'a')
        page_path = '\\'.join([ a.get_attribute('text').strip().replace('\n','').replace('\t','') for a in elements ])
        print(page_path)

    def close_browser(self):
        self.driver.close()
        self.driver = None
        
    # ------------------ data request and download ------------------
    
    def request_dbf(self, year, left_week, right_week, patient_data=True, requisition_export=None):
        '''
            Request the DBF file on notifications of severe acute respiratory syndrome. 
            The request is done considering a given year and an interval of epidemiological
            weeks. 

            Args:
            -----
                year:
                    String.
                left_week:
                    String.
                right_week:
                    String.
                patient_data:
                    Bool.
                export_requisition:
                    String. Filename. If not None, export the requisition number and the current time 
                    to the filename.
        '''
        # -- from main page, go to the page for requesting data
        self.homepage()
        
        a = WebDriverWait(self.driver, 20.0).until(EC.presence_of_all_elements_located((By.XPATH, './/a[@alt = "REGISTROS INDIVIDUAIS"]')))
        self.driver.execute_script('arguments[0].click()', a[1]) # The second is the one for the new database.
        
        # -- select hospitalized notifications
        selectElem = Select(WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/select[@name = "tipoFicha"]'))))
        selectElem.select_by_visible_text("SRAG Hospitalizado")
    
        # -- select radio box for epidemological year
        radio_epi = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/input[@name = "periodo:anoEpidemiologico"]')))
        self.driver.execute_script('arguments[0].click()', radio_epi)
        
        # -- checkbox for patient's data
        if patient_data:
            dados_pac_check = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/input[@name = "chkExportarDadosPaciente"]')))
            self.driver.execute_script('arguments[0].click()', dados_pac_check)
        
        # -- due to the particular behavior of the website, we fill up informatio next expecting for STALENESS
        input_ano = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/input[@name = "periodo:anoAnoEpidemiologico"]')))
        # -- now wait for staleness of forms (triggered by radio_epi)
        while True:
            try:
                dummy = input_ano.get_attribute("text")
            except StaleElementReferenceException:
                break
                
        # -- now we fill up period information without staleness
        input_ano = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/input[@name = "periodo:anoAnoEpidemiologico"]')))
        input_inicial = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/input[@name = "periodo:semanaInicial"]')))
        input_final = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/input[@name = "periodo:semanaFinal"]')))
        self.driver.execute_script('arguments[0].value=arguments[1];', input_ano, year)
        self.driver.execute_script('arguments[0].value=arguments[1];', input_inicial, left_week)
        self.driver.execute_script('arguments[0].value=arguments[1];', input_final, right_week)
        
        # -- request data and store requisition number
        gerar_dbf = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/input[@name = "gerarDbf"]')))
        self.driver.execute_script('arguments[0].click()', gerar_dbf)
        
        msg_success = WebDriverWait(self.driver, 60.0).until(EC.presence_of_element_located((By.XPATH, './/span[@class = "msgSUCCESS"]')))
        msgtext = BeautifulSoup(msg_success.get_attribute('outerHTML'), 'html.parser').get_text()
        self.requisition_number = msgtext.split(":")[1].strip().replace(".","")

        
        # Export requisition number, if necessary
        self.query = f'PERIOD:{year}/WEEK{left_week}/WEEK{right_week}'
        with open(requisition_export, 'a') as f:
            current_date = dt.datetime.today()
            csv_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow([self.requisition_number, self.query, f'DONE:{current_date.day:2.0f}/{current_date.month:2.0f}/{current_date.year}'.replace(' ', '0')])
        
        # -- allow chaining
        return self
        
    def query_file(self):
        '''
            Identify whether there is an available file for download matching the requisition number.
            It should be called after 'self.request_dbf' or chained to it.
        '''
        # -- from homepage, access the page to query the data (if there is no requisition just return to the homepage)
        self.homepage()
        if self.requisition_number is None:
            return self

        a = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located((By.XPATH, './/a[@alt = "CONSULTAR EXPORTAÇÕES DBF"]')))
        self.driver.execute_script('arguments[0].click()', a) # The second is the one for the new database.
        
        # -- extract the table info
        tableElem = WebDriverWait(self.driver, 20.0).until(EC.presence_of_element_located(( By.XPATH, './/table[@class = "TabelaResultado"]' )))
        tableHtml = BeautifulSoup(tableElem.get_attribute('outerHTML'), 'html.parser')
        
        records = []
        head, trows = tableHtml.find('thead'), tableHtml.find('tbody').find_all('tr')
        rename_cols = {hcell.attrs['id']: hcell.text.strip() for hcell in head.find_all('th')}
        for row in trows:
            record = { cell.attrs['headers'][0]: cell.text.replace("\n", "") for cell in row.find_all('td')  }
            records.append(record)
        self.requisition_table = pd.DataFrame(records).rename(rename_cols, axis=1)
        
        # -- allow chaining
        return self
    
    def download_file(self, sleep_time=2.5, verbose=False, max_loop=200):
        '''
            After a given data is requested, it waits for the file to be available and then
            performs the download.
            
            Args:
            -----
                sleep_time:
                    Float. Number of seconds to wait until the refresh of the page.
                verbose:
                    Bool. If True, provide a simple log for the waiting process.
                max_loop:
                    Integer. Maximum number of times to refresh the page.
        '''
        if self.requisition_number is None:
            raise Exception("No requisition number.")

        # -- wait until the file is available (or if a given number of refresh was done.)
        loop_n = 0
        while True:
            loop_n+=1 
            sub_tb = self.requisition_table[self.requisition_table["Número de Solicitação"]==self.requisition_number]
            if sub_tb.shape[0]==0:
                print('warning: no processing for the given request.')

            if verbose:
                print(f'waiting for file from requisition {self.requisition_number}, refresh {loop_n} ... ', end='')
            # -- if download is available, get the file and stop the wait.
            if (sub_tb.shape[0]==1 and sub_tb['Link'].iat[0]=='Download') or loop_n>max_loop:
                link_dbf = WebDriverWait(self.driver, 20.0).until(EC.presence_of_all_elements_located((By.XPATH, './/a[@class = "link"]')))
                # -- select the right download link in case there are more than one
                right_link = [ rlink for rlink in link_dbf if f'id={self.requisition_number}' in rlink.get_attribute('href') ]
                self.driver.execute_script('arguments[0].click()', right_link[0])
                self.downloaded = True
                if verbose: print('downloaded.')
                break
            else:
                if verbose: print('not yet.')
                time.sleep(sleep_time)
                self.query_file()

        return self
