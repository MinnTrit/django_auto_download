from Uploader import upload_to_folder
from Saver import save_to_database
import asyncio
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta
import random 
from playwright.sync_api import sync_playwright 
import os 
import time 
import pandas as pd
from datetime import datetime
import pycountry
from Printer import Printer
import json
from django.conf import settings
import redis

redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True, db=0)

class Scrapper:
    def __init__(self, start_date, end_date, seller_ids_list, launcher):
        self.start_date = start_date
        self.end_date = end_date
        self.seller_ids_list = seller_ids_list
        self.launcher = launcher
        self.missing_days = None
        self.search_list = None
        self.working_list = None

    def initialize(self) -> None:
        #Initialize the missing_days list attribute 
        start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
        generated_dates = []
        current_date = start_date
        while current_date <= end_date:
            generated_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        self.missing_days = generated_dates

        #Initialize the search_list attribute
        current_directory = os.getcwd() 
        file_name = 'BP_dict.xlsx'
        file_path = os.path.join(current_directory, file_name) 
        df = pd.read_excel(file_path)
        seller_type_list = df['seller_type'].tolist()
        seller_used_id_list = df['seller_used_id'].tolist()
        seller_name_list = df['name'].tolist()
        result_list = []
        for index in range(len(seller_type_list)): 
            data_point = {
                'seller_type': seller_type_list[index],
                'seller_used_id': seller_used_id_list[index],
                'seller_name': seller_name_list[index]
            }
            result_list.append(data_point)
        self.search_list = result_list
        
        #Initialize the working_list
        working_list = []
        for seller_id in self.seller_ids_list:
            for iterated_dict in self.search_list: 
                iterated_seller_id = iterated_dict.get('seller_used_id', '')
                if iterated_seller_id == seller_id: 
                    seller_type = iterated_dict.get('seller_type', '')
                    seller_name = iterated_dict.get('seller_name', '')
                    search_dict = { 
                        'seller_id': iterated_seller_id, 
                        'seller_type': seller_type, 
                        'seller_name': seller_name
                    }
            working_list.append(search_dict)
        self.working_list = working_list
        
    @staticmethod
    def getting_calendar_year_month(month_string):
        month_string = month_string.strip().lower()
        full_month_name = month_string.split('-')[1]

        month_map = {
            'january': 1, 'feburary': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }

        month_number = month_map.get(full_month_name)

        if month_number:
            first_day_of_month = datetime(datetime.now().year, month_number, 1)
            return first_day_of_month
        else:
            return None

    @staticmethod
    def getting_country(seller_used_id): 
        country = seller_used_id.split('.')[0]
        country_object = pycountry.countries.get(alpha_2=country)
        if country_object: 
                country_name = country_object.name 
                if country_name == 'Viet Nam': 
                    country_name = 'Vietnam'
        return country_name

    @staticmethod
    def hovering_date(day_button, page): 
        while True: 
            try: 
                day_button.hover() 
                calendar_object = page.query_selector('div.oui-date-picker-menu.open')
                if calendar_object: 
                    return calendar_object
                elif not calendar_object:
                    continue
            except Exception: 
                continue 
    
    @staticmethod
    def box_validations(page): 
        validated_button = 0
        span_country_list = []
        span_channel_list = []
        span_store_list = []
        while True: 
            all_menus = page.query_selector_all('ul.my-cascader-menu')
            try: 
                country_li_elements = all_menus[0].query_selector_all('li')
                channel_li_elements = all_menus[1].query_selector_all('li')
                store_li_elements = all_menus[2].query_selector_all('li')
                break
            except Exception:
                continue  
        for li_element in country_li_elements: 
            span_elements = li_element.query_selector_all('span')[0].get_attribute('class')
            span_country_list.append(span_elements)
        for li_element in channel_li_elements: 
            span_elements = li_element.query_selector_all('span')[0].get_attribute('class')
            span_channel_list.append(span_elements)
        for li_element in store_li_elements: 
            span_elements = li_element.query_selector_all('span')[0].get_attribute('class')
            span_store_list.append(span_elements)   
        if span_country_list.count('my-cascader-checkbox my-cascader-checkbox-checked') == 1 or \
            span_country_list.count('my-cascader-checkbox my-cascader-checkbox-indeterminate') == 1: 
            validated_button += 1
        if span_channel_list.count('my-cascader-checkbox my-cascader-checkbox-checked') == 1 or \
            span_channel_list.count('my-cascader-checkbox my-cascader-checkbox-indeterminate') == 1:
            validated_button += 1
        if span_store_list.count('my-cascader-checkbox my-cascader-checkbox-checked') == 1 or \
            span_store_list.count('my-cascader-checkbox my-cascader-checkbox-indeterminate') == 1: 
            validated_button += 1
        return validated_button

    def run(self):
        download_dir = r"D:\Data Engineer Course\Django website\myproject\temporary"
        executable_path = os.getenv('CHROMIUM_PATH')
        application_file = 'chrome.exe'
        chromium_path = os.path.join(executable_path, application_file)

        with open('cookies.txt', 'r') as file: 
            file_contents = file.read()
        cookies_data = json.loads(file_contents)
        for data in cookies_data: 
            data['sameSite'] = "None"

        with sync_playwright() as pw:
            loop = asyncio.get_running_loop()
            main_df = pd.DataFrame()
            #Print the message to start the job 
            printer = Printer(launcher=self.launcher, missing_days=self.missing_days, seller_ids_list=self.seller_ids_list)
            loop.create_task(printer.run_discord_client())
            loop.create_task(printer.create_logs('launched'))
            loop.create_task(printer.send_launched_logs())
            print('Start connecting to web browser')
            browser =  pw.chromium.launch(executable_path=chromium_path, headless=True)
            context =  browser.new_context(
                viewport={
                    'width': 1300,
                    'height': 850
                }
            )
            context.add_cookies(cookies_data)
            page = context.new_page()

            printer.to_start_recording.set()
            loop.create_task(printer.start_recording())
            page.goto('https://ba.lazada.com')
            print(f'Connected to page {page}')
            while True:
                try: 
                    #Query used to check the for the agree services
                    checked_box = page.wait_for_selector('input.ant-checkbox-input')
                    if checked_box: 
                        checked_box.click()
                    aggree_button = page.query_selector('button.ant-btn.css-18w61nb.ant-btn-default.oScK1i span')
                    if aggree_button: 
                        aggree_button.click()
                        print('Closed terms of services tag')
                    
                    time.sleep(1)
                    div_close_button = page.query_selector_all('div.ant-modal-content')[1]
                    if div_close_button: 
                        close_button = div_close_button.query_selector('button')
                    if close_button:
                        close_button.click()
                        print('Closed avertising tag')
                        break
                except Exception as e:
                    print(f"An error occurred: {e}")
                    pass
            
            print('Start downloading files')
            for seller in self.working_list: 
                home_decision = random.randint(0, 1)

                #Navigate to the product section
                getting_product = page.wait_for_selector('span.ant-menu-title-content a.MOSRIN')
                if getting_product: 
                    a_elements = page.query_selector_all('span.ant-menu-title-content a.MOSRIN')
                    for a in a_elements: 
                        if 'product' in a.text_content().lower(): 
                            a.click() 

                #Navigate to the all stores section
                getting_store = page.wait_for_selector('div.CQQLcL.v6BP9E')
                if getting_store: 
                    getting_store.click()
                    time.sleep(1)

                #Check for the select all sections
                select_all_check = page.query_selector('div.ZKwJZ5 div span.my-checkbox.css-gkqwjw.my-checkbox-checked')
                if select_all_check is None: 
                    select_all_button = page.query_selector('div.ZKwJZ5 div span')
                    if select_all_button: 
                        select_all_button.click()
                        time.sleep(0.5)

                #Defining the selected countries
                getting_countries = page.wait_for_selector('ul.my-cascader-menu') 
                input_country = Scrapper.getting_country(seller.get('seller_id', ''))
                if getting_countries:   
                    all_countries = page.query_selector_all('ul.my-cascader-menu')[0]
                    if all_countries: 
                        li_items = all_countries.query_selector_all('li')
                        for li in li_items: 
                            if input_country == li.text_content().lower() or input_country == li.text_content(): 
                                selected_country = li.query_selector('div')
                                print(f'Selected country is {li.text_content()}')
                                print(f'Start working with seller {seller}')
                            else: 
                                span_elements = li.query_selector_all('span')[0]
                                if span_elements: 
                                    span_elements.click()
                                    # random_sleep = random.randint(0, 5)
                                    # time.sleep(random_sleep)                         
                
                #Refocusing on the country element 
                selected_country.hover()

                #Getting the appropripate channels
                getting_channels = page.wait_for_selector('ul.my-cascader-menu') 
                input_seller_type = seller.get('seller_type', '')
                if getting_channels:   
                    all_channels = page.query_selector_all('ul.my-cascader-menu')[1]
                    if all_channels: 
                        li_items = all_channels.query_selector_all('li')
                        for li in li_items: 
                            similar_ratio = fuzz.ratio(li.text_content().lower(), input_seller_type.lower())
                            if similar_ratio > 80: 
                                selected_channel = li
                            else:
                                span_elements = li.query_selector_all('span')[0]
                                if span_elements: 
                                    span_elements.click()
                                    # random_sleep = random.randint(0, 5)
                                    # time.sleep(random_sleep)                  
                
                #Refocus on the selected country
                selected_channel.hover()

                #Getting the appropriate seller name
                getting_seller_name = page.wait_for_selector('ul.my-cascader-menu') 
                input_seller_name = seller.get('seller_name', '')
                if getting_seller_name:   
                    all_seller_names = page.query_selector_all('ul.my-cascader-menu')[2]
                    if all_seller_names: 
                        li_items = all_seller_names.query_selector_all('li')
                        for li in li_items: 
                            brand_portal_word = li.text_content().lower()
                            seller_name_word = input_seller_name.lower()
                            if brand_portal_word == seller_name_word: 
                                selected_seller_name = li
                            else: 
                                span_elements = li.query_selector_all('span')[0]
                                if span_elements: 
                                    span_elements.click()
                                    # random_sleep = random.randint(0, 5)
                                    # time.sleep(random_sleep) 
                printed_flagged = False
                while True: 
                    total_buttons = Scrapper.box_validations(page)
                    if total_buttons == 3:
                        break 
                    else: 
                        placeholders = page.query_selector('div.my-select-dropdown.bl_ZY8.my-cascader-dropdown.css-gkqwjw.my-select-dropdown-placement-bottomLeft')
                        placeholders.hover() 
                        if printed_flagged == False: 
                            print(f'Clicked not expected at {total_buttons} buttons')
                            print(f'Failed clicking for seller {seller}') 
                            printed_flagged = True

                #Pressing the confirm button 
                confirm_button = page.wait_for_selector('button.my-btn.css-gkqwjw.my-btn-primary')
                if confirm_button: 
                    confirm_button.click()
                    time.sleep(1)
                
                #Start working with download the files
                print(f'Start downloading files for seller {seller.get('seller_id')}')
                for missing_day in self.missing_days: 
                    day_button = page.query_selector('button.ant-btn.css-18w61nb.ant-btn-primary.ant-btn-sm')
                    calendar_object = Scrapper.hovering_date(day_button, page)
                    if calendar_object: 
                        month_year_object = page.query_selector('div.oui-dt-calendar-content.day')
                        current_year = month_year_object.query_selector('span[data-role="current-year"]').text_content()
                        current_month = month_year_object.query_selector('span[data-role="current-month"]').text_content()
                        date_string = f'{current_year}-{current_month}'
                        calendar_day_object = Scrapper.getting_calendar_year_month(date_string)
                        missing_day_object = datetime.strptime(missing_day[:8] + '01', '%Y-%m-%d')
                        while True:
                            #When missing days lower than the calendar day, go backward
                            if missing_day_object < calendar_day_object:
                                previous_button = page.query_selector('span.oui-dt-calendar-control[data-role="prev-month"]')
                                previous_button.click()
                                time.sleep(1)
                                month_year_object = page.wait_for_selector('div.oui-dt-calendar-content.day')
                                current_year = month_year_object.query_selector('span[data-role="current-year"]').text_content()
                                current_month = month_year_object.query_selector('span[data-role="current-month"]').text_content()
                                date_string = f'{current_year}-{current_month}'
                                calendar_day_object = Scrapper.getting_calendar_year_month(date_string)
                            #When missing days lower than the calendar day, go forward
                            elif missing_day_object > calendar_day_object: 
                                next_button = page.query_selector('span.oui-dt-calendar-control[data-role="next-month"]')
                                next_button.click()
                                time.sleep(1)
                                month_year_object = page.wait_for_selector('div.oui-dt-calendar-content.day')
                                current_year = month_year_object.query_selector('span[data-role="current-year"]').text_content()
                                current_month = month_year_object.query_selector('span[data-role="current-month"]').text_content()
                                date_string = f'{current_year}-{current_month}'
                                calendar_day_object = Scrapper.getting_calendar_year_month(date_string)
                            #When finds the match day, then break
                            elif missing_day_object == calendar_day_object: 
                                break
                    
                    #Getitng all the days
                    all_rows_days = page.query_selector_all('tbody tr.oui-dt-calendar-date-column')
                    date_object = datetime.strptime(missing_day, '%Y-%m-%d')
                    for single_row in all_rows_days: 
                        all_days_in_row = single_row.query_selector_all('td.current-month')
                        for single_day in all_days_in_row: 
                            if single_day.text_content() == str(date_object.day): 
                                single_day.click() 
                                break                  
                    #Getting the export button
                    try: 
                        with page.expect_download() as download_info: 
                            export_button = page.query_selector('div.v4dEWt a')
                            if export_button: 
                                export_button.click()
                        download = download_info.value
                        current_time = datetime.now()
                        current_time = datetime.strftime(current_time, '%Y-%m-%d')
                        seller_id = seller.get('seller_id')
                        file_name = f'{seller_id}_{missing_day}_brand-portal-download_{self.launcher}_{current_time}.xls'
                        download_path = os.path.join(download_dir, file_name)
                        download.save_as(download_path)
                    except Exception as e:
                        print(f"Failed to download or save the file: {e}")        
                    if len(main_df) == 0:
                        main_df = pd.read_excel(download_path, skiprows=5)
                        main_df['Day'] = missing_day
                    else: 
                        bp_df = pd.read_excel(download_path, skiprows=5)
                        bp_df['Day'] = missing_day
                        main_df = pd.concat([main_df, bp_df], ignore_index=True)
                    try: 
                        upload_to_folder(download_path, file_name)
                    except Exception as e:
                        print(f"Error processing file: {e}")
                            
                #Decide to navigate back home or not
                if home_decision == 0: 
                    getting_home = page.wait_for_selector('span.ant-menu-title-content a.MOSRIN')
                    if getting_home:
                        a_elements = page.query_selector_all('span.ant-menu-title-content a.MOSRIN')
                        for a in a_elements: 
                            if 'home' in a.text_content().lower(): 
                                a.click() 
                                time.sleep(2)

            #Saved the full df to the database
            print('Start saving the data')
            save_to_database(main_df)
            
            printer.to_end_recording.set()
            loop.create_task(printer.end_recording(main_df))
            #Print the logs to finish the job
            loop.create_task(printer.create_logs(logs_type='outputs'))
            printer.to_send_output.set()
            loop.create_task(printer.send_output_logs())
