import json
import time
import logging
import datetime
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup

logging.basicConfig(filename='./logs/ebay-scraper.log', level=logging.INFO)

class Scraper:
    def __init__(self):
        self.ids_path = './data/scraped-ids.csv'
        self.scraped_ids = self.get_scraped_ids()
        self.scraped_products = []
        self.new_items_data = []
        self.run_timestamp = str(datetime.datetime.now())

    def get_scraped_ids(self):
        ids_df = pd.read_csv(self.ids_path)
        return set(list(map(str, ids_df['item-id'])))

    def scrape_item_page(self, item_div):
        item_link = item_div.select('.s-item__link')[0]
        item_url = item_link.get('href').split('?')[0]
        item_id = 'id-'+item_url.split('?')[0].split('/')[-1]
        if item_id not in self.scraped_ids:
            self.scraped_ids.add(item_id)
            self.new_items_data.append((item_id, item_url))
            item_name = item_link.text.replace('Opens in a new window or tab', '').replace('New Listing', '')
            item_listing_date = item_div.select('.s-item__listingDate')[0].text
            item_price = int(item_div.select('.s-item__price')[0].text.replace("$", ""))
            print()
            country_info = ""

            response = requests.get(item_url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                item_img_url = soup.select('.ux-image-carousel-item.active img')[0].get('src')
                item_seller_div = soup.select('.x-sellercard-atf__info__about-seller')[0]
                seller_name = item_seller_div.text
                seller_url = item_seller_div.select('a')[0].get('href')

                labels = soup.select('dt')
                values = soup.select('dd')
                for i in range(len(labels)):
                    label = labels[i].text
                    value = values[i].text
                    try:
                        if 'country' in label.lower() or 'region' in label.lower() or 'location' in label.lower() or 'place' in label.lower():
                            country_info += label + ': ' + value + '\n'
                    except Exception as e:
                        logging.error(e)
                        continue

                item_description = ""
                desc_url = soup.select('.d-item-description iframe')[0].get('src')
                desc_response = requests.get(desc_url)
                if desc_response.status_code == 200:
                    soup = BeautifulSoup(desc_response.text, 'html.parser')
                    item_description = soup.select('body')[0].text
            else:
                item_img_url, seller_name, seller_url, item_description = ("", "", "", "")    

            return {
                'item_id': item_id,
                'item_url': item_url,
                'item_name': item_name,
                'item_img_url': item_img_url,
                'item_description': item_description,
                'item_listing_date': item_listing_date,
                'item_seller_name': seller_name,
                'item_seller_url': seller_url,
                'item_price': item_price,
                'item_country_info': country_info
            }
        return None


    def scrape_search_page(self, url: str):
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            is_final_page = soup.select('.pagination__next')[0].get('href')
            is_existing_item = False
            item_div_list = soup.select('.s-item__wrapper')
            for item_div in tqdm(item_div_list[2:], ncols=100):
                item_data = self.scrape_item_page(item_div)
                if item_data:
                    self.scraped_products.append(item_data)
                else:
                    is_existing_item = True
            return {
                'status': response.status_code,
                'existing_item': is_existing_item,
                'final_page': is_final_page is None
            }

        return {
            'status' : response.status_code,
            'final_page': None
        }    


    def export_to_excel(self):
        output_path = f'./output/ebay-run-{self.run_timestamp[:self.run_timestamp.index(".")].replace(" ", "__")}.xlsx'
        df = pd.DataFrame(self.scraped_products)
        df.to_excel(output_path, index=False)
        logging.info(f"{len(self.scraped_products)} items data exported to => {output_path}")


    def update_scraped_item_ids(self):
        ids_df = pd.read_csv(self.ids_path)
        data = {
            'date-scraped': [datetime.date.today().strftime("%d-%m-%Y")] * len(self.new_items_data),
            'item-id': [str(data[0]) for data in self.new_items_data],
            'item-url': [data[1] for data in self.new_items_data]
        }
        new_data_df = pd.DataFrame(data)
        ids_df = pd.concat([ids_df, new_data_df], ignore_index=True)
        ids_df.to_csv(self.ids_path, index=False)

    def start(self):
        page_num = 1
        while True:
            logging.info(f'--------------{datetime.datetime.now()}-----------------')
            logging.info(f'=> Searching page = {page_num}')
            search_url = f'https://www.ebay.com/sch/260/i.html?_from=R40&_nkw=rare+covers&_udlo=100&_sop=10&_ipg=240&_pgn={page_num}&rt=nc'
            search_res = self.scrape_search_page(search_url)
            if search_res['final_page'] or search_res['existing_item']:
                break
            page_num += 1

        self.update_scraped_item_ids()

        if len(self.scraped_products) > 0:
            self.export_to_excel()
        else:
            logging.info('=> No new items found in search!')
        return self.scraped_products

if __name__ == '__main__':
    scraper = Scraper()
    result = scraper.start()
    print(json.dumps(result))