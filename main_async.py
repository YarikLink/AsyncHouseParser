import json
import time
from urllib import response
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import dateparser
import psycopg2
from psycopg2.extensions import AsIs
import warnings
from datetime import datetime



timestrings = ['1 years 2 days ago' ,'3 hours 4 mins ago','5 mins 6 secs ago']

warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)




conn = psycopg2.connect(
    database="house_parser",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5433"
)
conn.autocommit = True
cur = conn.cursor()
current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
table_name = f"houses_{current_time}"

try:
    with conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {table_name}( id serial PRIMARY KEY, ad_title varchar(250) NOT NULL, ad_date  varchar(150) NOT NULL, ad_city varchar(250) NOT NULL, ad_beds varchar(250) NOT NULL, ad_description varchar(900) NOT NULL, ad_price varchar(200) NOT NULL);"
            )
except:
    print('Table houses already exists')



start_time = time.time()



async def get_page_data(session, page):
    url = f"https://www.kijiji.ca/b-apartments-condos/city-of-toronto/page-{page}/c37l1700273"
    
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
    }

    async with session.get(url=url, headers=headers) as response:
        response_text = await response.text()

        soup = BeautifulSoup(response_text, "lxml")
        city_houses = soup.find_all("div", class_=["container-results", "large-images"])[1].find_all("div", class_=["search-item", "top-feature"])
        for items in city_houses:
            clear_data = items.find_all("div", class_="clearfix") 
            try:
                ad_title = clear_data[0].find("a", class_="title").text.strip()
            except:
                ad_title = "Ad without title"

            try:
                date = clear_data[0].find("span", class_="date-posted").text.strip()
                ad_date = dateparser.parse(date).strftime("%d-%m-%Y")
            except:
                ad_date = "Ad without date"

            try:
                ad_city = clear_data[0].find("div", class_="location").find("span").text.strip()
            except:
                ad_city = "Ad without city"

            try:
                ad_beds = " ".join(clear_data[0].find("span", class_="bedrooms").text.split())
            except:
                ad_beds = "Ad without beds"

            try:
                ad_description = " ".join(clear_data[0].find("div", class_="description").text.split())
            except:
                ad_description = "Ad without description"

            try:
                ad_price = clear_data[0].find("div", class_="price").text.strip()
            except:
                ad_price = "Ad without price"

            houses_dict =  {
                    "ad_title": ad_title,
                    "ad_date": ad_date,
                    "ad_city": ad_city,
                    "ad_beds": ad_beds,
                    "ad_description": ad_description,
                    "ad_price": ad_price
                }

            columns = houses_dict.keys()
            values = [houses_dict[column] for column in columns]
            insert_statement = f'insert into {table_name} (%s) values %s ON CONFLICT DO NOTHING'
            cur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))
            print(f"Страница обработана")


async def check_data():
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
    }
    root_url = f"https://www.kijiji.ca/b-apartments-condos/city-of-toronto/c37l1700273"
    async with aiohttp.ClientSession() as session:
        response = await session.get(url=root_url, headers=headers)
        soup = BeautifulSoup(await response.text(), "lxml")
        pages_count = soup.find("div", class_="pagination").find_all("a")[-3].text
        data = []
        for page in range(1, int(pages_count) + 1):
            task = asyncio.create_task(get_page_data(session, page))
            data.append(task)
        await asyncio.gather(*data)    
   

def main():
    asyncio.run(check_data())
    finish_time = time.time() - start_time
    print("Затрачено на работу ", finish_time)
    


if __name__ == "__main__":
    main()
