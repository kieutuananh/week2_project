from bs4 import BeautifulSoup
import requests
import sqlite3
from collections import deque
import pandas as pd


TIKI_URL = 'https://tiki.vn'

conn = sqlite3.connect('tiki.db') # connect with tiki database
cur = conn.cursor() # create a class call cursor so that we can use sqlite method on it


def create_categories_table():
    """Create table categories in database"""

    query = """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255),
            url TEXT, 
            parent_id INT, 
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)
create_categories_table()

#def select_all():
    """SELECT all from table categories and give it in a list"""
    return cur.execute('SELECT * FROM categories;').fetchall()

#def delete_all():
    """DELETE all from table catergores and give an []"""

    return cur.execute('DELETE FROM categories;')

class Category:
    def __init__(self, cat_id, name, url, parent_id):
        self.cat_id = cat_id
        self.name = name
        self.url = url
        self.parent_id = parent_id

    def __repr__(self):
        return "ID: {}, Name: {}, URL: {}, Parent_id: {}".format(self.cat_id, self.name, self.url, self.parent_id)

    def save_into_db(self):
        query = """
            INSERT INTO categories (name, url, parent_id)
            VALUES (?, ?, ?);
        """
        val = (self.name, self.url, self.parent_id)
        try:
            cur.execute(query, val)
            self.cat_id = cur.lastrowid
        except Exception as err:
            print('ERROR BY INSERT:', err)

def get_url(url):
    #time.sleep(1)
    try:
        response = requests.get(url).text
        response = BeautifulSoup(response, 'html.parser')
        return response
    except Exception as err:
            print('ERROR BY REQUEST:', err)

def get_main_categories(save_db=False):
    soup = get_url(TIKI_URL)

    result = []
    for a in soup.findAll('a', {'class':'MenuItem__MenuLink-tii3xq-1 efuIbv'}):
        cat_id = None
        name = a.find('span', {'class':'text'}).text
        url = a['href']
        parent_id = None

        cat = Category(cat_id, name, url, parent_id)
        if save_db:
            cat.save_into_db()
        result.append(cat)
    return result


def get_sub_categories(category, save_db=False):
    name = category.name
    url = category.url
    result = []

    try:
        soup = get_url(url)
        div_containers = soup.findAll('div', {'class':'list-group-item is-child'})
        for div in div_containers:
            sub_id = None
            sub_name = div.a.text
            sub_url = 'http://tiki.vn' + div.a['href']
            sub_parent_id = category.cat_id

            sub = Category(sub_id, sub_name, sub_url, sub_parent_id)
            if save_db:
                sub.save_into_db()
            result.append(sub)
    except Exception as err:
        print('ERROR BY GET SUB CATEGORIES:', err)

    return result

def get_all_categories(main_categories):
    de = deque(main_categories)
    count = 0

    while de:
        parent_cat = de.popleft()
        sub_cats = get_sub_categories(parent_cat, save_db=True)
        # print(sub_cats)
        de.extend(sub_cats)
        count += 1

        if count % 100 == 0:
            print(count, 'times')

conn.commit()

# Get the lowest layer of categorises
lowest_category = pd.read_sql_query('''SELECT a.* 
                      FROM categories as a 
                      LEFT JOIN categories as b 
                      ON a.id = b.parent_id
                      WHERE b.id IS NULL 
                      ;''', conn)


def crawl_tiki_product():
    global data
    data = []
    count = 0
    for i in range(len(lowest_categories.url)):
        soup = get_url((lowest_categories.url)[i])
        contents = soup.find_all('div', class_='product-item')
        parent_id = (lowest_categories.parent_id)[i]
        time.sleep(1)
        try
            for content in contents[:10]:
            count += 1
            d = {'image':'', 'title':'', 'final_price':'', 'link':'', 'reviews':'', 'stars':'','parent_id': ''}
            try:
                d['parent_id'] = parent_id
                d['image'] = content.find('img', class_="product-image")['src']
                d['title']= content.find('a', class_='')['title']
                d['final_price'] = int(content.find('span',class_='final-price').text.strip().split()[0].replace('đ','').replace('.',''))
                d['link'] = content.find('a', class_='')['href']
                d['reviews'] = int(content.find('p',class_='review').text.strip('()').replace(' nhận xét',''))
            except:
                d['reviews'] = 0
            try:
                s = content.find('span',class_='rating-content').span['style'] 
                st = s.replace('width:','').replace('%','')
                d['stars'] = round(int(st)/100*5,1)
            except:
                d['stars'] = 0
            data.append(d)
            if count % 10 == 0:
            print(count, 'times')
        except:
            pass

    return data


def create_products_table():
    """Create a product table in database"""
    query = """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            final_price REAl,
            image TEXT,
            link TEXT,
            reviews INT,
            stars REAL,
            parent_id INT,
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)
create_products_table()



def add_to_products():
    """ Add products infors in data to table products in database"""
  for i in range(len(data)):
    title = data[i]['title']
    final_price = float(data[i]['final_price'])
    image = data[i]['image']
    link = data[i]['link']
    reviews = int(data[i]['reviews'])
    stars = float(data[i]['stars'])
    parent_id = int(data[i]['parent_id'])
    query = """
            INSERT INTO products (title, final_price, image, link, reviews, stars, parent_id)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """
    val = (title, final_price, image, link, reviews, stars, parent_id)
    try:
      cur.execute(query, val)
    except Exception as err:
      print('ERROR BY INSERT:', err)
