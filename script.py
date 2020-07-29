import argparse
import os
import pickle
import random
import sqlite3
import sys
import time
from string import Template

import requests
from bs4 import BeautifulSoup

import parse_html
import queries

#from fake_useragent import UserAgent
#from selenium import webdriver
#from selenium.common.exceptions import NoSuchElementException
#from webdriver_manager.chrome import ChromeDriverManager


# ua = UserAgent()
# headers = {'User-Agent': str(ua.chrome)}

headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

url_template = Template('https://www.amazon.in/s?k=$category&ref=nb_sb_noss_2')

customer_reviews_template = Template('https://www.amazon.in/review/widgets/average-customer-review/popover/ref=acr_search__popover?ie=UTF8&asin=$PID&ref=acr_search__popover&contextId=search')


# Start the session
session = requests.Session()


# Dump Directory
if not os.path.exists(os.path.join(os.getcwd(), 'dumps')):
	os.mkdir(os.path.join(os.getcwd(), 'dumps'))


def scrape_category_listing(categories, num_pages=None):
	# session = requests.Session()

	if num_pages is None:
		num_pages = 10000 # Keeping a big number
	else:
		if not isinstance(num_pages, int) or num_pages <= 0:
			raise ValueError("num_pages must be a positive integer or None (for all pages)")

	server_url = 'https://amazon.in'
	response = session.get(server_url, headers=headers)
	assert response.status_code == 200
	cookies = dict(response.cookies)
	print(cookies)
	time.sleep(10)

	final_results = dict()

	for category in categories:
		final_results[category] = dict()
		base_url = url_template.substitute(category=category)
		
		response = session.get(base_url, headers=headers, cookies=cookies)
		assert response.status_code == 200
		if hasattr(response, 'cookies'):
			cookies = {**cookies, **dict(response.cookies)}
		
		time.sleep(5)
		curr_page = 1
		curr_url = base_url

		while curr_page <= num_pages:
			time.sleep(3)
			html = response.content
			soup = BeautifulSoup(html, 'html.parser')
			
			if not os.path.exists(os.path.join(os.getcwd(), 'data', f'{category}')):
				os.mkdir(os.path.join(os.getcwd(), 'data', f'{category}'))
				
			with open(os.path.join(os.getcwd(), 'data', f'{category}', f'page_{curr_page}.html'), 'wb') as f:
				f.write(html)
			
			# product_map = parse_html.get_product_mapping(soup)
			
			product_info = parse_html.get_product_info(soup)

			print(product_info)

			final_results[category][curr_page] = product_info

			# current_page = driver.find_element_by_xpath("//ul[@class='a-pagination']//li[@class='a-selected']/a")
			
			page_element = soup.find("ul", class_="a-pagination")
			
			if page_element is None:
				response = session.get(base_url, headers=headers, cookies=cookies)
				if hasattr(response, 'cookies'):
					cookies = {**cookies, **dict(response.cookies)}
				time.sleep(3)
				break
			
			next_page = page_element.find("li", class_="a-last")
			if next_page is None:
				response = session.get(base_url, headers=headers, cookies=cookies)
				if hasattr(response, 'cookies'):
					cookies = {**cookies, **dict(response.cookies)}
				time.sleep(3)
				break
			
			page_url = next_page.find("a").attrs['href']
			
			print(page_url)
			
			response = session.get(server_url + page_url, headers={**headers, 'referer': curr_url}, cookies=cookies)
			if hasattr(response, 'cookies'):
				cookies = {**cookies, **dict(response.cookies)}
			curr_url = server_url + page_url
			time.sleep(5)
			curr_page += 1
		
		# Dump the category results
		results = dict()
		results[category] = final_results[category]
		
		with open(f'dumps/{category}.pkl', 'wb') as f:
			pickle.dump(results, f)
		
		# Insert to the DB
		queries.create_tables('db.sqlite')
		with sqlite3.connect('db.sqlite') as conn:
			queries.insert_product_listing(conn, results)
		
		time.sleep(4)
	return final_results


def scrape_product_detail(category, product_url):
	# session = requests.Session()
	server_url = 'https://amazon.in'
	response = session.get(server_url, headers=headers)
	assert response.status_code == 200
	cookies = dict(response.cookies)
	time.sleep(3)

	response = session.get(server_url + product_url, headers=headers, cookies=cookies)
	if hasattr(response, 'cookies'):
		cookies = {**cookies, **dict(response.cookies)}
	time.sleep(10)

	final_results = dict()

	time.sleep(3)
	html = response.content
	
	if not os.path.exists(os.path.join(os.getcwd(), 'data', f'{category}')):
		os.mkdir(os.path.join(os.getcwd(), 'data', f'{category}'))
		
	product_id = parse_html.get_product_id(product_url)
	print(product_id)
	
	with open(os.path.join(os.getcwd(), 'data', f'{category}', f'{product_id}.html'), 'wb') as f:
		f.write(html)
	
	soup = BeautifulSoup(html, 'html.parser')

	# Get the product details
	details = parse_html.get_product_data(soup)
	details['product_id'] = product_id # Add the product ID
	
	# Check if the product is sponsored
	sponsored = parse_html.is_sponsored(product_url)

	# Insert to the DB
	queries.create_tables('db.sqlite')
	with sqlite3.connect('db.sqlite') as conn:
		queries.insert_product_details(conn, details, is_sponsored=sponsored)

	#with open(f'dumps/dump_{product_id}.pkl', 'wb') as f:
	#	pickle.dump(details, f)
	
	time.sleep(4)
	
	# Get the qanda for this product
	if 'customer_lazy' in details and details['customer_lazy'] == True:
		qanda_url = details['customer_qa']
		response = session.get(qanda_url, headers={**headers, 'referer': server_url + product_url}, cookies=cookies)
		if hasattr(response, 'cookies'):
			cookies = {**cookies, **dict(response.cookies)}
		assert response.status_code == 200
		time.sleep(5)
		html = response.content
		soup = BeautifulSoup(html, 'html.parser')
		qanda, next_url = parse_html.get_qanda(soup)
		
		# Insert to the DB
		queries.create_tables('db.sqlite')
		with sqlite3.connect('db.sqlite') as conn:
			queries.insert_product_qanda(conn, qanda, product_id=product_id)
		
		#with open(f'dumps/dump_{product_id}_qanda.pkl', 'wb') as f:
		#	pickle.dump(qanda, f)
		print("URL for qand a is " + next_url)
	
	# Get the customer reviews
	if 'customer_reviews' in details and 'reviews_url' in details['customer_reviews']:
		reviews_url = details['customer_reviews']['reviews_url']
		response = session.get(server_url + reviews_url, headers={**headers, 'referer': server_url + product_url}, cookies=cookies)
		if hasattr(response, 'cookies'):
			cookies = {**cookies, **dict(response.cookies)}
		assert response.status_code == 200
		time.sleep(5)
		html = response.content
		soup = BeautifulSoup(html, 'html.parser')
		reviews, next_url = parse_html.get_customer_reviews(soup)
		
		# Insert the reviews to the DB
		queries.create_tables('db.sqlite')
		with sqlite3.connect('db.sqlite') as conn:
			queries.insert_product_reviews(conn, reviews, product_id=product_id)
		
		#with open(f'dumps/dump_{product_id}_reviews.pkl', 'wb') as f:
		#	pickle.dump(reviews, f)
		print("URL for customer reviews is " + next_url)
	
	time.sleep(3)

	return final_results



if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-c', '--categories', help='List of all categories (comma separated)', type=lambda s: [item.strip() for item in s.split(',')])
	parser.add_argument('--listing', help='Scraping the category listing', default=False, action='store_true')
	parser.add_argument('--detail', help='Scraping individual product details', default=False, action='store_true')
	parser.add_argument('-n', '--number', help='Number of Individual Product Details per category to fetch', type=int, default=0)
	parser.add_argument('--pages', help='Number of pages to scrape the listing details per category', type=int, default=1)

	args = parser.parse_args()

	categories = args.categories
	listing = args.listing
	detail = args.detail
	num_items = args.number
	num_pages = args.pages
	
	if categories is not None:
		if listing == True:
			results = scrape_category_listing(categories, num_pages=num_pages)
			if detail == True:
				for category in categories:
					curr_item = 0
					curr_page = 1
					while curr_item < num_items:
						if curr_page in results[category]:
							for title in results[category][curr_page]:
								if results[category][curr_page][title]['product_url'] is not None:
									product_url = results[category][curr_page][title]['product_url']
									print(product_url)
									product_detail_results = scrape_product_detail(category, product_url)
									curr_item += 1
									if curr_item == num_items:
										break
						else:
							break
						curr_page += 1
		else:
			for category in categories:
				with open(f'dumps/{category}.pkl', 'rb') as f:
					results = pickle.load(f)
				curr_item = 0
				curr_page = 1
				while curr_item < num_items:
					for title in results[category][curr_page]:
						if results[category][curr_page][title]['product_url'] is not None:
							product_url = results[category][curr_page][title]['product_url']
							print(product_url)
							product_detail_results = scrape_product_detail(category, product_url)
							curr_item += 1
							if curr_item == num_items:
								break
					curr_page += 1

	#results = scrape_product_detail('headphones', '/Sony-WH-1000XM3-Wireless-Cancellation-Headphones/dp/B07HZ8JWCL/ref=sr_1_197?dchild=1&keywords=headphones&qid=1595772158&sr=8-197')
	#print(results)
