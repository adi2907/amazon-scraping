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

if len(sys.argv) >= 2:
	categories = [arg for arg in sys.argv[1:]]
else:
	raise ValueError(f'Run the program using: python {sys.argv[0]} CATEGORY1 CATEGORY2 ...')

url_template = Template('https://www.amazon.in/s?k=$category&ref=nb_sb_noss_2')

customer_reviews_template = Template('https://www.amazon.in/review/widgets/average-customer-review/popover/ref=acr_search__popover?ie=UTF8&asin=$PID&ref=acr_search__popover&contextId=search')


# Start the session
session = requests.Session()


# Dump Directory
if not os.path.exists(os.path.join(os.getcwd(), 'dumps')):
	os.mkdir(os.path.join(os.getcwd(), 'dumps'))


def scrape_category_listing(categories):
	# session = requests.Session()
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

		while curr_page < 3:
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
		
		time.sleep(5)
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
	
	# Insert to the DB
	queries.create_tables('db.sqlite')
	with sqlite3.connect('db.sqlite') as conn:
		queries.insert_product_details(conn, details)

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
	results = scrape_category_listing(categories)
	#with open('dump.pkl', 'wb') as f:
	#	pickle.dump(results, f)
	
	for category in categories:
		with open(f'dumps/{category}.pkl', 'rb') as f:
			results = pickle.load(f)

		for title in results[category][1]:
			if results[category][1][title]['product_url'] is not None:
				product_url = results[category][1][title]['product_url']
				print(product_url)
				results = scrape_product_detail(category, product_url)
				break
	#results = scrape_product_detail('headphones', '/Sony-WH-1000XM3-Wireless-Cancellation-Headphones/dp/B07HZ8JWCL/ref=sr_1_197?dchild=1&keywords=headphones&qid=1595772158&sr=8-197')
	#print(results)
