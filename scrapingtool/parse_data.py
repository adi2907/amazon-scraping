from itertools import product
import os
import pickle
import re
from datetime import datetime
import requests
import fnmatch
from bs4 import BeautifulSoup

from utils import create_logger
from decouple import config

import flipkart_parser
import amazonin_parser

logger = create_logger(__name__)


def init_parser(url:str):
    domain = config("DOMAIN")
    host = f"www.{domain}"
    headers = {"Accept-Encoding":"gzip, deflate, br", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5", "Connection":"keep-alive", "DNT": "1", "Host": host, "Upgrade-Insecure-Requests":"1", "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
    response = requests.get(url,headers=headers,timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text,'html.parser')
        return soup
    else:
        return None


def is_sponsored(url: str) -> bool:
    return url.startswith("/gp/slredirect")


def get_product_id(url):
    domain = config("DOMAIN")
    if domain == "amazon.in":
        if url.startswith("https://"):
            url.replace("https://www.amazon.in","")
        if url.startswith("/gp/slredirect"):
            # Only match until the first % (?)
            # Ignore the first 2 charactes (2F)
            pattern = r'/gp/slredirect/.+dp%..(.+?)%.+'
        else:
            pattern = r'/.+/dp/(.+)/.+$'
        match = re.match(pattern, url)
        product_id = None
        if match and len(match.groups()) > 0:
            product_id = match.groups()[0]
        return product_id

    elif domain == "flipkart.com":
        if url.startswith("https://"):
            url.replace("https://www.flipkart.com","")

        pattern = r'.*/p/(.*?)([/?].*|$)'
        match = re.match(pattern, url)
        product_id = None
        if match and len(match.groups()) > 0:
            product_id = match.groups()[0]
        return product_id


def get_product_mapping(soup) -> dict:
    product_info = soup.find_all("a", class_='a-link-normal a-text-normal')

    product_map = dict()

    for product_details in product_info:
        url = product_details['href']
        match = re.match(r'/.+/dp/(.+)/.+$', url)
        if match and len(match.groups()) > 0:
            product_id = match.groups()[0]
            product_map[product_id] =  product_details.text.strip()
    
    return product_map

# Returns total number of products for the listing and first listing in current page
def get_total_products_number(soup):
    # Gets response like "1-24 of 334 results for smartphones"
    result_info_bar = soup.find("div",class_="a-section a-spacing-small a-spacing-top-small").text
    
    #First number shouldn't exceed total number of products
    curr_page_listing = result_info_bar.split()[0].split('-')[0]
    
    # Extract the total number i.e 334 and convert to number
    total_number=re.findall("\s[0-9]+\s", result_info_bar)[0]
    print("Total number of products "+total_number)
    return int(total_number),int(curr_page_listing)


def get_product_info(soup):
    domain = config("DOMAIN")
    if domain == "amazon.in":
        return get_product_info_amazonin(soup)
    elif domain == "flipkart.com":
        return get_product_info_flipkart(soup)
    else:
        logger.critical(f"Don't know how to parse product info for domain: {domain}")


def get_product_info_amazonin(soup, base_url="https://www.amazon.in", curr_serial_no=1):
    """Fetches the product details for all the products for a single page
    :return: product_info{}{} of product details
    :curr_serial_no: Tracker of total products scraped till now, @param curr_serial_no + # of products on page
    """
    product_bars = soup.find_all("div", class_="sg-col-inner")
    
    product_info = dict()

    serial_no = curr_serial_no
    
    for product_bar in product_bars:
        title_node = product_bar.find("a", class_="a-link-normal a-text-normal")
        
        if title_node is None:
            # Invalid node. Leave this
            continue
        
        title = title_node.find("span").text.strip()
        
        if title in product_info:
            # We've already covered this product
            serial_no += 1
            continue
        
        product_info[title] = dict()

        if title_node is not None and 'href' in title_node.attrs:
            product_info[title]['product_url'] = title_node.attrs['href']
            product_info[title]['product_id'] = get_product_id(title_node.attrs['href'])
        else:
            product_info[title]['product_url'] = None
            product_info[title]['product_id'] = None
            logger.warning(f"Product url and id data missing for product {title}")
        
        rating_row = product_bar.find("div", class_="a-row a-size-small")
        if rating_row is not None:
            for idx, row in enumerate(rating_row.find_all("span")):
                if idx == 0:
                    avg_rating = row.attrs['aria-label'].strip()
                else:
                    if 'aria-label' in row.attrs:
                        total_ratings = row.attrs['aria-label'].strip()
        else:
            avg_rating, total_ratings = None, None
            logger.warning(f"Avg and total ratings missing for {title}")
        
        
        product_info[title]['avg_rating'] = avg_rating
        product_info[title]['total_ratings'] = total_ratings

        #symbol = product_bar.find("span", class_="a-price-symbol")
        price_whole = product_bar.find("span", class_="a-price-whole")
        price_fraction = product_bar.find("span", class_="a-price-fraction")

        if price_whole is not None:
            product_info[title]['price'] = price_whole.text.strip()
            if price_fraction is not None:
                try:
                    if product_info[title]['price'][0].isdigit() == False:
                        product_info[title]['price'] = product_info[title]['price'][1:]
                    price_fraction = price_fraction.text.strip()
                    if price_fraction[0] == '.':
                        price_fraction = price_fraction[1:]
                    if product_info[title]['price'][-1] == '.':
                        product_info[title]['price'] += price_fraction
                    else:
                        product_info[title]['price'] += '.' + price_fraction
                except:
                    if product_info[title]['price'][-1] == '.':
                        product_info[title]['price'] += '0'
                    else:
                        product_info[title]['price'] += '.0'
        else:
            product_info[title]['price'] = None
            logger.warning(f"Price missing for {title}")

        # If the price is reduced, get the old price as well
        old_price = product_bar.find("span", class_="a-price a-text-price")
        if old_price is not None:
            product_info[title]['old_price'] = old_price.find("span", class_="a-offscreen").text.strip()
            if product_info[title]['old_price'][0].isdigit() == False:
                product_info[title]['old_price'] = product_info[title]['old_price'][1:]
        else:
            product_info[title]['old_price'] = None
            logger.warning(f"Old Price missing for {title}")
        
        # Check if this item is currently deliverable using secondary pricing information
        secondary_information = product_bar.find("span", class_="a-color-price")
        if secondary_information is None:
            product_info[title]['secondary_information'] = None
        else:
            product_info[title]['secondary_information'] = secondary_information.text.strip()
        
        # Get the image information
        img_node = product_bar.find("img", class_="s-image")
        if img_node is not None:
            product_info[title]['image'] = img_node.attrs['src']
        else:
            product_info[title]['image'] = None
        
        product_info[title]['serial_no'] = serial_no
        serial_no += 1
        
    return product_info, serial_no

def get_brand_model_title_flipkart(soup, acc):
    attribute_trs = soup.find_all("tr", class_="_1s_Smc")

    acc["model"] = ""
    for tr in attribute_trs:
        attr = tr.find("td", class_="_1hKmbr")
        if "Model" in attr.text:
            acc["model"] = tr.find("li", class_="_21lJbe").text

    acc["brand"] = ""
    breadcrumb_links = soup.find_all("a", class_="_2whKao")
    for a in breadcrumb_links:
        href = a.get("href") or ""
        if "~brand" in href:
            pattern = r'.*/(.*?)~brand.*'
            match = re.match(pattern, href)
            if match and len(match.groups()) > 0:
                acc["brand"] = match.groups()[0]

    acc["title"] = soup.find("span", class_="B_NuCI").text


def get_product_info_flipkart(soup, base_url="https://www.amazon.in", curr_serial_no=1):
    """Fetches the product details for all the products for a single page
    :return: product_info{}{} of product details
    :curr_serial_no: Tracker of total products scraped till now, @param curr_serial_no + # of products on page
    """
    products = soup.find_all("div", class_="_4ddWXP")
    product_info = dict()
    serial_no = curr_serial_no

    for product in products:
        title_node = product.find("a", class_="s1Q9rs")

        if title_node is None:
            # Invalid node. Leave this
            continue

        title = title_node.get("title")

        if title in product_info:
            # We've already covered this product
            serial_no += 1
            continue

        product_info[title] = dict()

        if 'href' in title_node.attrs:
            product_info[title]['product_url'] = title_node.attrs['href']
            product_info[title]['product_id'] = get_product_id(title_node.attrs['href'])
        else:
            product_info[title]['product_url'] = None
            product_info[title]['product_id'] = None
            logger.warning(f"Product url and id data missing for product {title}")

        rating_row = product.find("div", class_="gUuXy- _2D5lwg")
        if rating_row is not None:
            avg_rating_div = rating_row.find(class_='_3LWZlK')
            avg_rating = avg_rating_div.text
            total_ratings_span = rating_row.find(class_='_2_R_DZ')
            total_ratings = total_ratings_span.text
            if total_ratings.startswith("("):
                total_ratings = total_ratings[1:-1] # Remove braces
        else:
            avg_rating, total_ratings = None, None
            logger.warning(f"Avg and total ratings missing for {title}")

        product_info[title]['avg_rating'] = avg_rating
        product_info[title]['total_ratings'] = total_ratings

        price_whole = product.find("div", class_="_30jeq3")

        if price_whole is not None:
            product_info[title]['price'] = price_whole.text[1:]
        else:
            product_info[title]['price'] = None
            logger.warning(f"Price missing for {title}")

        # If the price is reduced, get the old price as well
        old_price = product.find("div", class_="_3I9_wc")
        if old_price is not None:
            product_info[title]['old_price'] = old_price.text[1:]
        else:
            product_info[title]['old_price'] = None
            logger.warning(f"Old Price missing for {title}")

        # TODO: What's secondary_information in FK?
        product_info[title]['secondary_information'] = None

        # Get the image information
        img_node = product.find("img", class_="_396cs4 _3exPp9")
        if img_node is not None:
            product_info[title]['image'] = img_node.attrs['src']
        else:
            product_info[title]['image'] = None

        product_info[title]['serial_no'] = serial_no
        serial_no += 1

    return product_info, serial_no

def get_feature_review_summartization(product_id):
    
    headers = {
        'authority': 'www.amazon.in',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'rtt': '100',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'accept': 'text/html,*/*',
        'cache-control': 'no-cache',
        'x-requested-with': 'XMLHttpRequest',
        'downlink': '9.75',
        'ect': '4g',
        'origin': 'https://www.amazon.in',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }
    params = (
        ('asin', product_id),
        ('language', 'en_IN'),
        ('lazyWidget', 'cr-summarization-attributes'),
    )
    
    response = requests.get('https://www.amazon.in/hz/reviews-render/ajax/lazy-widgets/stream', headers=headers, params=params)
    
def get_review_elements(soup):
    target_class = "a-popover-trigger a-declarative"
    js_elements = soup.find_all("a", class_=target_class)
    return js_elements


def get_reviews(soup):
    target_id = "histogramTable"
    results = soup.find(id=target_id)
    reviews = results.find_all("span", class_="a-size-base")[1:]
    five_star_reviews = reviews[0]
    other_reviews = reviews[2::2]
    results = [five_star_reviews.text.strip()] + [review.text.strip() for review in other_reviews]
    return results

def get_product_data_amazon(soup):
    pass

def get_product_data_flipkart(soup):
    pass

def get_product_data_new(soup):
    parser = None
    domain = config("DOMAIN")

    if domain == "amazon.in":
        parser = amazonin_parser.AmazoninParser(soup)
    elif domain == "flipkart.com":
        parser = flipkart_parser.FlipkartParser(soup)
    else:
        raise Exception(f"No parser implemented for domain {domain}")

    result = {}
    result["title"] = parser.title()
    result["byline_info"] = parser.byline_info()
    result["product_overview"] = parser.product_overview()
    result["features"] = parser.features()
    result["num_reviews"] = parser.num_reviews()
    result["answered_questions"] = parser.answered_questions()
    result["curr_price"] = parser.curr_price()
    result["offers"] = parser.offers()
    result["description"] = parser.description()
    result["product_details"] = parser.product_details()
    result["customer_qa"] = parser.customer_qa_path()
    result["histogram"] = parser.review_histogram()
    result["avg_rating"] = parser.avg_rating()
    result["featurewise_reviews"] = parser.featurewise_reviews()
    result["reviews_url"] = parser.reviews_url()
    return result

def get_product_data(soup, html=None):
    """Scrapes the Individual product detail page for a particular product
    """
    results = dict()
    center_col_node = soup.find("div", id="centerCol")
    if center_col_node is None:
        if html is not None:
            logger.error(f"HTML -> {html}")
            logger.newline()
        raise ValueError("Product Detail Not Found")
    
    # Product Title
    title = center_col_node.find("span", id="productTitle")
    if title is None:
        results['product_title'] = None
    else:
        results['product_title'] = title.text.strip()
    
    # Byline Information (Brand, etc)
    byline_info = center_col_node.find("a", id="bylineInfo")
    if byline_info is None:
        results['byline_info'] = None
    else:
        results['byline_info'] = dict()
        results['byline_info']['info'] = byline_info.text.strip()
        results['byline_info']['url'] = byline_info.attrs['href'] if 'href' in byline_info.attrs else None
    
    # Product Overview - Key points to know
    product_overview={}
    product_div = soup.find('div',{'id':'productOverview_feature_div'})
    table = product_div.find('table')
    if table is not None:
        rows = table.find_all('tr')
        for row in rows:
            aux = row.findAll('td')
            x=aux[0].text.strip()
            y=aux[1].text.strip()
            key=re.split(';|:|\n|&',x)[0].encode('ascii', 'ignore').decode('utf-8').strip()
            value=re.split(';|:|\n|&',y)[0].encode('ascii', 'ignore').decode('utf-8').strip()
            product_overview[key] = value
        results['product_overview'] =  product_overview 
    else:
        results['product_overview'] =  None 
    
    # Feature list - Key Selling Points
    feature_div = soup.find('div',{'id':'feature-bullets'})
    ul = feature_div.find('ul')
    if ul is not None:
        feature_list = [li.text.strip() for li in ul.findAll('li')]
        results['features'] = feature_list
    else:
        results['features'] = None
        
    
    # Rating Information
    num_reviews = center_col_node.find("span", id="acrCustomerReviewText")
    if num_reviews is None:
        results['num_reviews'] = None
    else:
        results['num_reviews'] = num_reviews.text.strip()
    
    # Answered Questions
    qanda_node = center_col_node.find("div", id="ask_feature_div")
    if qanda_node is None:
        results['answered_questions'] = None
    else:
        node = qanda_node.find("a", id="askATFLink")
        results['answered_questions'] = node.span.text.strip() if node and hasattr(node, 'span') else None
    
    # Price
    price_node = center_col_node.find("span", id="priceblock_ourprice")
    if price_node is None:
        price_node = center_col_node.find("span", id="priceblock_dealprice")
        if price_node is None:
            results['curr_price'] = None
        else:
            # Encoding and then again decoding to remove the Rupee Symbol
            results['curr_price'] = price_node.text.strip().encode('ascii', 'ignore').decode('utf-8')
            if results['curr_price'][0].isdigit() == False:
                results['curr_price'] = results['curr_price'][1:]
    else:
        results['curr_price'] = price_node.text.strip().encode('ascii', 'ignore').decode('utf-8')
        if results['curr_price'][0].isdigit() == False:
            results['curr_price'] = results['curr_price'][1:]
    

    # Offers
    offers_node = soup.find("div", id="sopp_feature_div")
    if offers_node is None:
        results['offers'] = None
    else:
        offers = []
        contents = offers_node.find_all(recursive=True)
        for content in contents:
            if hasattr(content, 'attrs') and 'href' in content.attrs and content.attrs['href'] == 'javascript:void(0)':
                pass
            else:
                if hasattr(content, 'text'):
                    offers.extend(filter(None, content.text.strip().split('\n')))
        results['offers'] = offers
    
    # Description
    description_node = soup.find("div", id="productDescription")
    if description_node is None:
        results['description'] = None
    else:
        contents = description_node.find_all(recursive=True)
        description = []
        for content in contents:
            if hasattr(content, 'text'):
                description.extend(filter(None, content.text.strip().split('\n')))
        results['description'] = description
    
    
    # Get product Details
    product_details = {}
    detail_node = soup.find("div",id="productDetails_feature_div")
    if detail_node is None:
        # Product Details
        detail_node = soup.find("div",id="detail-bullets_feature_div")
        if detail_node is not None:
            ul = detail_node.find('ul')
            if ul is not None:
                lis = ul.findAll('li')
                for row in lis:
                    th = row.find('span',{'class':'a-text-bold'})
                    td = th.find_next_sibling()
                    x=th.text.strip()
                    y=td.text.strip()
                    key=re.split(';|:|\n|&',x)[0].encode('ascii', 'ignore').decode('utf-8').strip()
                    value=re.split(';|:|\n|&',y)[0].encode('ascii', 'ignore').decode('utf-8').strip()              
                    product_details[key] = value
            results['product_details'] =  product_details
        else:
            results['product_details'] = None
    else:
        # Product Information - Technical details/Additional Information
        tables = detail_node.find_all('table')
        if tables is not None:
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    x=th.text.strip()
                    y=td.text.strip()
                    key=re.split(';|:|\n|&',x)[0].encode('ascii', 'ignore').decode('utf-8').strip()
                    value=re.split(';|:|\n|&',y)[0].encode('ascii', 'ignore').decode('utf-8').strip()              
                    product_details[key] = value
            results['product_details'] = product_details
        else:
            results['product_details'] = None

    
    # Customer Q&A
    customer_node = soup.find("div", class_="askWidgetQuestions askLiveSearchHide")
    if customer_node is None:
        # Try to see if the lazy URL is loaded
        customer_node = soup.find("div", class_="cdQuestionLazySeeAll")
        if customer_node is not None:
            url = customer_node.find("a").attrs['href']
            results['customer_qa'] = url
        else:
            results['customer_qa'] = None
    else:
        # Let's fetch it now
        # TODO: See how we can implement this
        details = dict()
        results['customer_qa'] = details
    
    # Customer Reviews
    customer_reviews = soup.find("div", id="reviewsMedley")
    if customer_reviews is None:
        results['customer_reviews'] = None
    else:
        content = dict()
        # Capture the Histogram first
        histogram = customer_reviews.find("span", class_="cr-widget-Histogram")
        if histogram is not None:
            results['histogram'] = []
            rows = histogram.find_all("tr", class_="a-histogram-row a-align-center")
            for row in rows:
                star = row.find("td", class_="aok-nowrap")
                star = star.span.a.text.strip() # x stars
                percent = row.find("td", class_="a-text-right a-nowrap")
                percent = percent.find("a", class_="a-link-normal").text.strip()
                data = {star: percent}
                results['histogram'].append(data)
        else:
            results['histogram'] = None
        
        # Get the average rating
        avg_rating = customer_reviews.find("span", {"data-hook": "rating-out-of-text"})
        if avg_rating is not None:
            try:
                avg_rating = avg_rating.text.strip().split()[0]
                avg_rating = float(avg_rating)
            except:
                avg_rating = None
        
        results['avg_rating'] = avg_rating
            
        
        featurewise_reviews = {}
        # Now we go to each featurewise review
        featurewise_reviews_node = customer_reviews.find("div", id= "cr-summarization-attributes-list")
        if featurewise_reviews_node is not None:
            feature_nodes = featurewise_reviews_node.find_all("div", {"data-hook": "cr-summarization-attribute"})
            if feature_nodes is not None:
                for node in feature_nodes:
                    span_nodes = node.find_all("span")
                    if span_nodes is not None:
                        key=span_nodes[0].text.strip()
                        featurewise_reviews[key]=span_nodes[2].text.strip() 
                results[featurewise_reviews]=featurewise_reviews
            else:
                results['featurewise_reviews'] = None
        else:
            results['featurewise_reviews'] = None
        
        # Now capture the reviews URL
        reviews = customer_reviews.find_all("div", id=re.compile(r'customer_review-.+'))
        if reviews is None:
            content['reviews'] = None
        else:
            reviews_url = customer_reviews.find("div", id="reviews-medley-footer")
            if reviews_url is not None:
                reviews_url = reviews_url.find("a", {"data-hook": "see-all-reviews-link-foot"})
                if reviews_url is not None:
                    reviews_url = reviews_url.attrs['href']
            results['reviews_url'] = reviews_url

    return results


def get_qanda(soup, page_num=None):
    """Parses the QandA page for a particular product
    """
    results = []
    widgets = soup.find_all("div", class_="a-section askInlineWidget")
    if widgets is not None:
        for widget in widgets:
            pairs = widget.find_all("div", class_="a-fixed-left-grid-col a-col-right")
            if pairs is not None:
                for pair in pairs:
                    qanda = dict()
                    date = pair.find("span", class_="a-color-tertiary aok-align-center")
                    if date is not None and hasattr(date, 'text'):
                        date = date.text.strip()
                        # Convert it into a datetime object
                        try:
                            try:
                                date = datetime.strptime(date, '· %d %B, %Y')
                            except:
                                date = datetime.strptime(date, '· %B %d, %Y')
                        except ValueError:
                            if ',' in date:
                                try:
                                    date = datetime.strptime(date, '%d %B, %Y')
                                except:
                                    date = datetime.strptime(date, '%B %d, %Y')
                            else:
                                try:
                                    date = datetime.strptime(date, '%d %B %Y')
                                except:
                                    date = datetime.strptime(date, '%B %d %Y')
                    else:
                        date = None

                    pair = pair.find_all("div", class_="a-fixed-left-grid-col a-col-right")
                    if pair is None:
                        continue
                    if len(pair) != 2:
                        # We need to avoid the node which is repeated due to the recursive search
                        # So we must proceed alternatively
                        continue
                    question = pair[0].find("a").span.text.strip()
                    qanda['question'] = question
                    answer = pair[1].span.text.strip()
                    qanda['answer'] = answer
                    qanda['date'] = date
                    qanda['page_num'] = page_num
                    results.append(qanda)
    
    # Check for number of Q&A results in page
    if len(results) == 0:
        logger.error(f"Result not found for Q&A, possible captcha or javascript")

    # Get the url of the next page, if it exists
    page_node = soup.find("ul", class_="a-pagination")
    next_url = None
    if page_node is not None:
        next_url = page_node.find("li", class_="a-last")
        if next_url is not None:
            next_url = next_url.find("a")
            if next_url is not None:
                next_url = next_url.attrs['href']
                if len(results) < 10:
                    logger.error(f"Result partially found for Q&A, possible captcha or javascript")

    return results, next_url


def get_customer_reviews(soup, content={}):
    # Now capture the reviews
    reviews = soup.find_all("div", id=re.compile(r'customer_review-.+'))
    
    if reviews is None:
        content['reviews'] = None
    else:        
        reviews_url = soup.find("div", id="reviews-medley-footer")
        if reviews_url is not None:
            reviews_url = reviews_url.find("a", {"data-hook": "see-all-reviews-link-foot"})
            if reviews_url is not None:
                reviews_url = reviews_url.attrs['href']
        content['reviews_url'] = reviews_url
        
        review_data = []
        for review in reviews:
            data = dict()
            header = review.find("a", class_="a-link-normal")
            if header is None:
                continue
            
            # Rating of the review
            try:
                rating = header.attrs['title']
            except Exception as ex:
                # Maybe newer edition of amazon.com
                logger.critical(f"Possibly newer edition of amazon. Trying hook")
                rating = review.find("a", {"data-hook": "cmps-review-star-rating"})
                rating = rating.span.text.strip()
            
            data['rating'] = rating
            title = review.find("a", {"data-hook": "review-title"})
            if title is not None:
                data['title'] = title.span.text.strip()
            else:
                data['title'] = None
            
            # Review Date
            date = review.find("span", {"data-hook": "review-date"})
            if date is not None:
                date = date.text.strip()
                tokens = date.split()
                country_tokens = []
                date_tokens = []
                curr = 1
                for token in tokens[2:]:
                    curr += 1
                    if token in ['the', 'a']:
                        continue
                    if token in ['on']:
                        curr += 1
                        break
                    country_tokens.append(token)
                for token in tokens[curr:]:
                    date_tokens.append(token)
                country = ' '.join([token for token in country_tokens])   
                date = ' '.join([token for token in date_tokens])
                # Convert it into a datetime object
                if ',' in date:
                    try:
                        # India
                        date = datetime.strptime(date, '%d %B, %Y')
                    except:
                        # Usa
                        date = datetime.strptime(date, '%B %d, %Y')
                else:
                    try:
                        date = datetime.strptime(date, '%d %B %Y')
                    except:
                        date = datetime.strptime(date, '%B %d %Y')
            data['review_date'] = date
            data['country'] = country

            # Review about which product type
            product_info = review.find("a", {"data-hook": "format-strip"})
            if product_info is not None:
                info = []
                if hasattr(product_info, 'text'):
                    info.append(product_info.text.strip())
                product_info = product_info.find_all(recursive=True)
                for _content in product_info:
                    if hasattr(_content, 'text'):
                        info.append(_content.text.strip())
                data['product_info'] = info
            else:
                data['product_info'] = None
            
            # Is this a verified purchase?
            verified_purchase = review.find("span", {"data-hook": "avp-badge"})
            if verified_purchase is None:
                data['verified_purchase'] = False
            else:
                data['verified_purchase'] = True

            # Review Body
            # We need to convert all <br> tags to a newline, for identification
            regex = re.compile(r"<br/?>", re.IGNORECASE)
            body = review.find("div", {"data-hook": "review-collapsed"})
            if body is None:
                # We're at the main page, so it's no longer collapsed
                body = review.find("span", {"data-hook": "review-body"})
            if body is not None:
                body = re.sub(regex, '\n', str(body.span))
                body = body[6:-7] # Remove <span> and </span>
            data['body'] = body

            # Number of people who liked this review
            helpful_votes = review.find("span", {"data-hook": "helpful-vote-statement"})
            if helpful_votes is None:
                data['helpful_votes'] = helpful_votes
            else:
                value = helpful_votes.text.strip().split()[0].replace(',', '')
                try:
                    data['helpful_votes'] = int(value)
                except ValueError:
                    # Possible words like 'One', 'Two'
                    value = value.lower()
                    mapping = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9}
                    data['helpful_votes'] = mapping[value]

            review_data.append(data)
        content['reviews'] = review_data
    
    # Now get the next page url, if there is one
    next_url = None
    next_url = soup.find("ul", class_="a-pagination")
    if next_url is not None:
        next_url = next_url.find("li", class_="a-last")
        if next_url is not None:
            next_url = next_url.find("a")
            if next_url is not None:
                next_url = next_url.attrs['href']
    return content, next_url


if __name__ == '__main__':
    # Only details
    url = 'https://www.amazon.in/iQOO-Storage-Processor-FlashCharge-Replacement/dp/B07WHR5RKH/ref=sr_1_1?dchild=1&keywords=B07WHR5RKH&qid=1631897443&qsid=257-7977364-1168566&sr=8-1&sres=B07WHR5RKH&th=1#customerReviews'
    soup = init_parser(url)
    results = get_product_data(soup)
    print(results)
    # Only reviews
    
    # Only Q&A
