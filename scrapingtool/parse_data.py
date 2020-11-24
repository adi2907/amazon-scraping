import os
import pickle
import re
from datetime import datetime

from bs4 import BeautifulSoup

from utils import create_logger

logger = create_logger(__name__)


def init_parser(category: str):
    if not os.path.exists(os.path.join(os.getcwd(), 'data', f'{category}.html')):
        raise ValueError(f'HTML file for category:{category} missing. Please make sure that it is downloaded')

    with open(os.path.join('data', f'{category}.html'), 'rb') as f:
        html_text = f.read()
        soup = BeautifulSoup(html_text, 'lxml')
    
    return soup


def is_sponsored(url: str) -> bool:
    return url.startswith("/gp/slredirect")


def get_product_id(url):
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


def get_product_info(soup, base_url="https://www.amazon.in", curr_serial_no=1):
    """Fetches the product details for all the products for a single page
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

        # If the price is reduced, get the old price as well
        old_price = product_bar.find("span", class_="a-price a-text-price")
        if old_price is not None:
            product_info[title]['old_price'] = old_price.find("span", class_="a-offscreen").text.strip()
            if product_info[title]['old_price'][0].isdigit() == False:
                product_info[title]['old_price'] = product_info[title]['old_price'][1:]
        else:
            product_info[title]['old_price'] = None
        
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
    
    # Feature Points
    feature_node = center_col_node.find("div", id="feature-bullets")
    if feature_node is None:
        results['features'] = None
    else:
        contents = feature_node.find_all(recursive=True)
        features = []
        for content in contents:
            if hasattr(content, 'text'):
                # Filter to remove the empty strings during string.split('\n)
                features.extend(filter(None, content.text.strip().split('\n')))
        results['features'] = features

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
    
    # Product Details
    detail_node = soup.find("div", id="prodDetails")
    if detail_node is None:
        results['product_details'] = None
    else:
        details = dict()
        headers = detail_node.find_all("div", class_="secHeader")
        flag = True
        if headers is not None:
            for header in headers:
                flag = False
                desc = header.text.strip()
                details[desc] = dict()
                # Per Table basis
                table = detail_node.find("table")
                if table is None:
                    continue
                labels, values = table.find_all("td", class_="label"), table.find_all("td", class_="value")
                if len(labels) != len(values):
                    continue
                for label, value in zip(labels, values):
                    _label, _value = label.text.strip(), value.text.strip()
                    details[desc][_label] = _value
        
        if flag == True:
            # For some categories, this may work
            # Ex: headphones
            tables = detail_node.find_all("table")
            titles = detail_node.find_all("h1")

            flag = True
            if tables is not None:
                for idx, table in enumerate(tables):
                    flag = False
                    try:
                        desc = titles[idx].text.strip()
                    except:
                        desc = f'Product Details {idx}'
                    details[desc] = dict()
                    # Per Table basis
                    labels, values = table.find_all("th"), table.find_all("td")
                    if len(labels) != len(values):
                        continue
                    for label, value in zip(labels, values):
                        _label, _value = label.text.strip(), value.text.strip()
                        details[desc][_label] = _value
            if flag == True:
                # Is Empty
                pass
            else:
                # Smartphones
                results['product_details'] = details
        else:
            # Smartphones
            results['product_details'] = details
    
    if detail_node is None:
        # Possible Empty. Ceiling Fan?
        detail_node = soup.find("div", id="detailBullets_feature_div")
        if detail_node is not None:
            details = dict()
            flag = True
            tables = detail_node.find_all("span", class_="a-list-item")
            desc = "Product Details"
            details[desc] = dict()
            for idx, detail in enumerate(tables):
                flag = False
                elements = detail.find_all("span")
                try:
                    label, value = elements[0].text.strip().replace("\n:", "").strip(), elements[1].text.strip()
                    details[desc][label] = value
                except:
                    pass
            
            if flag == True:
                # Is Empty
                pass

            results['product_details'] = details
    
    brand = None
    model = None
    try:
        if 'product_details' in results:
            if results['product_details'] not in (None, {}):
                # Get the brand and model
                key = 'Technical Details' if 'Technical Details' in results['product_details'] else 'Product Details'
                if key in results['product_details']:
                    if 'Brand' in results['product_details'][key]:
                        brand = results['product_details'][key]['Brand']
                    elif 'Brand Name' in results['product_details'][key]:
                        brand = results['product_details'][key]['Brand Name']
                    elif 'Manufacturer' in results['product_details'][key]:
                        brand = results['product_details'][key]['Manufacturer']
                    
                    if 'Model' in results['product_details'][key]:
                        model = results['product_details'][key]['Model']
                    elif 'Item model name' in results['product_details'][key]:
                        model = results['product_details'][key]['Item model name']
                else:
                    # Get it from byline_info
                    if 'byline_info' in results and 'info' in results['byline_info']:
                        brand = results['byline_info']['info']
                        if brand.startswith("Visit the "):
                            brand = brand.replace("Visit the ", "")
                            if brand.strip()[-1] == 'store':
                                brand = brand.replace(' store', '')
    except Exception as ex:
        print(ex)
    results['brand'] = brand
    results['model'] = model
    
    # Customer Q&A
    customer_node = soup.find("div", class_="askWidgetQuestions askLiveSearchHide")
    if customer_node is None:
        # Try to see if the lazy URL is loaded
        customer_node = soup.find("div", class_="cdQuestionLazySeeAll")
        if customer_node is not None:
            url = customer_node.find("a").attrs['href']
            results['customer_qa'] = url
            # Mark it as lazy so that we can fetch it later
            results['customer_lazy'] = True
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
        
        # Get the feature wise ratings
        attribute_widget = customer_reviews.find("div", {"data-hook": "summarization-attributes-widget"})
        if attribute_widget is not None:
            featurewise_reviews = dict()

            # Now we go to each featurewise review
            featurewise_reviews = attribute_widget.find("div", {"data-hook": "cr-summarization-attributes-list"})
            if featurewise_reviews is not None:
                nodes = featurewise_reviews.find_all("div", {"data-hook": "cr-summarization-attribute"})
                if nodes is not None:
                    featurewise_reviews = dict()
                    for node in nodes:
                        key = None
                        if hasattr(node, 'span') and hasattr(node.span, 'text'):
                            key = node.span.text.strip()
                        rating = node.find("span", class_="a-icon-alt")
                        if rating is not None:
                            rating = float(rating.text.strip())
                        else:
                            rating = 0.0
                        if key is not None:
                            featurewise_reviews[key] = rating
            
            results['featurewise_reviews'] = featurewise_reviews
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
    
    # Get the url of the next page, if it exists
    page_node = soup.find("ul", class_="a-pagination")
    next_url = None
    if page_node is not None:
        next_url = page_node.find("li", class_="a-last")
        if next_url is not None:
            next_url = next_url.find("a")
            if next_url is not None:
                next_url = next_url.attrs['href']

    return results, next_url


def get_customer_reviews(soup, content={}, page_num=None, first_request=False):
    # Now capture the reviews
    reviews = soup.find_all("div", id=re.compile(r'customer_review-.+'))
    num_reviews = None

    if first_request == True:
        # Get the number of reviews
        num_reviews = soup.find("span", {"data-hook": "cr-filter-info-review-count"})
        if num_reviews is not None:
            try:
                num_reviews = num_reviews.text.strip().split() # Showing, 1-5, of, 5, reviews
                try:
                    num_reviews = int(num_reviews[-2])
                except:
                    num_reviews = 100000
            except Exception as ex:
                print(ex)
    else:
        num_reviews = None

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

            data['page_num'] = page_num
            
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
    return content, next_url, num_reviews


if __name__ == '__main__':
    #soup = init_parser('sample')
    #results = get_reviews(soup)
    #print(results)
    
    #soup = init_parser('headphones/page_2')
    #results, _ = get_product_info(soup)
    #print(results)
    #print(len(results.keys()))

    #soup = init_parser('mobile/sample')
    soup = init_parser('haircolor_reviews')
    results = get_customer_reviews(soup, page_num=2, first_request=True)
    #results = get_product_info(soup)
    print(results)
    exit(0)
    #print(results['reviews_url'])
    page_element = soup.find("ul", class_="a-pagination")
    next_page = page_element.find("li", class_="a-last")
    page_url = next_page.find("a")
    page_url = page_url.attrs['href']
    print(page_url)
    exit(0)

    # Get the Product Data (Including Customer Reviews)
    #soup =  init_parser('headphones/B07HZ8JWCL')
    #results = get_product_data(soup)
    #print(results)

    # Get the customer reviews alone (https://www.amazon.in/Sony-WH-1000XM3-Wireless-Cancellation-Headphones/product-reviews/B07HZ8JWCL/ref=cm_cr_getr_d_paging_btm_prev_1?ie=UTF8&pageNumber=1&reviewerType=all_reviews)
    #soup = init_parser('headphones/reviews_B07HZ8JWCL')
    #results, next_url, num_reviews = get_customer_reviews(soup)
    #with open('dump_B07HZ8JWCL_reviews.pkl', 'wb') as f:
    #    pickle.dump(results, f)
    #print(results, next_url)

    # Get the QandA for this product alone (https://www.amazon.in/ask/questions/asin/B07HZ8JWCL/ref=cm_cd_dp_lla_ql_ll#nav-top)
    #soup = init_parser('headphones/qanda_B07HZ8JWCL')
    #results, next_url = get_qanda(soup)
    #print(results, next_url)

    soup = init_parser('listing')
    #soup = init_parser('detail')
    #results = get_product_data(soup)
    results, _ = get_product_info(soup)
    for title in results:
        product_url = results[title]['product_url']
        print(product_url)
