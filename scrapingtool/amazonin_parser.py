import parser_interface
import re

class AmazoninParser(parser_interface.ParserInterface):
    def title(self):
        center_col_node = self.soup.find("div", id="centerCol")
        if center_col_node is None:
            raise ValueError("Product Detail Not Found")

        self.center_col_node = center_col_node
        title = center_col_node.find("span", id="productTitle")

        if title:
            return title.text.strip()

    def byline_info(self):
        byline_info = self.center_col_node.find("a", id="bylineInfo")

        if byline_info:
            result = {}
            result['info'] = byline_info.text.strip()
            result['url'] = byline_info.attrs['href'] if 'href' in byline_info.attrs else None
            return result

    def product_overview(self):
        product_overview={}
        product_div = self.soup.find('div',{'id':'productOverview_feature_div'})
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
            return product_overview

    def features(self):
        feature_div = self.soup.find('div',{'id':'feature-bullets'})
        ul = feature_div.find('ul')

        if ul:
            feature_list = [li.text.strip() for li in ul.findAll('li')]
            return feature_list

    def num_reviews(self):
        num_reviews = self.center_col_node.find("span", id="acrCustomerReviewText")
        if num_reviews:
            return num_reviews.text.strip()

    def answered_questions(self):
        qanda_node = self.center_col_node.find("div", id="ask_feature_div")
        if qanda_node:
            node = qanda_node.find("a", id="askATFLink")
            return (node.span.text.strip() if node and hasattr(node, 'span') else None)

    def curr_price(self):
        price_node = self.center_col_node.find("span", id="priceblock_ourprice") or self.center_col_node.find("span", id="priceblock_dealprice")
        if price_node:
            # Encoding and then again decoding to remove the Rupee Symbol
            price = price_node.text.strip().encode('ascii', 'ignore').decode('utf-8')
            if price[0].isdigit() == False:
                return price[1:]
            else:
                return price

    def offers(self):
        offers_node = self.soup.find("div", id="sopp_feature_div")

        if offers_node:
            offers = []
            contents = offers_node.find_all(recursive=True)
            for content in contents:
                if hasattr(content, 'attrs') and 'href' in content.attrs and content.attrs['href'] == 'javascript:void(0)':
                    pass
                else:
                    if hasattr(content, 'text'):
                        offers.extend(filter(None, content.text.strip().split('\n')))
            return offers

    def description(self):
        description_node = self.soup.find("div", id="productDescription")
        if description_node:
            contents = description_node.find_all(recursive=True)
            description = []
            for content in contents:
                if hasattr(content, 'text'):
                    description.extend(filter(None, content.text.strip().split('\n')))
            return description

    def product_details(self):
        product_details = {}
        detail_node = self.soup.find("div",id="productDetails_feature_div")
        if detail_node is None:
            # Product Details
            detail_node = self.soup.find("div",id="detail-bullets_feature_div")
            if detail_node:
                ul = detail_node.find('ul')
                if ul:
                    lis = ul.findAll('li')
                    for row in lis:
                        th = row.find('span',{'class':'a-text-bold'})
                        td = th.find_next_sibling()
                        x=th.text.strip()
                        y=td.text.strip()
                        key=re.split(';|:|\n|&',x)[0].encode('ascii', 'ignore').decode('utf-8').strip()
                        value=re.split(';|:|\n|&',y)[0].encode('ascii', 'ignore').decode('utf-8').strip()
                        product_details[key] = value
                return product_details
        else:
            # Product Information - Technical details/Additional Information
            tables = detail_node.find_all('table')
            if tables:
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
                return product_details

    def customer_qa_path(self):
        customer_node = self.soup.find("div", class_="askWidgetQuestions askLiveSearchHide") or self.soup.find("div", class_="cdQuestionLazySeeAll")
        if customer_node:
            url = customer_node.find("a").attrs['href']
            return url

    def review_histogram(self):
        customer_reviews = self.soup.find("div", id="reviewsMedley")
        if customer_reviews:
            content = dict()
            # Capture the Histogram first
            histogram = customer_reviews.find("span", class_="cr-widget-Histogram")
            if histogram:
                result = []
                rows = histogram.find_all("tr", class_="a-histogram-row a-align-center")
                for row in rows:
                    star = row.find("td", class_="aok-nowrap")
                    star = star.span.a.text.strip() # x stars
                    percent = row.find("td", class_="a-text-right a-nowrap")
                    percent = percent.find("a", class_="a-link-normal").text.strip()
                    data = {star: percent}
                    result.append(data)
                return result

    def avg_rating(self):
        customer_reviews = self.soup.find("div", id="reviewsMedley")
        if customer_reviews:
            avg_rating = customer_reviews.find("span", {"data-hook": "rating-out-of-text"})
            if avg_rating:
                rating = None
                try:
                    avg_rating = avg_rating.text.strip().split()[0]
                    rating = float(avg_rating)
                except:
                    rating = None
                return rating

    def featurewise_reviews(self):
        customer_reviews = self.soup.find("div", id="reviewsMedley")
        if customer_reviews:
            featurewise_reviews = {}
            # Now we go to each featurewise review
            featurewise_reviews_node = customer_reviews.find("div", id= "cr-summarization-attributes-list")
            print(f"featurewise_reviews_node = {featurewise_reviews_node}")
            if featurewise_reviews_node:
                feature_nodes = featurewise_reviews_node.find_all("div", {"data-hook": "cr-summarization-attribute"})
                if feature_nodes:
                    for node in feature_nodes:
                        print(f"NODE")
                        span_nodes = node.find_all("span")
                        if span_nodes:
                            key=span_nodes[0].text.strip()
                            featurewise_reviews[key]=span_nodes[2].text.strip()
                    return featurewise_reviews

    def reviews_url(self):
        customer_reviews = self.soup.find("div", id="reviewsMedley")
        if customer_reviews:
            reviews = customer_reviews.find_all("div", id=re.compile(r'customer_review-.+'))
            if reviews:
                reviews_url = customer_reviews.find("div", id="reviews-medley-footer")
                if reviews_url:
                    reviews_url = reviews_url.find("a", {"data-hook": "see-all-reviews-link-foot"})
                    if reviews_url:
                        return reviews_url.attrs['href']
