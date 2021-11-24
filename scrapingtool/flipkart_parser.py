import parser_interface
import re

class FlipkartParser(parser_interface.ParserInterface):
    def title(self):
        return self.soup.find("span", class_="B_NuCI").text

    def byline_info(self):
        pass

    def product_overview(self):
        pass

    def features(self):
        highlights_section = self.soup.find("div", class_="_2418kt")
        if highlights_section:
            [li.text.strip() for li in highlights_section.find_all("li")]

    def num_reviews(self):
        divs = self.soup.find_all("div", class_="row _2afbiS")
        if(len(divs) == 2):
            return divs[1].text.replace(" Reviews", "")

    def answered_questions(self):
        pass

    def curr_price(self):
        price_whole = soup.find("div", class_="_30jeq3")
        if price_whole:
            return price_whole.text[1:]

    def offers(self):
        pass

    def description(self):
        description_node = self.soup.find("div", class_="_2o-xpa")
        if description_node:
            return description_node.text

    def product_details(self):
        product_details={}
        spec_div = self.soup.find('div', class_="_3dtsli")

        if spec_div:
            rows = spec_div.find_all('tr')
            for row in rows:
                aux = row.findAll('td')
                key=aux[0].text.strip()
                value=aux[1].text.strip()
                product_details[key] = value
            return product_details
        else:
            details_div = self.soup.find("div", class_="X3BRps")
            if details_div:
                rows = details_div.find_all("div", class_="row")
                for row in rows:
                    aux = row.findAll("div")
                    key=aux[0].text.strip()
                    value=aux[1].text.strip()
                    product_details[key] = value
                return product_details

    def customer_qa_path(self):
        all_questions_link = self.soup.find("a", class_="dVBe_p")
        if all_questions_link:
            return all_questions_link.attrs['href']

    def review_histogram(self):
        ul = self.soup.find("ul", class_="_36LmXx")
        if ul:
            lis = ul.find_all("li")
            result = []
            i = 5
            for li in lis:
                star = i
                percent = li.text.replace(",", "")
                percent = int(percent)
                result.append({star: percent})
                i -= 1
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
        aspect_divs = self.soup.find_all("div", class_="_2a78PX")
        featurewise_reviews = {}
        for aspect_div in aspect_divs:
            divs = aspect_div.find_all("div")
            featurewise_reviews[divs[1].text] = divs[0].text

        return featurewise_reviews

    def reviews_url(self):
        # We construct the URL here instead of getting the href because
        # the hyperlink to see all reviews is not present for all products.

        rating_div = self.soup.find("div", class_="_3_L3jD")
        if rating_div:
            span = rating_div.find("span")
            if span:
                text = span.get("id")
                match = re.match(r'productRating_.*?_(.*?)_', text)
                if match and len(match.groups()) > 0:
                    pid = match.groups()[0]
                    return f"https://www.flipkart.com/x/product-reviews/x?pid={pid}"
