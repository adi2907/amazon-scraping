# python-scraping

This is the repo for scraping online websites using python -list, category pages

**********

## Requirements

To install the requirements for this project, run the following:

```bash
pip install -r requirements.txt
```

### Setting up the Tor Relay Service

In order to use proxy services to switch identities periodically, you need to install the Tor service on your machine.

While the scraper will work even without the Tor service, it is highly recommended that you use it, in order to avoid getting an IP ban.

**Warning**: Don't misuse the rate of sending requests. It's possible that even with Tor, your real IP may get leaked. Proceed safely when scraping any website.

* Now, on your Linux machine, you can install tor via your package manager.

For example, in Ubuntu / Debian, you can install it using the below command:

```bash
sudo apt install tor
```

* You need to allow access to the Tor control port (9150). To do this, go to the configuration file `/etc/tor/torrc`

```bash
sudo vi /etc/tor/torrc
```

Now, you must uncomment the below lines

```bash
ControlPort 9051
## If you enable the controlport, be sure to enable one of these
## authentication methods, to prevent attackers from accessing it.
HashedControlPassword ABCDEFGHIJKLMNOP
CookieAuthentication 1
```

For the `HashedControlPassword`, you must get the hash using a password that you choose.

```bash
tor --hash-password "<YOUR-TOR-PASSWORD>"
```

Note down this password. We will need it later for authorising it via Python.

Get the hash from the output, and put it instead of `ABCDEFGHIJKLMNOP` in the `HashedControlPassword` option.


* Now, you can start the tor service using:

```bash
sudo service tor start
```

* To verify that Tor is working correctly, you must get the correct output when running this command:

```bash
curl --socks5 localhost:9050 --socks5-hostname localhost:9050 -s https://check.torproject.org/ | cat | grep -m 1 Congratulations | xargs
```

Now, go to your project directory (where the scraper is) and create a `.env` file. A sample template is given in the `.env.example` file.

```bash
touch .env
```

Put your Tor password in the below format:

```bash
TOR_PASSWORD = YOUR-TOR-PASSWORD
```

Finally, add the `ExitNodes` option to the `torrc`.

```bash
# ExitNodes Options
ExitNodes {IN}
```

This will mean that the Tor exit nodes will be an address in India. 

You have now setup the necessary requirements for running the scraper.

************

## Running the Scraper

After you've installed your dependencies, you can run the program in the following manner:

```bash
python scraper.py --categories "mobile, headphones" --listing --pages 1 --detail --number 2
```

Here, the `--categories` flag represents all the categories that you need to scrape. The program will scrape the take both the categories `["mobile", "headphone"]` and get the listing details for both these categories.

*NOTE*: By default, _all_ the reviews and QandAs for a product will be scraped. For more details, refer below.

If you only want to scrape the product listing, you can do it like this:

```bash
python scraper.py --categories "mobile, headphones" --listing --pages 2
```

This will scrape the first 2 pages for the product listing of mobiles and headphones.

The `--detail` flag is an optional flag if you want to fetch the product details.

To scrape only the product details, you can run it like this:

```bash
python scraper.py --categories "mobile, headphones" --details --number 2
```

This will fetch the first 2 product details for each category.

To scrape both the listing and details, you can run it like this:

```bash
python scraper.py --categories "mobile, headphones" --listing --pages 2 --detail --number 2
```

This will fetch the first 2 pages for product listing, and also scrape the first 2 products for each listing category.

## Scraping the QandA and Review Pages

The following options will restrict the scraper to scrape only until a certain number of pages, for the QandA, Reviews section.

```bash
python scraper.py --categories "headphones" --listing --pages 1 --detail --number 2 --review_pages 1 --qanda_pages 2
```

Here, the scraper will scrape 2 headphone products, and only fetch 1 review page. It will fetch the first 2 QandA pages.

## Scraping using IDs

You can also scrape using product IDs using the `--ids` option

```bash
python scraper.py --detail --ids "8172234988" --review_pages 3 --qanda_pages 3
```

## Scraping via a config file

To use a config file for setting up the scraper, using the `--config` option

```bash
# Listing
smartphones 50
refrigerator 20
earphones 100
ceiling fan 50
washing machine 20

# Detail
8172234988
8172234989
```

To run it, use:

```bash
python scraper.py --config "listing.conf"
```

***********