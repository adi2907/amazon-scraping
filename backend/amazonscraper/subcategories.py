# Here, all fields must belong to ProductDetails ('field' parameter)
subcategory_dict = {
    'headphones': {
        'Type': {
            'wired': {'field': 'product_title', 'predicate': lambda product_title: True if (product_title is not None and any(token in product_title.lower() for token in ['wired'])) else False},
            'wireless': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A1388921031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_feature_six_browse-bin%3A15564047031%7C15564048031%7C22749596031&dc&qid=1637309977&rnid=15564019031&ref=sr_nr_p_n_feature_six_browse-bin_4',
            'tws': {'field': 'product_title', 'predicate': lambda product_title: True if (product_title is not None and any(token in product_title.lower() for token in ['tws', 'true wireless', 'true-wireless', 'truly wireless', 'truly-wireless'])) else False},
        },
        'Price': {
            "<500": 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A1388921031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_36%3A-49900&dc&qid=1637310018&rnid=1318502031&ref=sr_nr_p_36_5',
            '500-1000': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A1388921031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_36%3A50000-99900&dc&qid=1637310045&rnid=1318502031&ref=sr_nr_p_36_1',
            '1000-2000':'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A1388921031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_36%3A100000-199900&dc&qid=1637310076&rnid=1318502031&ref=sr_nr_p_36_2',
            '2000-3000': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A1388921031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_36%3A200000-299900&dc&qid=1637310104&rnid=1318502031&ref=sr_nr_p_36_2',
            '3000-5000': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A1388921031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_36%3A300000-499900&dc&qid=1637310131&rnid=1318502031&ref=sr_nr_p_36_2',
            '>5000': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A1388921031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_36%3A500000-&dc&qid=1637310159&rnid=1318502031&ref=sr_nr_p_36_5',
        },
    },
    'smartphones': {
        'Price': {
            'budget (<10000)': 'https://www.amazon.in/s?k=smartphone&i=electronics&bbn=1805560031&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_36%3A400000-999900%2Cp_6%3AA14CZOWI0VEHLG%7CA1K6XQ7KUWCZYH%7CA23AODI1X2CEAE%7CAQUYM0O99MFUT&dc&qid=1637306580&rnid=1318502031&ref=sr_nr_p_36_1',
            'economy (10000-20000)': 'https://www.amazon.in/s?k=smartphone&i=electronics&bbn=1805560031&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%7CA1K6XQ7KUWCZYH%7CA23AODI1X2CEAE%7CAQUYM0O99MFUT%2Cp_36%3A1000000-1999900&dc&qid=1637306639&rnid=1318502031&ref=sr_nr_p_36_2',
            'mid premium (20000-30000)': 'https://www.amazon.in/s?k=smartphone&i=electronics&bbn=1805560031&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%7CA1K6XQ7KUWCZYH%7CA23AODI1X2CEAE%7CAQUYM0O99MFUT%2Cp_36%3A2000000-2999900&dc&qid=1637306687&rnid=1318502031&ref=sr_nr_p_36_5',
            'premium (>30000)': 'https://www.amazon.in/s?k=smartphone&i=electronics&bbn=1805560031&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA14CZOWI0VEHLG%7CA1K6XQ7KUWCZYH%7CA23AODI1X2CEAE%7CAQUYM0O99MFUT%2Cp_36%3A3000000-&dc&qid=1637306726&rnid=1318502031&ref=sr_nr_p_36_4',
        },
        'Features': {
            '5G': {'field': 'product_title', 'predicate': lambda product_title: True if (product_title is not None and '5g' in product_title.lower()) else False},
        }
    },
    'ceiling fan': {
        'Price': {
            'economy (<1500)': 'https://www.amazon.in/s?k=ceiling+fan&i=kitchen&bbn=4369221031&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_72%3A1318478031%2Cp_6%3AA1X5VLS1GXL2LN%7CA2EXL6MA8IE9DU%7CAH2BV6QWKVD69%7CAT95IG9ONZD7S%2Cp_36%3A-149900&dc&qid=1637310785&rnid=3444809031&ref=sr_nr_p_36_3',
            'standard (1500-2500)': 'https://www.amazon.in/s?k=ceiling+fan&i=kitchen&bbn=4369221031&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_6%3AA1X5VLS1GXL2LN%7CAT95IG9ONZD7S%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_36%3A150000-250000&dc&crid=1TGIH58I2LW9I&qid=1631171683&rnid=3444809031&sprefix=ceili%2Caps%2C380&ref=sr_nr_p_36_1',
            'premium (2500-4000)': 'https://www.amazon.in/s?k=ceiling+fan&i=kitchen&bbn=4369221031&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_72%3A1318478031%2Cp_6%3AA1X5VLS1GXL2LN%7CA2EXL6MA8IE9DU%7CAH2BV6QWKVD69%7CAT95IG9ONZD7S%2Cp_36%3A150000-249900&dc&qid=1637310877&rnid=3444809031&ref=sr_nr_p_36_2',
            'luxury (>4000)': 'https://www.amazon.in/s?k=ceiling+fan&i=kitchen&bbn=4369221031&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_72%3A1318478031%2Cp_6%3AA1X5VLS1GXL2LN%7CA2EXL6MA8IE9DU%7CAH2BV6QWKVD69%7CAT95IG9ONZD7S%2Cp_36%3A400000-&dc&qid=1637310919&rnid=3444809031&ref=sr_nr_p_36_3',
        },
        'Features': {
            'bldc': {'field': 'product_title', 'predicate': lambda product_title: True if (product_title is not None and 'bldc' in product_title.lower()) else False},
            'smart': {'field': 'features', 'predicate': lambda features: True if (features is not None and any(token in str(features).lower() for token in ['remote', 'bldc', 'smart', 'iot'])) else False},
            'lights': {'field': 'product_title', 'predicate': lambda product_title: True if (product_title is not None and any(token in product_title.lower() for token in ['light', 'lights', 'decorative'])) else False},
        }
    },
    'refrigerator': {
        'Capacity': {
            '<120': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480496031&dc&qid=1631171808&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_1',
            '120-200': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480497031&dc&qid=1631171864&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_2',
            '200-230': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480498031&dc&qid=1631171891&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_2',
            '230-300': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480499031&dc&qid=1631171912&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_4',
            '300-400': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480500031&dc&qid=1631171934&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_4',
            '>400': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480501031&dc&qid=1631171951&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_5',
        },
        'Door': {
            'multi door': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_thirteen_browse-bin%3A2753040031%7C2753045031&dc&qid=1631172184&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_7',
            'double door': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_thirteen_browse-bin%3A2753043031&dc&qid=1631172222&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_5',
            'single door': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_thirteen_browse-bin%3A2753044031&dc&qid=1631172241&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_7',
        },
        'Defrost': {
            'direct cool': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_eleven_browse-bin%3A2753030031&dc&qid=1631172731&rnid=2753029031&ref=sr_nr_p_n_feature_eleven_browse-bin_1',
            'frost free': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_eleven_browse-bin%3A2753031031&dc&qid=1631172764&rnid=2753029031&ref=sr_nr_p_n_feature_eleven_browse-bin_1',
        },
    },
    'washing machine': {
        'Automatic': {
            'semi automatic': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_sixteen_browse-bin%3A2753056031&dc&qid=1631172956&rnid=2753054031&ref=sr_nr_p_n_feature_sixteen_browse-bin_2',
            'fully automatic': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_sixteen_browse-bin%3A2753055031&dc&qid=1631172970&rnid=2753054031&ref=sr_nr_p_n_feature_sixteen_browse-bin_1',
        },
        'Loading': {
            'front load': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_fifteen_browse-bin%3A2753053031&dc&qid=1631173032&rnid=2753054031&ref=sr_nr_p_n_feature_sixteen_browse-bin_1',
            'top load': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_fifteen_browse-bin%3A2753052031&dc&qid=1631173038&rnid=2753051031&ref=sr_nr_p_n_feature_fifteen_browse-bin_2',
        },
        'Capacity': {
            '<7kg': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480508031%7C1480509031&dc&qid=1631173078&rnid=1480507031&ref=sr_nr_p_n_feature_seven_browse-bin_2',
            '7-8kg': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480510031&dc&qid=1631173108&rnid=1480507031&ref=sr_nr_p_n_feature_seven_browse-bin_2',
            '>=8kg': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S%2Cp_n_feature_seven_browse-bin%3A1480511031&dc&qid=1631173129&rnid=1480507031&ref=sr_nr_p_n_feature_seven_browse-bin_4',
        },
        'Price': {
            '<10000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and curr_price < 10000) else False},
            '10000-15000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and ((curr_price >= 10000) and (curr_price < 15000))) else False},
            '15000-20000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and ((curr_price >= 15000) and (curr_price < 20000))) else False},
            '20000-30000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and ((curr_price >= 20000) and (curr_price < 30000))) else False},
            '>30000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and curr_price >= 30000) else False},
        }
    },
}
