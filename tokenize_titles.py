import nltk
from nltk.corpus import stopwords
from sqlalchemy.orm import sessionmaker
from textblob import TextBlob

import db_manager
from utils import create_logger

logger = create_logger('tokenize')

nltk.download('stopwords')


def remove_stop_words(text):
    blob = TextBlob(str(text)).words
    outputlist = []
    MAX_WORDS = 4
    
    for idx, word in enumerate(blob):
        if word in stopwords.words('english'):
            if idx <= 2:
                pass
            else:
                continue
        outputlist.append(word)
    
    return(' '.join(word.lower() for word in outputlist[:MAX_WORDS]))


def tokenize_titles():
    Session = sessionmaker(bind=db_manager.engine, autocommit=False, autoflush=True)

    session = Session()

    queryset = db_manager.query_table(session, 'ProductListing', 'all')

    for instance in queryset:
        title = instance.title
        
        if title is None:
            continue
        
        instance.short_title = remove_stop_words(title)
    
    try:
        session.commit()
        logger.info(f"Updated short titles!")
    except Exception as ex:
        session.rollback()
        logger.critical(f"Exception during inserting short_title: {ex}")


if __name__ == '__main__':
    tokenize_titles()
