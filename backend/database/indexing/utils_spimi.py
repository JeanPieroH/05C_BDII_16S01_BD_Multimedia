import re
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from nltk.tokenize import word_tokenize

STOPWORDS = set(stopwords.words("english"))
STEMMER = SnowballStemmer("english")

def preprocess(text: str) -> list[str]:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)  # remove punctuation
    tokens = word_tokenize(text)
    filtered = [t for t in tokens if t.isalpha() and t not in STOPWORDS]
    stemmed = [STEMMER.stem(t) for t in filtered]
    return stemmed
