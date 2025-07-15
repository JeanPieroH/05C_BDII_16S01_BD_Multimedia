import re
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from nltk.tokenize import word_tokenize

STOPWORDS = set(stopwords.words("english"))
STEMMER = SnowballStemmer("english")

def check_nltk_punkt():
    """
    Verifica si el recurso 'punkt' está disponible.
    Si no está, lo descarga automáticamente.
    """
    import nltk
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        print("Descargando 'punkt' de NLTK...")
        nltk.download('punkt')
    try:
        nltk.data.find('tokenizers/punkt_tab')
    except LookupError:
        print("Descargando 'punkt_tab' de NLTK...")
        nltk.download('punkt_tab')

def preprocess(text: str) -> list[str]:
    check_nltk_punkt()
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)  # remove punctuation
    tokens = word_tokenize(text)
    filtered = [t for t in tokens if t.isalpha() and t not in STOPWORDS]
    stemmed = [STEMMER.stem(t) for t in filtered]
    return stemmed
