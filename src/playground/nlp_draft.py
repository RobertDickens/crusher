from nltk import sent_tokenize, word_tokenize
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.sentiment.sentiment_analyzer import SentimentAnalyzer


ps = PorterStemmer()

t = ['shit team', 'surprisingly good', 'i... am actually hopeful']

for w in t:
    print(SentimentAnalyzer(w).classify())
