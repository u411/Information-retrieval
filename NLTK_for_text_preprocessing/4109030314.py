#refer:https://machinelearningknowledge.ai/11-techniques-of-text-preprocessing-using-nltk-in-python/#Why_Text_Preprocessing_is_Needed 

import pandas as pd
df=pd.read_csv('all_annotated.tsv',sep='\t')
df_text=df[['Tweet']]
df_text.head()

#i) lowercasing
df_text['Tweet']=df_text['Tweet'].str.lower()
df_text.head()

#ii) remove extra whitespaces
def remove_whitespace(text):
    return  " ".join(text.split())

df_text['Tweet']=df['Tweet'].apply(remove_whitespace)

#iii) tokenization
#may need "pip install nltk"
from nltk import word_tokenize
import nltk
nltk.download('punkt')
df_text['Tweet']=df_text['Tweet'].apply(lambda X: word_tokenize(X))
df_text.head()

#iv) spelling correction
from spellchecker import SpellChecker
def spell_check(text):
    result = []
    spell = SpellChecker()
    for word in text:
        correct_word = spell.correction(word)
        result.append(correct_word)
        print("Running ..."+correct_word)
    
    return result

df_text['Tweet'] = df_text['Tweet'].apply(spell_check)
df_text.head()

#v) removing stopwords
from nltk.corpus import stopwords
print(stopwords.words('english'))
en_stopwords = stopwords.words('english')

def remove_stopwords(text):
    result = []
    for token in text:
        if token not in en_stopwords:
            result.append(token)
            
    return result

df_text['Tweet'] = df_text['Tweet'].apply(remove_stopwords)
df_text.head()

#vi) removing punctuations
from nltk.tokenize import RegexpTokenizer

def remove_punct(text):
    
    tokenizer = RegexpTokenizer(r"\w+")
    lst=tokenizer.tokenize(' '.join(text))
    return lst

df_text['Tweet'] = df_text['Tweet'].apply(remove_punct)
df_text.head()

#vii) removing frequent words
from nltk import FreqDist

def frequent_words(df):
    
    lst=[]
    for text in df.values:
        lst+=text[0]
    fdist=FreqDist(lst)
    return fdist.most_common(10)
frequent_words(df_text)

freq_words = frequent_words(df_text)

lst = []
for a,b in freq_words:
    lst.append(b)

def remove_freq_words(text):
    
    result=[]
    for item in text:
        if item not in lst:
            result.append(item)
    
    return result
    
df_text['Tweet']=df_text['Tweet'].apply(remove_freq_words)

#viii) lemmatization
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize,pos_tag

def lemmatization(text):
    
    result=[]
    wordnet = WordNetLemmatizer()
    for token,tag in pos_tag(text):
        pos=tag[0].lower()
        
        if pos not in ['a', 'r', 'n', 'v']:
            pos='n'
            
        result.append(wordnet.lemmatize(token,pos))
    
    return result

df_text['Tweet']=df_text['Tweet'].apply(lemmatization)
df_text.head()

#ix) stemming
from nltk.stem import PorterStemmer

def stemming(text):
    porter = PorterStemmer()
    
    result=[]
    for word in text:
        result.append(porter.stem(word))
    return result

df_text['Tweet']=df_text['Tweet'].apply(stemming)
df_text.head()

#x) removal of tags
import re
def remove_tag(text):
    
    text=' '.join(text)
    html_pattern = re.compile('<.*?>')
    return html_pattern.sub(r'', text)

df_text['Tweet'] = df_text['Tweet'].apply(remove_tag)
df_text.head()

#xi) removal of URLs
def remove_urls(text):
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    return url_pattern.sub(r'', text)

df_text['Tweet'] = df_text['Tweet'].apply(remove_urls)
df_text.head()
df_text.to_csv('processed_data.tsv', sep='\t', index=False)