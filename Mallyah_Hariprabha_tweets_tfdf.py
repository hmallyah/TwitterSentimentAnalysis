import MapReduce
import sys
import re
import string
from itertools import groupby

i = 0
mr = MapReduce.MapReduce()

def mapper(each_tweet):
    # Mapper code goes in here
    global i
    i += 1
    text = each_tweet["text"]
    each_tweet["tweet_no"] = i
    text = re.sub('https?:\\\/\\\/[^\s<>"]+|www\.[^\s<>"]+', '', text)
    text = re.sub('https?[^\s<>"]+', '', text)
    text = re.sub('rt[^a-zA-Z0-9]|R[Tt]|[Rr]etweet', '',text) #retweet
    text = re.sub('[@#][a-zA-Z0-9]*', '',text) #hashtags,prefixes
    text = text.lower()
    #text = text.decode(encoding='UTF-8',errors='strict')
    text = text.encode('utf-8').translate(None, string.punctuation)
    #print text
    for word in text.split():
        mr.emit_intermediate(word, each_tweet["tweet_no"])

def reducer(key, list_of_values):
    new_list_of_values = []
    tweet_no_tf = {}
    final_list =[]
    freq = [len(list(group)) for key1, group in groupby(list_of_values)]
    for i in list_of_values:
        if i not in new_list_of_values:
            new_list_of_values.append(i)
    #print new_list_of_values, freq
    tweet_no_tf = dict(zip(new_list_of_values,freq))
    #print key,tweet_no_tf
    word_df = len(tweet_no_tf)
    #print word_df
    for tweet_no, tf in tweet_no_tf.iteritems():
        temp = [tweet_no,tf]
        final_list.append(temp)
    mr.emit((key,word_df,final_list))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

