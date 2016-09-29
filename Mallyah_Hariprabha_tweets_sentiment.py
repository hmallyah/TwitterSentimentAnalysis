import sys
import string
import re
import MapReduce

mr = MapReduce.MapReduce()
scores = {}
i = 0

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
    text = text.encode('utf-8').translate(None, string.punctuation)
    text = text.lower()
    #print text
    for word in text.split():
        for term in scores:
            if word == term:
                mr.emit_intermediate(each_tweet["tweet_no"], scores[word])
            elif word != term:
                mr.emit_intermediate(each_tweet["tweet_no"], 0)

def reducer(key, list_of_values):
    # Reducer code goes in here
    tweet_score = 0
    for word_score in list_of_values:
        tweet_score += word_score
    mr.emit((key,float(tweet_score)))

if __name__ == '__main__':
    afinnfile = open(sys.argv[1])       # Make dictionary out of AFINN_111.txt file.
    for line in afinnfile:
        term, score  = line.split("\t")  # The file is tab-delimited. #\t means the tab character.
        scores[term] = int(score)  # Convert the score to an integer.
    tweet_data = open(sys.argv[2])
    mr.execute(tweet_data, mapper, reducer)


