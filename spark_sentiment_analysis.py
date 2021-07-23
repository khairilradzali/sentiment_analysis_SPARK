from textblob import TextBlob
from pyspark import SparkConf, SparkContext
import re



def abb_en(line):
   abbreviation_en = {
    'u': 'you',
    'thr': 'there',
    'asap': 'as soon as possible',
    'lv' : 'love',    
    'c' : 'see'
   }
   
   abbrev = ' '.join (abbreviation_en.get(word, word) for word in line.split())
   return (abbrev)

def remove_features(data_str):
   
    url_re = re.compile(r'https?://(www.)?\w+\.\w+(/\w+)*/?')    
    mention_re = re.compile(r'@|#(\w+)')  
    RT_re = re.compile(r'RT(\s+)')
    num_re = re.compile(r'(\d+)')
    
    data_str = str(data_str)
    data_str = RT_re.sub(' ', data_str)  
    data_str = data_str.lower()  
    data_str = url_re.sub(' ', data_str)   
    data_str = mention_re.sub(' ', data_str)  
    data_str = num_re.sub(' ', data_str)
    return data_str

def check_polarity(polarity):
    if polarity>0.0:
        polarityvalue="Positive"
    elif polarity<0.0:
        polarityvalue="Negative"
    else:
        polarityvalue="Neutral"
    return polarityvalue
  
   
#Write your main function here
def main(sc,filename):
    #take only rdd file
    rdd=sc.textFile(filename).map(lambda x:x.split(",")).filter(lambda x:len(x)==8).filter(lambda x:len(x[0])>1)
    
    #Extract only tweets
    tweets = rdd.map(lambda x:x[7]).map(lambda x:x.lower()).map(lambda x:abb_en(remove_features(x))).map(lambda x:TextBlob(x).sentiment.polarity).map(lambda x:check_polarity(x))
    
    #Combine two files
    combine=rdd.zip(tweets).map(lambda x:str(x).replace("'"," ")).map(lambda x:str(x).replace('"',' '))
    
    combine.saveAsTextFile("bitcoin_mock")
  
   
if __name__ == "__main__":
   conf = SparkConf().setMaster("local[1]").setAppName("Bitcoin Mock Test")
   sc = SparkContext(conf=conf)
   filename="bitcoin.csv"
   main(sc,filename)

   sc.stop()
