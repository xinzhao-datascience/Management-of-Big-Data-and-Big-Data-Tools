from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import * 
from pyspark.sql import Row

import sys
import argparse

# custom argument input 
parser = argparse.ArgumentParser(description = "DS8003 Final Exam" ) 
# starts with "-w", the program takes in user's search word
parser.add_argument('-w', '--word',type = str,  help = "Words need to searched, use , to separate")
# starts with "-n", the program takes in top n tf-idf score 
parser.add_argument('-n', '--number',type = int , help = "top n document")
# custom argument input 
args = parser.parse_args()


def tf_idf_dataframe(sc,sqlContext):
	# sores all documents from the document path
	data = sc.wholeTextFiles("/user/root/xin_zhao/final_exam/football")
	# count the number of document
	num_docs = data.count()
	# change all text to lower case and spilt each word by space 
	temp = data.map(lambda docs:(docs[0], docs[1].lower().split(" ")))
	# create dataframe for each document with two columns file and text
	df = sqlContext.createDataFrame(temp,['file','text'])
	# each word in each document (tf) 
	tf = df.withColumn('text', explode(col('text'))).groupBy('file', 'text').count()
	# find each distinct word in all documents
	wc = df.withColumn("text",explode(col('text')))
	# find each distinct word in how many documents
	distinct_word = wc.distinct().groupBy('text').count()
	# calculate idf by using the formula
	idf = distinct_word.withColumn('idf', log10(num_docs/ (col('count')))).drop('count')
	# join tf dataframe with idf dataframe to calculate tf_idf
	new_dataframe=tf.join(idf, tf.text==idf.text,'outer').drop(idf.text)
	# calculate tf_idf by multiplying tf with idf
	tf_idf = new_dataframe.withColumn('tf_idf', (col('idf')* (col('count'))))
	# order tf_idf scores with descending order
	sorted_tf_idf=tf_idf.orderBy('tf_idf',ascending=False)
	# save tf_idf as csv format
	sorted_tf_idf.write.csv("/user/root/xin_zhao/final_exam/tf_idf")
	return(sorted_tf_idf)
	
def search_engine(sc,tf_idf_index,query,n):
	# change query words to lower case and split each word with space
	words = query.lower().split(",")
	# get the length of the word
	query_length = len(words)
	# parallelize words into RDD format
	query_rdd = sc.parallelize(words).map(lambda x: Row(x))
	#covert query to dataframe format
	query_dataframe = sqlContext.createDataFrame(query_rdd,['text'])
	# trim query without unnecessary white space
	trim_dataframe = query_dataframe.withColumn('text',trim(col('text')))
	# search query within the documents (tf)
	search_dataframe= tf_idf_index.join(trim_dataframe, tf_idf_index.text==trim_dataframe.text,'inner').drop(trim_dataframe.text)
	# combine tf_idf scores of each query word 
	sum_tf_idf = search_dataframe.groupBy("file").agg({"*":"count", "tf_idf":"sum"})
	# change column names
	sum_tf_idf = sum_tf_idf.withColumnRenamed("count(1)", "num_match_word").withColumnRenamed("sum(tf_idf)", "sum_score")	
	#  calculate final tf_idf score for the document by using sum_score times number of matched words and divide by total number of query words 
	final_result = sum_tf_idf.select(sum_tf_idf.file, (sum_tf_idf.sum_score * sum_tf_idf.num_match_word) / query_length)
	# change column names	
	final_result = final_result.withColumnRenamed("((sum_score * num_match_word) / " + str(query_length) + ")", "tf_idf")
	# sort tf_idf scores with descending order, and output n result
	final_result = final_result.orderBy("tf_idf", ascending=False)
	final_result.show(n,False)

	
def main(sc):
	# store user input as query word
	search_word = args.word
	# store user input as top n document 
	topn = args.number
	# build tf_idf index
	tf_idf = tf_idf_dataframe(sc,sqlContext)
	# search query and return tf_idf
	search_engine(sc,tf_idf,search_word,topn)

if __name__  == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)
    main(sc)
    sc.stop()



#spark-submit --master yarn-client --executor-memory 512m --num-executors 5 --executor-cores 1 --driver-memory 512m finalexam_final.py
