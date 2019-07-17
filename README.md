**Summary:**

- This search query program will take a certain words separated by &quot;,&quot;  and retrieve the top N matching document with the word being search, how many word occurrence in the document, idf score, and tf-idf score
- This program is developed with PySpark, SparkSQL and Dataframes
- This program in not case sensitive, therefore user can enter the word in both uppercase and lowercase
- This program trims all unnecessary white space of user input


The tf-idf inverted index is built on the dataset from BBC sports website. The dataset consists of a total of 265 text files which contains football sports news.

**Program Insruction:**

In order to run the python file , the program takes 2 parameters from the user. The first parameter starts with &quot;-w&quot;, the program will take in the words that need to be search in the documents, user &quot;,&quot; to separate between words. The second parameter starts with &quot;-n&quot; which will take in how many result to output according to the tf-idf score.

**Functions Description:**

_tf\_idf\_dataframe(sc,sqlContext):_

This function will create a store the dataset in RDD format first, it counts the number of documents in the dataset, and clean the data by converting all letters to lower case and split each word by space. Then the dataset is converted to a dataframe. The program will calculate term frequency of each word, find distinct words in how many documents and calculate idf. In order to calculate tf\_idf, tf dataframe is joined with idf dataframe, then tf\_idf is calculated by multiplying tf with idf. In the end the tf\_idf is sorted with descending order. The tf\_idf index is saved as csv format

_search\_engine(sc,tf\_idf\_index,query,n):_

This function takes in the tf idf index created in tf\_idf\_dataframe, the words need to be searched and the number of result will be output. First the program cleans the query by changing all words to lower case and split each word by space. Then find out the how many words in the query. The query is first taken as RDD format, then it is converted to dataframe format for further tasks. The dataframe is trimmed by cleaning out unnecessary white spaces. The program joins the tf\_idf index created by tf\_idf\_dataframe function with the query word and find out the tf\_idf of each word. Then each word&#39;s tf\_idf is combined to see the relevance of all words in each document. Then the final tf\_idf score is calculated by using the combined socre times number of matched words and divide by total number of words. The final result is sorted with descending order and output n number of result


