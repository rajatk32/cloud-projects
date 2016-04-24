Step 1:
===============================================
Term Frequency (TF)

Term frequency ranking assumes that a document is more important for a word if it occurs more frequently for that word. Term Frequency counts the raw frequency of a term in the document, i.e. the number of times the term t occurs in the document d.

Inverse Document Frequency (IDF)

Notice that Term Frequency (TF) Ranking is excessively biased towards documents with many common words ("a", "the", "this"...) and does not give sufficient weight to rarer words ("sharding", "scaling", "infrastructure"...).
The Inverse Document Frequency (IDF) is a measure of content relevance that is higher for terms that occur less frequently in the corpus. In its simplest form, the IDF of a keyword can begin to be computed by dividing the total number of documents in the corpus by the number of documents in which the term occurs. A log is then run on this fraction
Low-frequency terms will thus have a higher IDF and hence will be better at identifying relevant documents for a query.

The TF-IDF of a word i in document j may be expressed as:
Wi,j = TFi,j  X log(N/DFi)

where TFi,j = no. of occurrences of i in j
DFi = no. of documents containing i
N = total no. of documents

1. For each word in a document, compute the number of times it occurs. Store this in the form (word,title),count. Here, count is the term frequency for each word for each document.
2. Convert the text field(XMLArticle) into lowercase alphabets and space only.
Delete all XML tags.
Replace all \\n (displayed as \n in text)
Replace all punctuations and numbers with a whitespace.
Split by whitespaces to find words.
3. For each word in the dataset:
Count the number of documents in which each word occurs (d_word).
Count the total number of documents in the dataset (N).
Compute the IDF for each word-document pair, using log(N/d_word)
Store this in the form (word,title),idf
4. Multiply the results of the previous two steps to generate the TF-IDF for each word. Store this in the form (word,title),tfidf.
5. Finally, from your list of TF-IDF, find the top 100 documents for the search term "cloud". Store these in descending order of frequency (order ties alphabetically by title) a tab-separated file named "tfidf" in the format:
document_title    tf-idf_value

Step 2:
===============================================
PageRank represents numerically the importance of a page in the web or of a document in the corpus. The intuition behind PageRank is that when many pages link to a single page P, then P is an important page. And of course, the larger the number of incoming links, the greater the importance of the page.

To compute PageRank on a graph dataset, there are two stages.

Stage 1 creates a simple network from the given text corpus. This is in the form of an adjacency list. An adjacency list is a graph expressed as :

Node	Neighbors
a	        b,c,d
b	        c,d
c	        d
d	        e

Stage 2 involves running PageRank on this graph.

To compute the PageRank, follow these steps:

1. Take as your input the data from s3://s15-p42-part2/.
2. Construct an adjacency list for each document (as explained above).
3. Assign a weight 1.0 to each document.
4. Iterate the following steps 10 times:
i. For each node in the graph, distribute the PageRank equally amongst all its neighbors. This can be thought of as a map operation.
ii. Update the PageRank of each document to 0.15 + 0.85 * (contributions), where contributions is the total PageRank earned from all neighbors of the document.
iii. Make sure you handle the case of dangling pages. Dangling pages are pages with no outbound links. These are not handled in the example implementation (from the Spark example code). Hence do not rely on it as only the example code as a template for what to do.
Consider the following starting position:

    key: page1 value: 1.0 page2 page3 
    key: page2 value: 1.0 page3 page1
    key: page3 value: 1.0 
    
After 1 iteration, the same contributions will be received by each page, as shown:
    key: page1  contributions received: 0.5 adjacency list: page2 page3 
    key: page2  contributions received: 0.5 adjacency list: page3 page1 
    key: page3  contributions received: 1.0 adjacency list: 
    
page3 is a dangling page. Dangling pages are pages with no outbound links. In PageRank, they generally represent pages that have not been crawled yet. Unfortunately, the total aggregate of all PageRank values should be a constant (as per the formal definition of PageRank). However, dangling pages do not emit any weight and hence, the system tends to lose weight at each iteration. The way to correct this is by redistributing the weight of dangling pages across all the other pages at the end of an iteration. In this example, there is only one dangling page (page3). Hence, its contribution (1.0 should be distributed equally amongst page1, page2, page3).

Hence, the new PageRanks are:

    page1 = 0.15 + 0.85 * (0.5 + 1.0/3) = 0.8583
    page2 = 0.15 + 0.85 * (0.5 + 1.0/3) = 0.8583
    page3 = 0.15 + 0.85 * (1.0 + 1.0/3) = 1.2833
    
5. In each iteration, the program should end with the data structure format it started with. This ensures that the algorithm is correctly iterative.
6. Finally, from your output list of PageRank, find the top 100 documents. Store these in descending order of frequency (order ties alphabetically by title) a tab-separated file named "pagerank" of the type:
document_title  pagerank_value