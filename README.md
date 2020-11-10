# Data_Mining_Frequent_Item
# Introduction
The problem of discovering association rules between itemsets in a sales transaction database (a set of baskets) includes the following two sub-problems [R. Agrawal and R. Srikant, VLDB '94 (Connexions vers un site externe.)]:

Finding frequent itemsets with support at least s;
Generating association rules with confidence at least c from the itemsets found in the first step.
Remind that an association rule is an implication X → Y, where X and Y are itemsets such that X∩Y=∅. Support of the rule X → Y is the number of transactions that contain X⋃Y. Confidence of the rule X → Y the fraction of transactions containing X⋃Y in all transactions that contain X.

# Task
You are to solve the first sub-problem: to implement the Apriori algorithm for finding frequent itemsets with support at least s in a dataset of sales transactions. Remind that support of an itemset is the number of transactions containing the itemset. To test and evaluate your implementation, write a program that uses your Apriori algorithm implementation to discover frequent itemsets with support at least s in a given dataset of sales transactions.

The implementation can be done using any big data processing framework, such as Apache Spark, Apache Flink, or no framework, e.g., in Java, Python, etc.  

Optional task for extra bonus
Solve the second sub-problem, i.e., develop and implement an algorithm for generating association rules between frequent itemsets discovered by using the Apriori algorithm in a dataset of sales transactions. The rules must have support at least s and confidence at least c, where s and c are given as input parameters.

# Datasets
As a sale transaction dataset, you can use this dataset, which includes generated transactions (baskets) of hashed items – you use any browser, e.g., Google Chrome, or a text editor, e.g., WordPad to view the file under Windows.
You can also use any other transaction datasets as an input dataset that you can find on the Web.
Readings
Lecture 3: Frequent Itemsets
Chapter 6Connexions vers un site externe. of Mining of Massive Datasets, by Jure Leskovec, Anand Rajaraman, and Jeffrey D. Ullman, 2nd edition, Cambridge University Press, 2014
R. Agrawal and R. Srikant. Fast Algorithms for Mining Association Rules (Connexions vers un site externe.), VLDB '94
# Submission, Presentation and Demonstration
To submit your homework, you upload your solution in a zip file to Canvas. Canvas records the submission time.  Submission on time, i.e. before the deadline,  is awarded with 3 exam bonus points if your homework is accepted. Bonus will not be given, if you miss the deadline.  Your homework solution must include 

Source code (with comments); 
Makefile or scripts to build and run (if needed); 
Report (in PDF) with a short description of your solution, instructions how to build and to run, command-line parameters, if any (including default values), results, e.g., plots or screenshots. 
Within a week after the homework deadline, you present and demonstrate your homework on your own laptop to a course instructor. A Doodle pool will be provided to book a time slot for presentation.