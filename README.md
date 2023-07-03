**NEWDAY ASSIGNMENT**

![image](https://github.com/kaithisharath92/NewDay/assets/132219522/9d3598cd-d4a4-45f9-915f-710a0cf6e4b1)


**Versions:**

1) Java 1.8.0_202 
2) Spark: 3.1.3
3) Maven 3.8.8

 
⚙️ **Build commands**
Used Maven for the build management.
Below are the commands for maven.
1.	mvm package
2.	mvm clean


**Spark submit command**:
spark-submit \
--master "local" \
--deploy-mode "client" \
--jars NewDayProject\newDayAssignment\target\MovieRatings-1.0-SNAPSHOT.jar \
--class org.newDay.assignment.MovieRatings  \
NewDayProject\MovieRatings\target\MovieRatings-1.0-SNAPSHOT.jar \
<Path for movies>\movies.DAT \
<path for ratings >\ratings.DAT \
<path for movie stats >\moviesStats.parquet  \
<path for top movies >\ratings.parquet 


Note:
1) I have developed the code on windows, that is the reason I could not add executory memory , cores etc.
2) I have added the input .DAT files in resources so that test cases can run with the actual data
