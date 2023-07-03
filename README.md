**NEWDAY ASSIGNMENT**

![image](https://github.com/kaithisharath92/NewDay/assets/132219522/9d3598cd-d4a4-45f9-915f-710a0cf6e4b1)



💡**Description**

The job needs to do the following:
1. Read in movies.dat and ratings.dat to spark dataframes.
2. Creates a new dataframe, which contains the movies data and 3 new columns max, min and
average rating for that movie from the ratings data.
3. Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies
based on their rating.
4. Write out the original and new dataframes in an efficient format of your choice.

**Versions:**

Java 1.8.0_202  ![image](https://github.com/kaithisharath92/NewDay/assets/132219522/e2f984c4-6249-404d-a661-aa3773222a43)

Spark: 3.1.3
IntelliJ idea: IntelliJ IDEA 2022.3.2 (Community Edition) 
Maven 3.8.8

 
⚙️ **Build commands**
Used Maven for the build management.
Below are the commands for maven.
1.	mvm package
2.	mvm clean


**Spark submit command**:
C:\Users\shara>spark-submit --master "local" --deploy-mode "client" --jars C:\Users\shara\NewDayProject\newDayAssignment\target\MovieRatings-1.0-SNAPSHOT.jar --class org.newDay.assignment.MovieRatings  C:\Users\shara\NewDayProject\MovieRatings\target\MovieRatings-1.0-SNAPSHOT.jar C:\Users\shara\Downloads\ml-1m\movies.DAT C:\Users\shara\Downloads\ml-1m\ratings.DAT C:\Users\shara\Downloads\ml-1m\moviesStats.parquet  C:\Users\shara\Downloads\ml-1m\ratings.parquet



<<movies.DAT>>  
<<ratings.DAT>>
<<moviesStats.parquet>>
<<ratings.parquet>>





















