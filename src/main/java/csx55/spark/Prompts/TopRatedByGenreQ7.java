package main.java.csx55.spark.Prompts;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import main.java.csx55.spark.DataSingleton;

/*
 * This just encapsulates the code for the seventh prompt. Nothing special here. You'll find the same unremarkable design in the other prompt classes. It's just a way to organize the code.
 * The task is to come up with my own question so I chose: What are the top 3 rated movies for each genre?
 */
public class TopRatedByGenreQ7 {
    public final String OUTPUT = "output/top_rated_by_genre";

    public TopRatedByGenreQ7() {

    }

    public void run() {
        DataSingleton ds = DataSingleton.getInstance();
        Dataset<Row> movies = ds.get("movies");
        Dataset<Row> ratings = ds.get("ratings");

        // // joinSets
        Dataset<Row> joined = movies.join(
             ratings,
             movies.col("movieId").equalTo(ratings.col("movieId"))
        );
        
        Dataset<Row> genreRank = joined.select("movieID", "genres", "rating");
        genreRank = genreRank.groupBy("genres", "movieID").agg( functions.avg("rating").as("avg_rating"));
        
        genreRank = genreRank.sort(functions.desc("avg_rating"));
        genreRank = genreRank.groupBy("genres").agg(functions.collect_list("movieID").as("movieID"), functions.collect_list("avg_rating").as("avg_rating"));
        genreRank = genreRank.withColumn("top_3", functions.array(functions.col("movieID").getItem(0), functions.col("movieID").getItem(1), functions.col("movieID").getItem(2)));
        genreRank = genreRank.drop("movieID");
        genreRank = genreRank.withColumnRenamed("top_3", "movieID");
        genreRank = genreRank.withColumn("top_3_avg_rating", functions.array(functions.col("avg_rating").getItem(0), functions.col("avg_rating").getItem(1), functions.col("avg_rating").getItem(2)));
        genreRank = genreRank.drop("avg_rating");
        genreRank = genreRank.withColumnRenamed("top_3_avg_rating", "avg_rating");


        String path = ds.getPath() + OUTPUT;
        genreRank.write().format("csv").save(path);
    }
    
}
