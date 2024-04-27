package csx55.spark.Prompts;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import csx55.spark.DataSingleton;

/*
 * This just encapsulates the code for the fourth prompt. Nothing special here. You'll find the same unremarkable design in the other prompt classes. It's just a way to organize the code.
The question is: What are the top 3 combinations of genres that have the highest average rating?
 */
public class TopGenreCombQ4 {

    public final String OUTPUT = "output/top_genre_comb";

    public TopGenreCombQ4(){

    }

    public void run(){
        DataSingleton ds = DataSingleton.getInstance();
        Dataset<Row> movies = ds.get("movies");
        Dataset<Row> ratings = ds.get("ratings");

        // joinSets
        Dataset<Row> joined = movies.join(
            ratings,
            movies.col("movieId").equalTo(ratings.col("movieId"))
        );
        
        Dataset<Row> genreRank = joined.select("genres", "rating");
        genreRank = genreRank.groupBy("genres").agg( functions.avg("rating").as("avg_rating"));
        genreRank = genreRank.sort(functions.desc("avg_rating"));
        genreRank = genreRank.limit(3);        

        String path = ds.getPath() + OUTPUT;
        genreRank.write().format("csv").save(path);
    }
    
}
