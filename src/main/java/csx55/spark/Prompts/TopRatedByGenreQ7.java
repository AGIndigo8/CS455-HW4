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
        
        Dataset<Row> genreRank = joined.select("genres", "rating", "title");
        genreRank = genreRank.groupBy("genres", "title").agg( functions.avg("rating").as("avg_rating"));
        // Top 3 rated movies for EACH genre
        genreRank = genreRank.sort(functions.desc("avg_rating"));
        genreRank = genreRank.groupBy("genres").agg(functions.collect_list("title").as("top_3_movies"));


        String path = ds.getPath() + OUTPUT;
        genreRank.write().format("csv").save(path);
    }
    
}
