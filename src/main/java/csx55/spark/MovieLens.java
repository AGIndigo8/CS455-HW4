package csx55.spark;

import org.apache.spark.api.java.JavaSparkContext;

import csx55.spark.Prompts.*;

import org.apache.spark.sql.SparkSession;

public class MovieLens {


    public static void main(String[] args) {
       
        SparkSession spark = SparkSession.builder()
                .appName("MovieLens")
                .master("local")
                .getOrCreate();

      
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        DataSingleton ds = DataSingleton.getInstance(sc, spark);

        String task = args[0];

        if(task.equals("Q1")){
            MoviesByYearQ1 q1 = new MoviesByYearQ1();
            q1.run();
        } else if(task.equals("Q2")){
            AvgNumGenresQ2 q2 = new AvgNumGenresQ2();
            q2.run();
        } else if(task.equals("Q3")){
            GenresRank q3 = new GenresRank();
            q3.run();
        } else if(task.equals("Q4")){
            TopGenreCombQ4 q4 = new TopGenreCombQ4();
            q4.run();
        } else if(task.equals("Q5")){
            ComedyQ5 q5 = new ComedyQ5();
            q5.run();
        } else if(task.equals("Q6")){
            MoviesPerGenre q6 = new MoviesPerGenre();
            q6.run();
        }

        
        sc.close();
    }
}