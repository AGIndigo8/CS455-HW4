package csx55.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import main.java.csx55.spark.Prompts.*;

import org.apache.spark.sql.SparkSession;

import java.util.List;

public class MovieLens {

    HashMap<String, Dataset> data = new HashMap<String, Dataset>();


    public static void main(String[] args) {
       
        SparkSession spark = SparkSession.builder()
                .appName("MovieLens")
                .master("local")
                .getOrCreate();

      
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        DataSingleton ds = DataSingleton.getInstance(sc);

        List<String> first10 = data.take(10);
        for (String s : first10) {
            System.out.println(s);
        }

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