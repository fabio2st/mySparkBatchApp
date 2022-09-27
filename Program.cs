using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace mySparkBatchApp
{
    internal class Program
    {
        static readonly DateTime s_referenceDate = new DateTime(2015, 10, 20);
        static void Main(string[] args)
        {
            SparkSession spark = SparkSession
                 .Builder()
                 .AppName("GitHub and Spark Batch")
                 .GetOrCreate();

            DataFrame projectsDf = spark
                .Read()
                .Schema("id INT, url STRING, owner_id INT, " +
                "name STRING, descriptor STRING, language STRING, " +
                "created_at STRING, forked_from INT, deleted STRING," +
                "updated_at STRING")
                .Csv("D:\\Development\\mySparkBatchApp\\data\\projects_smaller.csv");
            projectsDf.Show();

            // Drop any rows with NA values
            DataFrameNaFunctions dropEmptyProjects = projectsDf.Na();
            DataFrame cleanedProjects = dropEmptyProjects.Drop("any");

            // Remove unnecessary columns
            cleanedProjects = cleanedProjects.Drop("id", "url", "owner_id");
            cleanedProjects.Show();

            // Average number of times each language has been forked
            DataFrame groupedDF = cleanedProjects
                .GroupBy("language")
                .Agg(Avg(cleanedProjects["forked_from"]));
            // Sort by most forked languages first
            groupedDF.OrderBy(Desc("avg(forked_from)")).Show();

            spark.Udf().Register<string, bool>(
                "MyUDF",
                (date) => DateTime.TryParse(date, out DateTime convertedDate) &&
                    (convertedDate > s_referenceDate));
            cleanedProjects.CreateOrReplaceTempView("dateView");

            DataFrame dateDf = spark.Sql("SELECT *, MyUDF(dateView.updated_at) AS datebefore FROM dateView");
            dateDf.Show();

            spark.Stop();
        }
    }
}
