import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;

import java.util.*;
import java.sql.Timestamp;

public class AvailabilityPredictor {
    public static void main(String[] args) {
        int COUNT_THRES = 2;        
        double PROB_THRES = 0.2;

        /* Data: ~3.6 mill entries:
           - stations:        {id, status, total_docks, latitude, longitude, label, time, primarykey(id+total_docks)}
           - available_bikes: {station_id, time, count} 
         */

        // Input: Target DOW, Target TOD, Station ID
        // Output: Number from (0,1) = probability of a bike being available within the target window


        /* (0) Start Spark session */

        SparkSession spark = SparkSession
          .builder()
          .appName("Simple App")
          .getOrCreate();


        /* (1) Take all data and put into spark-friendly interface/object */        
        
        Dataset<Row> jdbcDF_bikes = spark.read()
          .format("jdbc") // java database connector
          .option("url", "jdbc:postgresql:citibike")
          .option("dbtable", "public.available_bikes")
          .load();

        Dataset<Row> jdbcDF_stations = spark.read()
          .format("jdbc")
          .option("url", "jdbc:postgresql:citibike")
          .option("dbtable", "public.stations")
          .load();
        
        Iterator<Row> iterator2 = jdbcDF_stations.toLocalIterator();
        while (iterator2.hasNext()) {
            Row entry = iterator2.next();
            System.out.println(entry);
        }
        
        //jdbcDF.printSchema();


        /* (2) Obtain user input */
        
        Scanner scan = new Scanner(System.in);
        System.out.println("\n==============================================================================\n");
        System.out.print("Citi Bike Availability:\nEnter Station ID: ");
        String sid = scan.nextLine();
        System.out.print("Enter day of week (Monday, Tuesday, ...): ");
        String sdow = scan.nextLine();
        System.out.print("Enter hour of day (0, 1, ..., 23): ");
        String shour = scan.nextLine();
        System.out.print("Enter minute of hour (00, 01, ... 59): ");
        String smin = scan.nextLine();
        System.out.println("\n==============================================================================\n");


        /* (3) Parse user input */

        int stationID = Integer.parseInt(sid);
        
        int target_dow = 1; // (1-7 maps to Sun-Sat)
        if (sdow.equals("Sunday")) {
            target_dow = 1;
        } else if (sdow.equals("Monday")) {
            target_dow = 2;
        } else if (sdow.equals("Tuesday")) {
            target_dow = 3;
        } else if (sdow.equals("Wednesday")) {
            target_dow = 4;
        } else if (sdow.equals("Thursday")) {
            target_dow = 5;
        } else if (sdow.equals("Friday")) {
            target_dow = 6;
        } else if (sdow.equals("Saturday")) {
            target_dow = 7;
        }

        int target_hour = Integer.parseInt(shour);
        int target_min = Integer.parseInt(smin);

        // Calculate "target minute of day"
        int target_mod = target_hour * 60 + target_min;
       

        /* (4) Add useful columns to the Dataset */

        // Add a column for day of the week - First find day of year, then use modulus function
        jdbcDF_bikes = jdbcDF_bikes.withColumnRenamed("count", "cnt");
        Dataset jdbcDF_bikes_tmp1 = jdbcDF_bikes.withColumn("doy", dayofyear(col("time")));
        Dataset jdbcDF_bikes_tmp2 = jdbcDF_bikes_tmp1.withColumn("dow", col("doy").plus(4).mod(7));

        // Add a column for minute of day
        Dataset jdbcDF_bikes_tmp3 = jdbcDF_bikes_tmp2.withColumn("hour", hour(col("time")));
        Dataset jdbcDF_bikes_tmp4 = jdbcDF_bikes_tmp3.withColumn("minute", minute(col("time")));
        Dataset jdbcDF_bikes_tmp5 = jdbcDF_bikes_tmp4.withColumn("mod", col("hour").multiply(60).plus(col("minute")));

        //jdbcDF_bikes_tmp5.show();
        //System.out.println("\n\ntotal entries (using .count()) = "+jdbcDF_bikes_tmp5.count()+"\n\n");
     

        /* (5) Search through entries for station ID and timestamp within our time window */
        
        // Create String query/filter expression to find all entries with matching station ID, day of week, and time frame
        String filterExp1 = "station_id = " + stationID;
        String filterExp2 = "dow = " + target_dow;
        String filterExp3 = "ABS(mod - " + target_mod + ") < 20";
        String filterExp = filterExp1 + " AND " + filterExp2 + " AND " + filterExp3;

        // Filter Dataset and store count of matching entries
        long totalFoundEntries = jdbcDF_bikes_tmp5.filter(filterExp).count();
        //System.out.println("\n\ntotal entries w/ matching stationID & target_dow & mod timeframe = " + totalFoundEntries + "\n\n");
       
        // Add to filter expression in order to check for bike count
        String filterExp4 = "cnt > " + COUNT_THRES;
        filterExp = filterExp + " AND " + filterExp4;

        // Filter Dataset and store count of matching bike entries
        long totalFoundBikeEntries = jdbcDF_bikes_tmp5.filter(filterExp).count();
        //System.out.println("\n\ntotal entries w/ matching stationID & target_dow & mod timeframe AND BIKES!!!! = " + totalFoundBikeEntries + "\n\n");


         /* (6) Return probability of available bike and print output to user */
        
        double prob = ((double) totalFoundBikeEntries) / (totalFoundEntries);
        //System.out.println("========================================\nOUTPUT: "+prob+"\n=========================================");
        System.out.println("==============================================================================\n");
        System.out.println("Availability of station "+sid+" at "+shour+":"+smin+" will be:  "+prob+"\n");
        System.out.println("==============================================================================\n");


        /* ---------------------------- Additional Functionality ---------------------------- */

        /* If probability is below some threshold, suggest other nearby stations. */
        
        if (prob < PROB_THRES) {
            System.out.println("\nThe availability is below our threshold of "+PROB_THRES+".");
            System.out.print("Would you like to find a nearby station with higher availability? (y/n) ");
            String schoice = scan.nextLine();

            if (schoice.equals("y")) {
                // (A) Sort the stations by distance from input station ID, using a lat-long compare method

                // (B) Go through list in order until an entry has prob > PROB_THRES

                // (C) Print to user

            } else {
                // Do nothing?
            }
        }

        // Stop Spark session
        spark.stop();
    }


    /* Iterative implementation to find matching dataset entries 
    public static double findAvailabilityIteratively(Dataset<Row> db, int stationID, int target_dow, int target_mod) {
        int totalFoundEntries = 0;      // entries with same station ID
        int availableBikesEntries = 0;  // entries with same station ID AND timestamp within time window

        Iterator<Row> iterator = db.toLocalIterator();
        while (iterator.hasNext()) {
            Row entry = iterator.next();

            int id = entry.getInt(0);                           // Get station id
            Timestamp timestamp = entry.getAs(1);               // Get timestamp
            Calendar cal = GregorianCalendar.getInstance();     // Convert timestamp to Calendar instance
            cal.setTime(timestamp); 
            int dow = cal.get(java.util.Calendar.DAY_OF_WEEK);  // DAY_OF_WEEK returns 1-7
            int mod = cal.get(java.util.Calendar.HOUR_OF_DAY) * 60 + cal.get(java.util.Calendar.MINUTE);
            int count = entry.getInt(2);

            // Compare station IDs; check if timestamp is within time window
            if ( (id==stationID) && (dow==target_dow) && (Math.abs(mod - target_mod)<20) ) {
                totalFoundEntries++;
                if (count > COUNT_THRES) {
                    availableBikesEntries++;
                }
            }
        }
        return (double) availableBikesEntries / totalFoundEntries;
    }*/

}
