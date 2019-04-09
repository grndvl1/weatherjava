import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Date;

/**
 * Parsed the data into separate csv files.  I would normally create databases with hourly, daily and monthly tables
 * along with another table for sunrise sunset data since that data is a string and not a real time value.  The Date
 * is not SQL standard and I would convert this into a timestamp instead making compares easier.  After all that
 * most of queries like AVG and STDEV can be done with SQL.  Seeing as I have time constraint I removed extra columns
 * not needed so the CSVJDBC driver could work and I didn't have to type all 90 columns when reading in the Properties
 * for the columns.
 */
public class MainClass {

    private static int WindChillMaxTemp = 40;
    private static int WindChillMinWind = 3;
    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws Exception {
        // Load the driver.
        Class.forName("org.relique.jdbc.csv.CsvDriver");

        // Question 1
        Connection conn1 = connection();
        System.out.println(getAvgDev(conn1, "q3", "2017-01-01").toString());

        // Question 2
        windChill(conn1,"q3", "2017-01-01").forEach((key, value) -> System.out.println("Date " + key + "  WindChill = " + value));

        // Question 3
        findSimilarWeather(conn1).forEach((key, value) -> System.out.println("Date Texas = " + key + "  Date Atlanta = " + value));
        conn1.close();

    }

    private static Connection connection() throws SQLException {
        Properties props = new Properties();
        props.put("suppressHeaders", "true");
        props.put("headerline", "STATION_NAME,DATE,TIME,HOURLYDRYBULBTEMPF,HOURLYWindSpeed,DAILYSunrise,DAILYSunset");
        props.put("columnTypes", "String,Date,String,Int,Int,Int,Int");

        // Create a connection. The first command line parameter is the directory containing the .csv files.
        return DriverManager.getConnection("jdbc:relique:csv:" + "csv", props);
    }

    private static Connection connectionSim() throws SQLException {
        Properties props = new Properties();
        props.put("suppressHeaders", "true");
        props.put("headerline", "DATE,AVG,DEV");
        props.put("columnTypes", "String,Int,Int");

        // Create a connection. The first command line parameter is the directory containing the .csv files.
        return DriverManager.getConnection("jdbc:relique:csv:" + "csv", props);
    }

    // Question 1
    private static AverageAndDeviation getAvgDev(Connection conn, String database, String date) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery("SELECT DATE, TIME, HOURLYDRYBULBTEMPF, DAILYSunrise, DAILYSunset FROM "+database+" WHERE DATE='"+date+"'" );

        Statement stmt1 = conn.createStatement();
        ResultSet results1 = stmt1.executeQuery("SELECT DATE, TIME, HOURLYDRYBULBTEMPF, DAILYSunrise, DAILYSunset FROM "+database+" WHERE DATE='"+date+"'" );

        double avg = 0.0;
        double deviation = 0.0;
        int n = 0;

        // Find the average first
        while(results.next()) {
            int time = Integer.parseInt(results.getString(2).replaceFirst(":",""));
            int sunrise = results.getInt(4);
            int sunset = results.getInt(5);
            if (time > sunrise && time < sunset){
                avg += results.getInt(3);
                n++;
            }
        }
        avg = avg/n;

        // find the standard deviation
        while(results1.next()) {
            int time = Integer.parseInt(results1.getString(2).replaceFirst(":",""));
            int sunrise = results1.getInt(4);
            int sunset = results1.getInt(5);
            if (time > sunrise && time < sunset){
                deviation += Math.pow(results1.getInt(3) - avg, 2);
            }
        }

        deviation = Math.sqrt(deviation/n);

        return  new AverageAndDeviation(avg, deviation);
    }

    // Question 2
    private static Map<String, String> windChill(Connection conn, String database, String date) throws SQLException {
        Map<String,String> map = new LinkedHashMap<>();
        Statement stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery("SELECT * FROM "+database+" WHERE DATE='"+date+"'" );

        while(results.next()) {
            int temp = results.getInt(4);
            if (temp < WindChillMaxTemp && results.getInt(5) >= WindChillMinWind) {
                double wind16 = Math.pow(results.getInt(5), 0.16);
                int wc = (int)(35.74 + (0.6215*temp) - (35.75*wind16) + (0.4275*temp*wind16));
                map.put(results.getString(2) + " " + results.getString(3), "" + wc);
            }
        }

        return map;
    }

    // Question 3 does not indicate same date to compare so I compare every date to each other for 2017, to this
    // I previously extracted the Temp into q3.csv and q3a.csv to make parsing easier and calculated the Average and
    // Standard Deviation, creating a new file with that data. If this was in a SQL database I wouldn't have to do this
    // but for the sake of CSV and this driver it was a rabbit hole I went down.  I decided the temp was the most
    // important thing since we all measure that and are aware of it.  I ignored the facts of possible rain and such
    // given time I would have added more columns to the data.  So once I made these file The function then parses them
    // into resultsets and loops through finding dates from Texas that were the same average temp and standard deviation.
    // Which is a pretty good indicator that the days were similar.  This function creates a map of similar dates and
    // prints that out
    private static Map<String, String> findSimilarWeather(Connection conn) throws SQLException {
        writeSimilarWeatherData("TexasAVG.csv", conn, "q3", "CANADIAN TX US");
        writeSimilarWeatherData("AtlantaAVG.csv", conn, "q3a", "ATLANTA HARTSFIELD INTERNATIONAL AIRPORT GA US");

        Connection conn2 = connectionSim();
        Statement stmt = conn2.createStatement();
        ResultSet resultsTx = stmt.executeQuery("SELECT * FROM TexasAVG WHERE DATE LIKE '%2017%'");

        Map<String, String> map = new LinkedHashMap<>();
        resultsTx.next(); // some reason this is reading the header the first line...even though the props say not to.
        while (resultsTx.next()) {
            Statement stmt2 = conn2.createStatement();
            ResultSet resultsAt = stmt2.executeQuery("SELECT * FROM AtlantaAVG WHERE DATE LIKE '%2017%'");
            resultsAt.next();// some reason is reading the header the first next()...even though the props say not to.
            while (resultsAt.next()) {
                if (resultsTx.getInt(2) == resultsAt.getInt(2) &&
                        resultsTx.getInt(3) == resultsAt.getInt(3)) {
                    map.put(resultsTx.getString(1), resultsAt.getString(1) + " Avg=" +
                            resultsTx.getInt(2) + ", STDDEV="+ resultsTx.getInt(3));
                }
            }
        }
        conn2.close();
        return map;
    }

    // helper function to get first and last date in a file
    private static Map<String, Date> firstLastDate(Connection c, String database, String stationName) throws SQLException {
        Map<String,Date> map = new LinkedHashMap<>();
        Statement stmt = c.createStatement();
        ResultSet results = stmt.executeQuery("SELECT * FROM "+database + " WHERE STATION_NAME='"+stationName+"'"  );
        // bad way to do this but jdbc library limits the finding first and last rows.  unable to use resutls.firt() or results.last()
        while (results.next()) {
            if (!map.containsKey("first")) {
                map.put("first", results.getDate(2));
            }
            map.put("last", results.getDate(2));
        }
        return map;
    }

    // Java Date to add a day to a Date object using this to increment by one day
    public static Date addDays(Date date, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days); //minus number would decrement the days
        return cal.getTime();
    }

    // Creates a separate file to calculate the Avg and the STDEV for easier comparison.  Only does it once.
    public static void writeSimilarWeatherData(String fileName, Connection conn, String database, String stationName) throws SQLException {
        File file = new File("csv/"+ fileName);
        if (!file.exists()) {
            Map<String, AverageAndDeviation>  mapWeatherData = new LinkedHashMap<>();
            Map<String,Date> mapDatesTx = firstLastDate(conn, database, stationName);
            String date = mapDatesTx.get("first").toString();
            while (!date.equals(mapDatesTx.get("last").toString())) {
                AverageAndDeviation avd = getAvgDev(conn, database, date);
                System.out.println("date="+ date + "  Data=" + avd);
                mapWeatherData.put(date, avd);
                try {
                    Date myDate = format.parse(date);
                    date = format.format(addDays(myDate, 1));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            try {
                FileWriter outputfile = new FileWriter(file);
                CSVWriter writer = new CSVWriter(outputfile);

                String[] header = {"DATE", "AVG", "DEV"};
                writer.writeNext(header);
                for (Map.Entry<String, AverageAndDeviation> entry : mapWeatherData.entrySet()) {
                    String[] data1 = new String[]{entry.getKey(), "" + (int) entry.getValue().average, "" + (int) entry.getValue().deviation};
                    writer.writeNext(data1);
                }

                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
