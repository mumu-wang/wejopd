package telenav.src;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.format.DateTimeFormatter;


public class ParserFileTest {

    @Test
    public void readFileTest() {
        ParserFile parserFile = new ParserFile();
        Dataset<Row> row = parserFile.readFileByParquet("e:\\temp\\wejopd\\0220\\*.gz.parquet");
        Dataset<TripLine> lineDataset = parserFile.parserFile(row);
        parserFile.writeTripDataset("D:/vehicle_out/trip.csv", lineDataset);
    }

    @Test
    public void timeStampTest() {
        String timeStringLater = "2021-09-01T20:34:59.000-0400";
        Instant instantLater = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnxx").parse(timeStringLater, Instant::from);
        String timeStringEarly = "2021-09-01T17:26:07.000-0700";
        Instant instantEarly = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnxx").parse(timeStringEarly, Instant::from);
        Assert.assertTrue(instantLater.getEpochSecond() > instantEarly.getEpochSecond());
    }

    @Test
    public void mainTest() {
//        Exploration.main(new String[]{"-d","file:///e:/temp/wejopd/0902/hr=0/*.gz.parquet", "-o", "file:///d:/vehicle_out/test_spark",});
//        Exploration.main(new String[]{"-d","s3://wejopd/Vehicle-Movements/dt=2021-09-02/hr=0/*.gz.parquet", "-o", "file:///d:/vehicle_out/test_spark",});
    }

    @Test
    public void peekResult() {
        Exploration.main(new String[]{"-r", "file:///d:/vehicle_out/trips/*.csv", "-o", "file:///d:/vehicle_out/test_spark"});
    }

    @Test
    public void filterData() {
        Exploration.main(new String[]{"-d", "file:///d:/vehicle_out/trips/*.csv"
                , "-o", "file:///d:/vehicle_out/filter"
                , "-f", "POLYGON((-121.85583 37.48462,-121.92492 37.45387,-121.94508 37.46902,-122.05124 37.45901,-122.07776 37.4753,-122.12391 37.45286,-122.15254 37.45778,-122.19058 37.43123,-122.20242 37.36002,-122.14629 37.26865,-122.04381 37.20284,-122.02619 37.16682,-121.7555 37.04908,-121.726 37.02096,-121.73619 37.01534,-121.71671 37.01237,-121.73601 36.9903,-121.69592 36.98569,-121.69783 36.9721,-121.66447 36.96342,-121.64577 36.93232,-121.62475 36.94045,-121.59035 36.92615,-121.57576 36.89302,-121.48848 36.9834,-121.44995 36.98872,-121.41849 36.96016,-121.21538 36.96125,-121.24657 36.98523,-121.23334 37.01175,-121.24865 37.03368,-121.20819 37.06197,-121.24545 37.08915,-121.21736 37.12305,-121.23663 37.15667,-121.26207 37.15931,-121.28108 37.1836,-121.29849 37.16596,-121.32841 37.16595,-121.35783 37.18477,-121.39901 37.1501,-121.4218 37.2213,-121.45575 37.24944,-121.45805 37.28414,-121.40575 37.31097,-121.42365 37.35884,-121.40907 37.3807,-121.45665 37.39553,-121.47261 37.42334,-121.4629 37.4515,-121.48677 37.47565,-121.47188 37.48186,-121.85583 37.48462))"});
    }

}