package telenav.src;

import lombok.SneakyThrows;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;
import scala.*;

import java.io.Serializable;
import java.lang.Long;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;


/**
 * @program: wejopd
 * @description:
 * @author: Lin.wang
 * @create: 2022-04-24 14:31
 **/
public class ParserFile implements Serializable {

    public static final int TIME_THRESHOLD = 300;
    public static final int DISTANCE_THRESHOLD = 1000;
    public static final int VALID_DISTANCE = 0;

    public ParserFile() {
//        SparkConf sparkConf = new SparkConf()
//                .setAppName("wejopd")
//                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
////                .set("spark.hadoop.fs.s3a.endpoint", "yourendpoint")
//                .set("spark.hadoop.fs.s3a.connection.maximum", "200")
//                .set("spark.hadoop.fs.s3a.fast.upload", "true")
//                .set("spark.hadoop.fs.s3a.connection.establish.timeout", "500")
//                .set("spark.hadoop.fs.s3a.connection.timeout", "5000")
//                .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
//                .set("spark.hadoop.com.amazonaws.services.s3.enableV4", "true")
//                .set("spark.hadoop.com.amazonaws.services.s3.enforceV4", "true")
//                .set("spark.hadoop.fs.s3a.proxy.host", "ec6d-wejopdproxy-01.mypna.com")
//                .set("spark.hadoop.fs.s3a.proxy.port", "3128");
        SparkSession
                .builder()
//                .master("local[*]")
                .appName("exploration wejopd ev data")
                .config("spark.sql.crossJoin.enabled", true)
//                .config(sparkConf)
                .getOrCreate();
    }

    public Dataset<Row> readFileByParquet(String path) {
        SparkSession sparkSession = SparkSession.getActiveSession().get();
        return sparkSession.read().parquet(path);
    }

    public Dataset<Row> readFileCsv(String path) {
        SparkSession sparkSession = SparkSession.getActiveSession().get();
        return sparkSession.read().option("header", "true").csv(path);
    }

    @SneakyThrows
    public Dataset<Row> filterRawParquetByBBox(Dataset<Row> dataset, String bbox){
        final WKTReader reader = new WKTReader();
        Geometry polygon = reader.read(bbox);
        return dataset.filter(x->{
            GenericRowWithSchema location = (GenericRowWithSchema) x.getAs("location");
            double latitude = location.getAs("latitude");
            double longitude = location.getAs("longitude");
            Point point = new GeometryFactory().createPoint(new Coordinate(longitude, latitude));
            return polygon.contains(point);
        });
    }

    public Dataset<TripLine> makeTrips(Dataset<Row> rowDataset) {
        Dataset<Tuple3<String, Long, TripNode>> objectDataset = rowDataset.map(x -> {
            // <journeyID, instantTime, TripNode>
            TripNode tripNode = new TripNode();
            long timeStamp = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnxx").parse(x.getAs("capturedTimestamp"), Instant::from).getEpochSecond();
            tripNode.setInstantTime(timeStamp);

            tripNode.setDataPointId(x.getAs("dataPointId"));
            tripNode.setJourneyId(x.getAs("journeyId"));
            GenericRowWithSchema location = (GenericRowWithSchema) x.getAs("location");
            double latitude = location.getAs("latitude");
            double longitude = location.getAs("longitude");
            tripNode.setCoordinate(new Coordinate(longitude, latitude));
            GenericRowWithSchema metrics = (GenericRowWithSchema) x.getAs("metrics");
            tripNode.setSpeed(metrics.getAs("speed"));
            tripNode.setHeading(metrics.getAs("heading"));
            return Tuple3.apply(x.getAs("journeyId"), timeStamp, tripNode);
        }, Encoders.tuple(Encoders.STRING(), Encoders.LONG(), Encoders.kryo(TripNode.class)));

        return objectDataset
                .groupByKey(Tuple3::_1, Encoders.STRING())
                .flatMapGroups((k, v) -> {
                    List<TripLine> lineList = new ArrayList<>();
                    List<Coordinate> coordinates = new ArrayList<>();
                    List<Tuple3> tuple3List = IteratorUtils.toList(v);
                    List<TripNode> tripNodes = new ArrayList<>();
                    for (Tuple3 tuple3 : tuple3List) {
                        tripNodes.add((TripNode) tuple3._3());
                    }
                    List<TripNode> collect = tripNodes.stream().sorted((x, y) -> {
                        long o1 = x.getInstantTime();
                        long o2 = y.getInstantTime();
                        return Long.valueOf(o1 - o2).intValue();
                    }).collect(Collectors.toList());

                    TripNode lastTripNode = new TripNode();
                    for (TripNode node : collect) {
                        if (CollectionUtils.isEmpty(coordinates)) {
                            coordinates.add(node.getCoordinate());
                            lastTripNode = node;
                            continue;
                        }
                        if (lastTripNode.getCoordinate().equals(node.getCoordinate())) {
                            lastTripNode = node;
                            continue;
                        }
                        GeodesicData geodesicData = Geodesic.WGS84.Inverse(
                                node.getCoordinate().y
                                , node.getCoordinate().x
                                , lastTripNode.getCoordinate().y
                                , lastTripNode.getCoordinate().x);
                        if (node.getInstantTime() - lastTripNode.getInstantTime() < TIME_THRESHOLD && geodesicData.s12 < DISTANCE_THRESHOLD) {
                            coordinates.add(node.getCoordinate());
                            lastTripNode = node;
                        } else {
                            // generate trip
                            addTrip2List(coordinates, lastTripNode, lineList);
                            coordinates.clear();
                        }
                    }

                    addTrip2List(coordinates, lastTripNode, lineList);
                    coordinates.clear();
                    return lineList.iterator();
                }, Encoders.kryo(TripLine.class));

    }

    public void writeTripDataset(String output, Dataset<TripLine> tripLineDataset) {
        tripLineDataset.map(x -> Tuple4.apply(x.toString(), x.getJourneyId(), Instant.ofEpochSecond(x.getInstantTime()).toString(), x.getDistance()), Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING(), Encoders.LONG()))
//                .repartition(3)
                .write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv(output);
    }

    @SneakyThrows
    public void filterRecordByBbox(Dataset<Row> dataset, String bbox, String output) {
        final WKTReader reader = new WKTReader();
        Geometry polygon = reader.read(bbox);
        dataset
                .filter(x -> {
                    WKTReader readLinestring = new WKTReader();
                    Geometry lineString = readLinestring.read((String) x.getAs("_1"));
                    return polygon.contains(lineString) || polygon.intersects(lineString);
                })
                .repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .csv(output);
    }

    @SneakyThrows
    public void filterRecordByJourneyId(Dataset<Row> dataset, String journeyId, String output) {
        dataset
                .filter(x -> x.getAs("journeyId").equals(journeyId))
                .map(x -> {
                    String timeStamp = x.getAs("capturedTimestamp");
                    String journey = x.getAs("journeyId");
                    GenericRowWithSchema location = (GenericRowWithSchema) x.getAs("location");
                    double latitude = location.getAs("latitude");
                    double longitude = location.getAs("longitude");
                    GenericRowWithSchema metrics = (GenericRowWithSchema) x.getAs("metrics");
                    double speed = metrics.getAs("speed");
                    return Tuple5.apply(timeStamp, journey, latitude, longitude, speed);
                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.DOUBLE(), Encoders.DOUBLE(), Encoders.DOUBLE()))
                .sort(col("_1"))
                .repartition(1)
                .write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv(output);
    }

    public void filterRecordByDistance(String tripPath, String outputPath, String distance) {
        SparkSession sparkSession = SparkSession.getActiveSession().get();
        Dataset<Row> resultDataset = sparkSession.read().option("header", "true").csv(tripPath);
        resultDataset
                .filter(x -> Integer.parseInt(x.getAs("_4")) > Integer.parseInt(distance))
                .repartition(1)
                .write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv(outputPath);

    }

    private void addTrip2List(List<Coordinate> coordinates, TripNode lastTripNode, List<TripLine> lineList) {
        if (coordinates.size() >= 2) {
            TripLine tripLine = new TripLine();
            LineString lineString = new GeometryFactory().createLineString(coordinates.toArray(new Coordinate[0]));
            tripLine.setLineString(lineString);
            tripLine.setJourneyId(lastTripNode.getJourneyId());
            tripLine.setInstantTime(lastTripNode.getInstantTime());
            long distance = Math.round(lineString.getLength() / 180 * Math.PI * 6371393);
            tripLine.setDistance(distance);
            if (distance > VALID_DISTANCE) {
                lineList.add(tripLine);
            }
        }
    }


}
