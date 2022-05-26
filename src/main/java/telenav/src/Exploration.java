package telenav.src;

import lombok.SneakyThrows;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;

import java.util.*;

/**
 * @program: wejopd
 * @description:
 * @author: Lin.wang
 * @create: 2022-04-24 14:30
 **/
public class Exploration {
    @SneakyThrows
    private static Map<String, String> getArgs(String[] args) {
        Map<String, String> mapArgs = new HashMap<>();
        Options options = new Options();
        options.addOption("i", "input", true, "--i input data path")
                .addOption("o", "output", true, "--o output data path")
                .addOption("t", "target", true, "--t operation target")
                .addOption("p", "parameter", true, "--p addition parameters")
        ;
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("i")) {
            mapArgs.put("i", cmd.getOptionValue("i"));
        }
        if (cmd.hasOption("o")) {
            mapArgs.put("o", cmd.getOptionValue("o"));
        }
        if (cmd.hasOption("t")) {
            mapArgs.put("t", cmd.getOptionValue("t"));
        }
        if (cmd.hasOption("p")) {
            mapArgs.put("p", cmd.getOptionValue("p"));
        }
        return mapArgs;
    }

    public static void main(String[] args) {
        Map<String, String> mapArgs = getArgs(args);
        if (StringUtils.isEmpty(mapArgs.get("i"))
                || StringUtils.isEmpty(mapArgs.get("o"))
                || StringUtils.isEmpty(mapArgs.get("t"))) {
            throw new IllegalArgumentException("[ERROR] start up parameter error, please check!!");
        }

        if (StringUtils.equalsIgnoreCase(StringUtils.trim(mapArgs.get("t")), "filter_raw_bbox_make_trips")) {
            ParserFile parserFile = new ParserFile();
            Dataset<Row> row = parserFile.readFileByParquet(mapArgs.get("i"));
            Dataset<Row> filterRow = parserFile.filterRawParquetByBBox(row, mapArgs.get("p"));
            Dataset<TripLine> lineDataset = parserFile.makeTrips(filterRow);
            parserFile.writeTripDataset(mapArgs.get("o"), lineDataset);
        } else if (StringUtils.equalsIgnoreCase(StringUtils.trim(mapArgs.get("t")), "make_trips")) {
            ParserFile parserFile = new ParserFile();
            Dataset<Row> row = parserFile.readFileByParquet(mapArgs.get("i"));
            Dataset<TripLine> lineDataset = parserFile.makeTrips(row);
            parserFile.writeTripDataset(mapArgs.get("o"), lineDataset);
        } else if (StringUtils.equalsIgnoreCase(StringUtils.trim(mapArgs.get("t")), "filter_distance")) {
            ParserFile parserFile = new ParserFile();
            parserFile.filterRecordByDistance(mapArgs.get("i"), mapArgs.get("o"), String.valueOf(mapArgs.get("p")));
        } else if (StringUtils.equalsIgnoreCase(StringUtils.trim(mapArgs.get("t")), "filter_bbox")) {
            ParserFile parserFile = new ParserFile();
            Dataset<Row> row = parserFile.readFileCsv(mapArgs.get("i"));
            parserFile.filterRecordByBbox(row, mapArgs.get("p"), mapArgs.get("o"));
        } else if (StringUtils.equalsIgnoreCase(StringUtils.trim(mapArgs.get("t")), "filter_jouney")) {
            ParserFile parserFile = new ParserFile();
            Dataset<Row> row = parserFile.readFileByParquet(mapArgs.get("i"));
            parserFile.filterRecordByJourneyId(row, mapArgs.get("p"), mapArgs.get("o"));
        }
    }
}
