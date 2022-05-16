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
        options.addOption("d", "data", true, "-d data path")
                .addOption("o", "output", true, "-o output data path")
                .addOption("f", "filter", true, "-f filter bbox")
                .addOption("r", "result", true, "-r result data path")
                .addOption("j", "filter", true, "-j filter journey id")
        ;
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("d")) {
            mapArgs.put("d", cmd.getOptionValue("d"));
        }
        if (cmd.hasOption("r")) {
            mapArgs.put("r", cmd.getOptionValue("r"));
        }
        if (cmd.hasOption("o")) {
            mapArgs.put("o", cmd.getOptionValue("o"));
        }
        if (cmd.hasOption("f")) {
            mapArgs.put("f", cmd.getOptionValue("f"));
        }
        if (cmd.hasOption("j")) {
            mapArgs.put("j", cmd.getOptionValue("j"));
        }
        return mapArgs;
    }

    public static void main(String[] args) {
        Map<String, String> mapArgs = getArgs(args);
        // -d -o -f , filter record by bbox
        if (StringUtils.isNotEmpty(mapArgs.get("d"))
                && StringUtils.isNotEmpty(mapArgs.get("o"))
                && StringUtils.isNotEmpty(mapArgs.get("f"))
        ) {
            ParserFile parserFile = new ParserFile();
            Dataset<Row> row = parserFile.readFileCsv(mapArgs.get("d"));
            parserFile.filterRecordByBbox(row, mapArgs.get("f"), mapArgs.get("o"));
        } // -d -o -j , filter record by journey Id
        else if (StringUtils.isNotEmpty(mapArgs.get("d"))
                && StringUtils.isNotEmpty(mapArgs.get("o"))
                && StringUtils.isNotEmpty(mapArgs.get("j"))
        ) {
            ParserFile parserFile = new ParserFile();
            Dataset<Row> row = parserFile.readFileByParquet(mapArgs.get("d"));
            parserFile.filterRecordByJourneyId(row, mapArgs.get("j"), mapArgs.get("o"));
        }// -d -o  comprise trips
        else if (StringUtils.isNotEmpty(mapArgs.get("d"))
                && StringUtils.isNotEmpty(mapArgs.get("o"))) {
            ParserFile parserFile = new ParserFile();
            Dataset<Row> row = parserFile.readFileByParquet(mapArgs.get("d"));
            Dataset<TripLine> lineDataset = parserFile.parserFile(row);
            parserFile.writeTripDataset(mapArgs.get("o"), lineDataset);
        } // -r -o , filter trips where distance > 4000 m
        else if (StringUtils.isNotEmpty(mapArgs.get("r"))
                && StringUtils.isNotEmpty(mapArgs.get("o"))) {
            ParserFile parserFile = new ParserFile();
            parserFile.analysisResultFiles(mapArgs.get("r"), mapArgs.get("o"));
        }

    }
}
