package telenav.src;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;

/**
 * @program: wejopd
 * @description:
 * @author: Lin.wang
 * @create: 2022-04-25 10:46
 **/


@Getter
@Setter
public class TripNode implements Serializable {
    private String journeyId;
    private String dataPointId;
    private Long instantTime;
    private Coordinate coordinate;
    private double speed;
    private double heading;
}
