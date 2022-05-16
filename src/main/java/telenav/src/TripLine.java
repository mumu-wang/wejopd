package telenav.src;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;

import java.io.Serializable;

/**
 * @program: wejopd
 * @description:
 * @author: Lin.wang
 * @create: 2022-04-25 13:21
 **/

@Setter
@Getter
public class TripLine implements Serializable {
    private LineString lineString;
    private String journeyId;
    private long instantTime;
    private long distance;

    @Override
    public String toString() {
        return lineString.toString();
    }
}
