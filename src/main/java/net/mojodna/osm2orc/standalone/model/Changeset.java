package net.mojodna.osm2orc.standalone.model;


import org.openstreetmap.osmosis.core.domain.common.TimestampContainer;

import java.util.HashMap;
import java.util.Map;

public class Changeset {

    public static final String ID = "id";
    public static final String CREATED_AT = "created_at";
    public static final String CLOSED_AT = "closed_at";
    public static final String OPEN = "open";
    public static final String NUM_CHANGES = "num_changes";
    public static final String USER = "user";
    public static final String UID = "uid";
    public static final String MIN_LAT = "min_lat";
    public static final String MAX_LAT = "max_lat";
    public static final String MIN_LON = "min_lon";
    public static final String MAX_LON = "max_lon";
    public static final String COMMENTS_COUNT = "comments_count";
    public static final String TAG = "tag";

    private static long instanceCount = 0;

    private long id;
    private TimestampContainer createdAt;
    private TimestampContainer closedAt;
    private boolean open;
    private long numChanges;
    private String user;
    private Long uid;
    private Double minLat;
    private Double maxLat;
    private Double minLon;
    private Double maxLon;
    private long commentsCount;
    private Map<String, String> tags = new HashMap<>();

    public Changeset(long id, TimestampContainer createdAt, TimestampContainer closedAt,
                     boolean open, long numChanges, String user,
                     Long uid, Double minLat, Double maxLat,
                     Double minLon, Double maxLon, long commentsCount) {
        ++instanceCount;
        this.id = id;
        this.createdAt = createdAt;
        this.closedAt = closedAt;
        this.open = open;
        this.numChanges = numChanges;
        this.user = user;
        this.uid = uid;
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.commentsCount = commentsCount;
    }

    public long getId() {
        return id;
    }

    public TimestampContainer getCreatedAt() {
        return createdAt;
    }

    public TimestampContainer getClosedAt() {
        return closedAt;
    }

    public boolean isOpen() {
        return open;
    }

    public long getNumChanges() {
        return numChanges;
    }

    public String getUser() {
        return user;
    }

    public Long getUid() {
        return uid;
    }

    public Double getMinLat() {
        return minLat;
    }

    public Double getMaxLat() {
        return maxLat;
    }

    public Double getMinLon() {
        return minLon;
    }

    public Double getMaxLon() {
        return maxLon;
    }

    public long getCommentsCount() {
        return commentsCount;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public long instanceCount() {
        return instanceCount;
    }
}
