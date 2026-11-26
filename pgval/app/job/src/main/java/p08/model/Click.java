package p08.model;

/**
 * Input event from the "click" topic:
 * {"id":1,"timestamp":"2026-11-03T10:00:05.000","page":"/help"}, timestamp in UTC.
 *
 * Public fields and an empty constructor make it a Flink POJO (fast serialization).
 */
public class Click {
    public long id;
    public String page;
    /** Event time, epoch milliseconds. */
    public long ts;

    public Click() {
    }

    public Click(long id, String page, long ts) {
        this.id = id;
        this.page = page;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Click{id=" + id + ", page=" + page + ", ts=" + ts + "}";
    }
}
