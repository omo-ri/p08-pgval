package p08.model;

/**
 * An output record. The record itself is written to Kafka as the JSON value,
 * {@link #kafkaKey()} as the JSON key (null for no key).
 */
public interface Keyed {
    String kafkaKey();
}
