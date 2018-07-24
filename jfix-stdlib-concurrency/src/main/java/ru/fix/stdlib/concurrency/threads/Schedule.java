package ru.fix.stdlib.concurrency.threads;


import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Objects;

public class Schedule {

    public enum Type {
        /**
         * scheduleWithFixedDelay
         */
        DELAY,
        /**
         * scheduleAtFixedRate
         */
        RATE
    }

    private final Type type;
    private final long value;

    public Schedule(Type type, long value) {
        this.type = type;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public long getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schedule schedule = (Schedule) o;
        return value == schedule.value
                && type == schedule.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    /**
     * Creates Schedule settings for periodical action with fixed rate
     *
     * @param rate the period between successive executions
     */
    public static Schedule withRate(long rate) {
        return new Schedule(Type.RATE, rate);
    }


    public static DynamicProperty<Schedule> withRate(DynamicProperty<? extends Number> rate) {
        return rate.map(newValue -> new Schedule(Type.RATE, newValue.longValue()));
    }


    /**
     * Creates Schedule settings for periodical action with fixed delay
     *
     * @param delay the delay between the termination of one execution and the commencement of the next
     */
    public static Schedule withDelay(long delay) {
        return new Schedule(Type.DELAY, delay);
    }

    public static DynamicProperty<Schedule> withDelay(DynamicProperty<? extends Number> delay) {
        return delay.map(newValue -> new Schedule(Type.DELAY, newValue.longValue()));
    }
}
