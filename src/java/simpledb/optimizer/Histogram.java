package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.storage.Field;

public interface Histogram {
    public void addValue(Field v);

    public double estimateSelectivity(Predicate.Op op, Field v);
}
