package simpledb.optimizer;

import java.util.Arrays;

import simpledb.execution.Predicate;
import simpledb.execution.Predicate.Op;
import simpledb.storage.Field;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram implements Histogram {
    private final int cnt[], numb, min, max, w;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it
     * receives. It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through
     * the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed. For
     * example, you shouldn't simply store every value that you see in a sorted
     * list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this
     *                class for histogramming
     * @param max     The maximum integer value that will ever be passed to this
     *                class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // DONE: some code goes here
        numb = Math.min(buckets, max - min + 1);
        cnt = new int[numb];
        this.min = min;
        this.max = max;
        ntups = 0;
        // ceil((max-min+1)/numb)
        w = (max - min) / numb + 1;
    }

    private int loc(int v) {
        return (v - min) / w;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // DONE: some code goes here
        ++cnt[loc(v)];
        ++ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate of
     * the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // DONE: some code goes here
        if (op == Predicate.Op.EQUALS) {
            if (v < min || v > max) {
                return 0.0;
            }
            int i = loc(v);
            return 1.0 * cnt[i] / w / ntups;
        }
        if (op == Predicate.Op.NOT_EQUALS) {
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        if (op == Predicate.Op.GREATER_THAN) {
            if (v > max) {
                return 0.0;
            }
            if (v < min) {
                return 1.0;
            }
            int i = loc(v);
            int br = (i + 1) * w - 1 + min;
            double res = 1.0 * (br - v) / w;
            res *= 1.0 * cnt[i] / ntups;
            for (int j = i + 1; j < numb; ++j) {
                res += 1.0 * cnt[j] / w / ntups;
            }
            return res;
        }
        if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.EQUALS, v)
                    + estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        }
        if (op == Predicate.Op.LESS_THAN) {
            return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
        }
        if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        }
        return -1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic join
     *         optimization. It may be needed if you want to implement a more
     *         efficient optimization
     */
    public double avgSelectivity() {
        // DONE: some code goes here
        // skip it
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // DONE: some code goes here
        return "IntHistogram NumB=" + numb + ",min=" + min + ",max=" + max + ",cnt="
                + Arrays.toString(cnt);
    }
    
    private int fetchVal(Field v) {
        return Integer.parseInt(v.toString());
    }

    @Override
    public void addValue(Field v) {
        // DONE Auto-generated method stub
        addValue(fetchVal(v));
    }

    @Override
    public double estimateSelectivity(Op op, Field v) {
        // DONE Auto-generated method stub
        return estimateSelectivity(op, fetchVal(v));
    }

//    public static void main(String[] args) {
//        IntHistogram h = new IntHistogram(10, 1, 10);
//        h.addValue(3);
//        h.addValue(3);
//        h.addValue(3);
//        h.addValue(1);
//        h.addValue(10);
//        System.out.println(h.estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, 3));
//    }
}
