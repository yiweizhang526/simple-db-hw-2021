package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 要比较字段的号1
     * */
    private int fieldId1;

    /**
     * 操作符
     * */
    private Predicate.Op op;

    /**
     * 要比较的字段的号2
     * */
    private int fieldId2;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.fieldId1 = field1;
        this.op = op;
        this.fieldId2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        Field tmp1 = t1.getField(fieldId1);
        Field tmp2 = t2.getField(fieldId2);
        return tmp1.compare(op, tmp2);
    }
    
    public int getField1()
    {
        // some code goes here
        return this.fieldId1;
    }
    
    public int getField2()
    {
        // some code goes here
        return this.fieldId2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return this.op;
    }
}
