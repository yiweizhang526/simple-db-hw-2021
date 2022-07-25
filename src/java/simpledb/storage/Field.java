package simpledb.storage;

import simpledb.execution.Predicate;
import simpledb.common.Type;

import java.io.*;

/**
 * Interface for values of fields in tuples in SimpleDB.
 */
public interface Field extends Serializable{
    /**
     * 接口的注释说明就知道，当我们让实体类实现Serializable接口时，
     *  其实是在告诉JVM此类可被序列化，可被默认的序列化机制序列化。
     *
     *    序列化是将对象状态转换为可保持或传输的格式的过程。与序列化相对的是反序列化，
     *    它将流转换为对象。这两个过程结合起来，可以轻松地存储和传输数据。
     *
     *    序列化作用
     *    提供一种简单又可扩展的对象保存恢复机制。
     *    对于远程调用，能方便对对象进行编码和解码，就像实现对象直接传输。
     *    可以将对象持久化到介质中，就像实现对象直接存储。
     *    允许对象自定义外部存储的格式。
     * Write the bytes representing this field to the specified
     * DataOutputStream.
     * @see DataOutputStream
     * @param dos The DataOutputStream to write to.
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * Compare the value of this field object to the passed in value.
     * @param op The operator
     * @param value The value to compare this Field to
     * @return Whether or not the comparison yields true.
     */
    boolean compare(Predicate.Op op, Field value);

    /**
     * Returns the type of this field (see {@link Type#INT_TYPE} or {@link Type#STRING_TYPE}
     * @return type of this field
     */
    Type getType();
    
    /**
     * Hash code.
     * Different Field objects representing the same value should probably
     * return the same hashCode.
     */
    int hashCode();
    boolean equals(Object field);

    String toString();
}
