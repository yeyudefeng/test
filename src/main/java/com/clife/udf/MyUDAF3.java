package com.clife.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;


/**
 * 实现udaf类，多进一出
 * https://www.yisu.com/zixun/62775.html
 * 自定义UDAF统计b字段大于30的记录个数 countbigthan(b,30)实现代码
 */
//继承类型检查类
public class MyUDAF3 extends AbstractGenericUDAFResolver {


    // 参数个数判断
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly two argument is expected");
        }

        return new GenericUDAFCountBigThanEvaluator();// 返回处理逻辑类
    }

    // 处理逻辑类
    public static class GenericUDAFCountBigThanEvaluator extends
            GenericUDAFEvaluator {
        private LongWritable result;
        private PrimitiveObjectInspector inputOI1;
        private PrimitiveObjectInspector inputOI2;

        // init方法map，reduce阶段都得执行
        // map阶段parameters长度与UDAF输入的参数个数有关
        // reduce阶段，parameters长度为1
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {

            result = new LongWritable(0);

            inputOI1 = (PrimitiveObjectInspector) parameters[0];
            if (parameters.length > 1) {
                inputOI2 = (PrimitiveObjectInspector) parameters[1];
            }

            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            // 最终结果返回类型

        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {

            CountAgg agg = new CountAgg();// 存放部分聚合值

            reset(agg);

            return agg;
        }

        // 缓存对象初始化
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            CountAgg countagg = (CountAgg) agg;
            countagg.count = 0;

        }

        // 具体逻辑
        // iterate只在map端运算
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 2);
            if (parameters == null || parameters[0] == null
                    || parameters[1] == null) {
                return;
            }

            double base = PrimitiveObjectInspectorUtils.getDouble(
                    parameters[0], inputOI1);
            double tmp = PrimitiveObjectInspectorUtils.getDouble(parameters[1],
                    inputOI2);

            if (base > tmp) {
                ((CountAgg) agg).count++;
            }
        }

        // map阶段返回部分结果
        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException {
            result.set(((CountAgg) agg).count);
            return result;
        }

        // 合并部分结果 map(含有Combiner)和reduce都执行,parial传递terminatePartial得到的部分结果
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {
                long p = PrimitiveObjectInspectorUtils.getLong(partial,
                        inputOI1);
                ((CountAgg) agg).count += p;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {

            result.set(((CountAgg) agg).count);
            return result;
        }

        public class CountAgg implements AggregationBuffer {

            long count;
        }

    }

}