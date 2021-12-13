package com.clife.udf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class GetSetUDAF extends AbstractGenericUDAFResolver {
    public GetSetUDAF() {
    }

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        } else {
            ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
            if (oi.getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(0, "Argument must be PRIMITIVE, but " + oi.getCategory().name() + " was passed.");
            } else {
                PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector)oi;
                if (inputOI.getPrimitiveCategory() != PrimitiveCategory.STRING) {
                    throw new UDFArgumentTypeException(0, "Argument must be String, but " + inputOI.getPrimitiveCategory().name() + " was passed.");
                } else {
                    return new GetSetUDAF.GetSetEvaluator();
                }
            }
        }
    }

    public static class GetSetEvaluator extends GenericUDAFEvaluator {
        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector stringOI;
        Set<String> total = new HashSet();
        private boolean warned = false;

        public GetSetEvaluator() {
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert parameters.length == 1;

            super.init(m, parameters);
            if (m != Mode.PARTIAL1 && m != Mode.COMPLETE) {
                this.stringOI = (PrimitiveObjectInspector)parameters[0];
            } else {
                this.inputOI = (PrimitiveObjectInspector)parameters[0];
            }

            this.outputOI = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorOptions.JAVA);
            return this.outputOI;
        }

        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            GetSetUDAF.GetSetEvaluator.SumAgg result = new GetSetUDAF.GetSetEvaluator.SumAgg();
            return result;
        }

        public void reset(AggregationBuffer agg) throws HiveException {
            new GetSetUDAF.GetSetEvaluator.SumAgg();
        }

        //迭代处理原始数据parameters并保存到agg中
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert parameters.length == 1;

            if (parameters[0] != null) {
                GetSetUDAF.GetSetEvaluator.SumAgg myagg = (GetSetUDAF.GetSetEvaluator.SumAgg)agg;
                Object p1 = this.inputOI.getPrimitiveJavaObject(parameters[0]);
                String[] str1 = p1.toString().split(",");
                StringBuffer sb = new StringBuffer();
                if (str1.length == 1){
                    sb.append(str1[0]);
                }
                if (str1.length > 1){
                    for (int i = 0; i < str1.length; i++) {
                        for (int k = 0; k < str1.length; k++) {
                            if ( i >= k && str1[i].equals(str1[k])){
                                continue;
                            }
//                            if (str1[i].equals(str1[k])) {
//                                continue;
//                            }
                            String value =  str1[i]+'-'+str1[k];
                            sb.append(value).append(',');
                        }
                    }
                    String[] str2 = sb.deleteCharAt(sb.length() - 1).toString().split(",");
                    Set<String> set1 = new HashSet(Arrays.asList(str2));
                    myagg.add(set1);
                }else{
                    Set<String> set2 = new HashSet();
                    myagg.add(set2);
                }
            }

        }

        //以持久化的方式返回agg表示的部分聚合结果，这里的持久化意味着返回值只能Java基础类型、数组、基础类型包装器、Hadoop的Writables、Lists和Maps
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            GetSetUDAF.GetSetEvaluator.SumAgg myagg = (GetSetUDAF.GetSetEvaluator.SumAgg)agg;
            this.total.addAll(myagg.sum);
            return StringUtils.join(this.total.toArray(), ",");
        }

        //合并由partial表示的部分聚合结果到agg中
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
//            if (partial != null) {
//                GetSetUDAF.GetSetEvaluator.SumAgg myagg1 = (GetSetUDAF.GetSetEvaluator.SumAgg)agg;
//                String str = (String)this.stringOI.getPrimitiveJavaObject(partial);
//                String[] array = str.split(",");
//                Set partialSum = new HashSet(Arrays.asList(array));
//                GetSetUDAF.GetSetEvaluator.SumAgg myagg2 = new GetSetUDAF.GetSetEvaluator.SumAgg();
//                myagg2.add(partialSum);
//                myagg1.add(myagg2.sum);
//            }

        }

        //返回最终结果
        public Object terminate(AggregationBuffer agg) throws HiveException {
            GetSetUDAF.GetSetEvaluator.SumAgg myagg = (GetSetUDAF.GetSetEvaluator.SumAgg)agg;
            this.total = myagg.sum;
            Object[] array = this.total.toArray();
            String splitSet = StringUtils.join(array, ",");
            return splitSet;
        }

        static class SumAgg implements AggregationBuffer {
            Set<String> sum = new HashSet();

            SumAgg() {
            }

            void add(Set num) {
                this.sum.addAll(num);
            }
        }
    }

}
