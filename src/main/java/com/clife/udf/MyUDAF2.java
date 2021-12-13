//package com.clife.udf;
//
//import org.apache.hadoop.hive.ql.exec.UDAF;
//import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
//
//import java.util.List;
//
///**
// * 实现udaf类，多进一出
// * 计算消费金额前三的平均值
// */
//public class MyUDAF2 extends UDAF {
//
//    public static class AvgState {
//        private List<Double> topNList;
//        private double mSum;
//
//    }
//    public static class AvgEvaluator implements UDAFEvaluator {
//        AvgState state;
//
//        public AvgEvaluator() {
//            super();
//            state = new AvgState();
//            init();
//        }
//
//
//        public void init() {
//            state.mSum = 0;
//            state.mCount = 0;
//        }
//
//
//        /**
//         * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean * * @param o * @return
//         */
//
//        public boolean iterate(Double o, Integer n) {
//            if (o != null) {
//                state.mSum += o;
//                state.mCount++;
//            }
//            return true;
//        }
//
//        /**
//         * terminatePartial无参数，其为iterate函数遍历结束后，返回轮转数据， * terminatePartial类似于hadoop的Combiner * * @return
//         */
//
//        public AvgState terminatePartial() {
//            // combiner
//            return state.mCount == 0 ? null : state;
//        }
//
//        /**
//         * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean * * @param o * @return
//         */
//
//        public boolean merge(AvgState avgState) {
//            if (avgState != null) {
//                state.mCount += avgState.mCount;
//                state.mSum += avgState.mSum;
//            }
//            return true;
//        }
//
//        /**
//         * terminate返回最终的聚集函数结果 * * @return
//         */
//        public Double terminate() {
//            return state.mCount == 0 ? null : Double.valueOf(state.mSum / state.mCount);
//        }
//    }
//}
