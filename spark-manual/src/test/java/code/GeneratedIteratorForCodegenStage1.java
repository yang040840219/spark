package code;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * 2020/9/21
 */
final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
    private Object[] references;
    private scala.collection.Iterator[] inputs;
    private boolean sort_needToSort_0;
    private org.apache.spark.sql.execution.UnsafeExternalRowSorter sort_sorter_0;
    private org.apache.spark.executor.TaskMetrics sort_metrics_0;
    private scala.collection.Iterator<InternalRow> sort_sortedIter_0;
    private int columnartorow_batchIdx_0;
    private boolean serializefromobject_resultIsNull_0;
    private boolean serializefromobject_resultIsNull_1;
    private java.lang.String[] serializefromobject_mutableStateArray_0 = new java.lang.String[2];
    private org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[] columnartorow_mutableStateArray_2 = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[2];
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] columnartorow_mutableStateArray_3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[10];
    private org.apache.spark.sql.vectorized.ColumnarBatch[] columnartorow_mutableStateArray_1 = new org.apache.spark.sql.vectorized.ColumnarBatch[1];
    private scala.collection.Iterator[] columnartorow_mutableStateArray_0 = new scala.collection.Iterator[1];
    private org.apache.spark.sql.Row[] mapelements_mutableStateArray_0 = new org.apache.spark.sql.Row[1];

    public GeneratedIteratorForCodegenStage1(Object[] references) {
        this.references = references;
    }

    public void init(int index, scala.collection.Iterator[] inputs) {
        partitionIndex = index;
        this.inputs = inputs;
        wholestagecodegen_init_0_0();
        wholestagecodegen_init_0_1();

    }

    private void mapelements_doConsume_0(org.apache.spark.sql.Row mapelements_expr_0_0, boolean mapelements_exprIsNull_0_0) throws java.io.IOException {
        boolean mapelements_isNull_1 = true;
        scala.Tuple2 mapelements_value_1 = null;
        if (!false) {
            mapelements_mutableStateArray_0[0] = mapelements_expr_0_0;

            mapelements_isNull_1 = false;
            if (!mapelements_isNull_1) {
                Object mapelements_funcResult_0 = null;
                mapelements_funcResult_0 = ((scala.Function1) references[5] /* literal */).apply(mapelements_mutableStateArray_0[0]);

                if (mapelements_funcResult_0 != null) {
                    mapelements_value_1 = (scala.Tuple2) mapelements_funcResult_0;
                } else {
                    mapelements_isNull_1 = true;
                }

            }
        }

        serializefromobject_doConsume_0(mapelements_value_1, mapelements_isNull_1);

    }

    private void wholestagecodegen_init_0_1() {
        columnartorow_mutableStateArray_3[7] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
        columnartorow_mutableStateArray_3[8] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 64);
        columnartorow_mutableStateArray_3[9] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 64);

    }

    private void sort_addToSorter_0() throws java.io.IOException {
        if (columnartorow_mutableStateArray_1[0] == null) {
            columnartorow_nextBatch_0();
        }
        while ( columnartorow_mutableStateArray_1[0] != null) {
            int columnartorow_numRows_0 = columnartorow_mutableStateArray_1[0].numRows();
            int columnartorow_localEnd_0 = columnartorow_numRows_0 - columnartorow_batchIdx_0;
            for (int columnartorow_localIdx_0 = 0; columnartorow_localIdx_0 < columnartorow_localEnd_0; columnartorow_localIdx_0++) {
                int columnartorow_rowIdx_0 = columnartorow_batchIdx_0 + columnartorow_localIdx_0;
                do {
                    boolean columnartorow_isNull_1 = columnartorow_mutableStateArray_2[1].isNullAt(columnartorow_rowIdx_0);
                    UTF8String columnartorow_value_1 = columnartorow_isNull_1 ? null : (columnartorow_mutableStateArray_2[1].getUTF8String(columnartorow_rowIdx_0));

                    boolean filter_value_2 = !columnartorow_isNull_1;
                    if (!filter_value_2) continue;

                    boolean filter_isNull_2 = true;
                    boolean filter_value_3 = false;
                    boolean filter_isNull_3 = columnartorow_isNull_1;
                    int filter_value_4 = -1;
                    if (!columnartorow_isNull_1) {
                        UTF8String.IntWrapper filter_intWrapper_0 = new UTF8String.IntWrapper();
                        if (columnartorow_value_1.toInt(filter_intWrapper_0)) {
                            filter_value_4 = filter_intWrapper_0.value;
                        } else {
                            filter_isNull_3 = true;
                        }
                        filter_intWrapper_0 = null;
                    }
                    if (!filter_isNull_3) {
                        filter_isNull_2 = false; // resultCode could change nullability.
                        filter_value_3 = filter_value_4 > 2;

                    }
                    if (filter_isNull_2 || !filter_value_3) continue;

                    ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* numOutputRows */).add(1);

                    boolean columnartorow_isNull_0 = columnartorow_mutableStateArray_2[0].isNullAt(columnartorow_rowIdx_0);
                    UTF8String columnartorow_value_0 = columnartorow_isNull_0 ? null : (columnartorow_mutableStateArray_2[0].getUTF8String(columnartorow_rowIdx_0));

                    deserializetoobject_doConsume_0(columnartorow_value_0, columnartorow_isNull_0, columnartorow_value_1, false);

                } while(false);
// shouldStop check is eliminated
            }
            columnartorow_batchIdx_0 = columnartorow_numRows_0;
            columnartorow_mutableStateArray_1[0] = null;
            columnartorow_nextBatch_0();
        }

    }

    private void deserializetoobject_doConsume_0(UTF8String deserializetoobject_expr_0_0, boolean deserializetoobject_exprIsNull_0_0, UTF8String deserializetoobject_expr_1_0, boolean deserializetoobject_exprIsNull_1_0) throws java.io.IOException {
        Object[] deserializetoobject_values_0 = new Object[2];

        boolean deserializetoobject_isNull_3 = true;
        java.lang.String deserializetoobject_value_3 = null;
        if (!deserializetoobject_exprIsNull_0_0) {
            deserializetoobject_isNull_3 = false;
            if (!deserializetoobject_isNull_3) {
                Object deserializetoobject_funcResult_0 = null;
                deserializetoobject_funcResult_0 = deserializetoobject_expr_0_0.toString();
                deserializetoobject_value_3 = (java.lang.String) deserializetoobject_funcResult_0;

            }
        }
        if (deserializetoobject_isNull_3) {
            deserializetoobject_values_0[0] = null;
        } else {
            deserializetoobject_values_0[0] = deserializetoobject_value_3;
        }

        boolean deserializetoobject_isNull_5 = true;
        java.lang.String deserializetoobject_value_5 = null;
        if (!deserializetoobject_exprIsNull_1_0) {
            deserializetoobject_isNull_5 = false;
            if (!deserializetoobject_isNull_5) {
                Object deserializetoobject_funcResult_1 = null;
                deserializetoobject_funcResult_1 = deserializetoobject_expr_1_0.toString();
                deserializetoobject_value_5 = (java.lang.String) deserializetoobject_funcResult_1;

            }
        }
        if (deserializetoobject_isNull_5) {
            deserializetoobject_values_0[1] = null;
        } else {
            deserializetoobject_values_0[1] = deserializetoobject_value_5;
        }

        final org.apache.spark.sql.Row deserializetoobject_value_2 = new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(deserializetoobject_values_0, ((org.apache.spark.sql.types.StructType) references[4] /* schema */));

        mapelements_doConsume_0(deserializetoobject_value_2, false);

    }

    private void columnartorow_nextBatch_0() throws java.io.IOException {
        if (columnartorow_mutableStateArray_0[0].hasNext()) {
            columnartorow_mutableStateArray_1[0] = (org.apache.spark.sql.vectorized.ColumnarBatch)columnartorow_mutableStateArray_0[0].next();
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numInputBatches */).add(1);
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* numOutputRows */).add(columnartorow_mutableStateArray_1[0].numRows());
            columnartorow_batchIdx_0 = 0;
            columnartorow_mutableStateArray_2[0] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(0);
            columnartorow_mutableStateArray_2[1] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(1);

        }
    }

    private void serializefromobject_doConsume_0(scala.Tuple2 serializefromobject_expr_0_0, boolean serializefromobject_exprIsNull_0_0) throws java.io.IOException {
        serializefromobject_resultIsNull_0 = false;

        if (!serializefromobject_resultIsNull_0) {
            if (serializefromobject_exprIsNull_0_0) {
                throw new NullPointerException(((java.lang.String) references[6] /* errMsg */));
            }
            boolean serializefromobject_isNull_2 = true;
            java.lang.String serializefromobject_value_2 = null;
            if (!false) {
                serializefromobject_isNull_2 = false;
                if (!serializefromobject_isNull_2) {
                    Object serializefromobject_funcResult_0 = null;
                    serializefromobject_funcResult_0 = serializefromobject_expr_0_0._1();

                    if (serializefromobject_funcResult_0 != null) {
                        serializefromobject_value_2 = (java.lang.String) serializefromobject_funcResult_0;
                    } else {
                        serializefromobject_isNull_2 = true;
                    }

                }
            }
            serializefromobject_resultIsNull_0 = serializefromobject_isNull_2;
            serializefromobject_mutableStateArray_0[0] = serializefromobject_value_2;
        }

        boolean serializefromobject_isNull_1 = serializefromobject_resultIsNull_0;
        UTF8String serializefromobject_value_1 = null;
        if (!serializefromobject_resultIsNull_0) {
            serializefromobject_value_1 = org.apache.spark.unsafe.types.UTF8String.fromString(serializefromobject_mutableStateArray_0[0]);
        }
        serializefromobject_resultIsNull_1 = false;

        if (!serializefromobject_resultIsNull_1) {
            if (serializefromobject_exprIsNull_0_0) {
                throw new NullPointerException(((java.lang.String) references[7] /* errMsg */));
            }
            boolean serializefromobject_isNull_7 = true;
            java.lang.String serializefromobject_value_7 = null;
            if (!false) {
                serializefromobject_isNull_7 = false;
                if (!serializefromobject_isNull_7) {
                    Object serializefromobject_funcResult_1 = null;
                    serializefromobject_funcResult_1 = serializefromobject_expr_0_0._2();

                    if (serializefromobject_funcResult_1 != null) {
                        serializefromobject_value_7 = (java.lang.String) serializefromobject_funcResult_1;
                    } else {
                        serializefromobject_isNull_7 = true;
                    }

                }
            }
            serializefromobject_resultIsNull_1 = serializefromobject_isNull_7;
            serializefromobject_mutableStateArray_0[1] = serializefromobject_value_7;
        }

        boolean serializefromobject_isNull_6 = serializefromobject_resultIsNull_1;
        UTF8String serializefromobject_value_6 = null;
        if (!serializefromobject_resultIsNull_1) {
            serializefromobject_value_6 = org.apache.spark.unsafe.types.UTF8String.fromString(serializefromobject_mutableStateArray_0[1]);
        }
        columnartorow_mutableStateArray_3[9].reset();

        columnartorow_mutableStateArray_3[9].zeroOutNullBytes();

        if (serializefromobject_isNull_1) {
            columnartorow_mutableStateArray_3[9].setNullAt(0);
        } else {
            columnartorow_mutableStateArray_3[9].write(0, serializefromobject_value_1);
        }

        if (serializefromobject_isNull_6) {
            columnartorow_mutableStateArray_3[9].setNullAt(1);
        } else {
            columnartorow_mutableStateArray_3[9].write(1, serializefromobject_value_6);
        }
        sort_sorter_0.insertRow((UnsafeRow)(columnartorow_mutableStateArray_3[9].getRow()));

    }

    protected void processNext() throws java.io.IOException {
        if (sort_needToSort_0) {
            long sort_spillSizeBefore_0 = sort_metrics_0.memoryBytesSpilled();
            sort_addToSorter_0();
            sort_sortedIter_0 = sort_sorter_0.sort();
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[10] /* sortTime */).add(sort_sorter_0.getSortTimeNanos() / 1000000);
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[8] /* peakMemory */).add(sort_sorter_0.getPeakMemoryUsage());
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[9] /* spillSize */).add(sort_metrics_0.memoryBytesSpilled() - sort_spillSizeBefore_0);
            sort_metrics_0.incPeakExecutionMemory(sort_sorter_0.getPeakMemoryUsage());
            sort_needToSort_0 = false;
        }

        while ( sort_sortedIter_0.hasNext()) {
            UnsafeRow sort_outputRow_0 = (UnsafeRow)sort_sortedIter_0.next();

            append(sort_outputRow_0);

            if (shouldStop()) return;
        }
    }

    private void wholestagecodegen_init_0_0() {
        sort_needToSort_0 = true;
        sort_sorter_0 = ((org.apache.spark.sql.execution.SortExec) references[0] /* plan */).createSorter();
        sort_metrics_0 = org.apache.spark.TaskContext.get().taskMetrics();

        columnartorow_mutableStateArray_0[0] = inputs[0];
        columnartorow_mutableStateArray_3[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 64);
        columnartorow_mutableStateArray_3[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 64);
        columnartorow_mutableStateArray_3[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 64);
        columnartorow_mutableStateArray_3[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 64);
        columnartorow_mutableStateArray_3[4] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
        columnartorow_mutableStateArray_3[5] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
        columnartorow_mutableStateArray_3[6] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);

    }

}