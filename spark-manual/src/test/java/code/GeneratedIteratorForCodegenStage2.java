package code;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 * 2020/9/21
 */

final class GeneratedIteratorForCodegenStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
    private Object[] references;
    private scala.collection.Iterator[] inputs;
    private boolean sort_needToSort_0;
    private org.apache.spark.sql.execution.UnsafeExternalRowSorter sort_sorter_0;
    private org.apache.spark.executor.TaskMetrics sort_metrics_0;
    private scala.collection.Iterator<InternalRow> sort_sortedIter_0;
    private scala.collection.Iterator inputadapter_input_0;

    public GeneratedIteratorForCodegenStage2(Object[] references) {
        this.references = references;
    }

    public void init(int index, scala.collection.Iterator[] inputs) {
        partitionIndex = index;
        this.inputs = inputs;
        sort_needToSort_0 = true;
        sort_sorter_0 = ((org.apache.spark.sql.execution.SortExec) references[0] /* plan */).createSorter();
        sort_metrics_0 = org.apache.spark.TaskContext.get().taskMetrics();

        inputadapter_input_0 = inputs[0];

    }

    private void sort_addToSorter_0() throws java.io.IOException {
        while ( inputadapter_input_0.hasNext()) {
            InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();

            sort_sorter_0.insertRow((UnsafeRow)inputadapter_row_0);
// shouldStop check is eliminated
        }

    }

    protected void processNext() throws java.io.IOException {
        if (sort_needToSort_0) {
            long sort_spillSizeBefore_0 = sort_metrics_0.memoryBytesSpilled();
            sort_addToSorter_0();
            sort_sortedIter_0 = sort_sorter_0.sort();
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* sortTime */).add(sort_sorter_0.getSortTimeNanos() / 1000000);
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* peakMemory */).add(sort_sorter_0.getPeakMemoryUsage());
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* spillSize */).add(sort_metrics_0.memoryBytesSpilled() - sort_spillSizeBefore_0);
            sort_metrics_0.incPeakExecutionMemory(sort_sorter_0.getPeakMemoryUsage());
            sort_needToSort_0 = false;
        }

        while ( sort_sortedIter_0.hasNext()) {
            UnsafeRow sort_outputRow_0 = (UnsafeRow)sort_sortedIter_0.next();

            append(sort_outputRow_0);

            if (shouldStop()) return;
        }
    }

}
