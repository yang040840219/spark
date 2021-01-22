package code;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.types.UTF8String;

/* 005 */ // codegenStageId=1
/* 006 */ final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
    /* 007 */   private Object[] references;
    /* 008 */   private scala.collection.Iterator[] inputs;
    /* 009 */   private boolean agg_initAgg_0;
    /* 010 */   private org.apache.spark.unsafe.KVIterator agg_mapIter_0;
    /* 011 */   private org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap agg_hashMap_0;
    /* 012 */   private org.apache.spark.sql.execution.UnsafeKVExternalSorter agg_sorter_0;
    /* 013 */   private scala.collection.Iterator inputadapter_input_0;
    /* 014 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] expand_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[4];
    /* 015 */
    /* 016 */   public GeneratedIteratorForCodegenStage1(Object[] references) {
        /* 017 */     this.references = references;
        /* 018 */   }
    /* 019 */
    /* 020 */   public void init(int index, scala.collection.Iterator[] inputs) {
        /* 021 */     partitionIndex = index;
        /* 022 */     this.inputs = inputs;
        /* 023 */
        /* 024 */     inputadapter_input_0 = inputs[0];
        /* 025 */     expand_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 96);
        /* 026 */     expand_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 96);
        /* 027 */     expand_mutableStateArray_0[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 96);
        /* 028 */     expand_mutableStateArray_0[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 96);
        /* 029 */
        /* 030 */   }
    /* 031 */
    /* 032 */   private void expand_doConsume_0(InternalRow inputadapter_row_0, UTF8String expand_expr_0_0, boolean expand_exprIsNull_0_0, UTF8String expand_expr_1_0, boolean expand_exprIsNull_1_0, UTF8String expand_expr_2_0, boolean expand_exprIsNull_2_0) throws java.io.IOException {
        /* 033 */     boolean expand_isNull_1 = true;
        /* 034 */     UTF8String expand_value_1 =
                /* 035 */     null;
        /* 036 */     boolean expand_isNull_2 = true;
        /* 037 */     UTF8String expand_value_2 =
                /* 038 */     null;
        /* 039 */     boolean expand_isNull_3 = true;
        /* 040 */     int expand_value_3 =
                /* 041 */     -1;
        /* 042 */     for (int expand_i_0 = 0; expand_i_0 < 2; expand_i_0 ++) {
            /* 043 */       switch (expand_i_0) {
                /* 044 */       case 0:
                    /* 045 */         expand_isNull_1 = expand_exprIsNull_1_0;
                    /* 046 */         expand_value_1 = expand_expr_1_0;
                    /* 047 */
                    /* 048 */         expand_isNull_2 = true;
                    /* 049 */         expand_value_2 = ((UTF8String)null);
                    /* 050 */
                    /* 051 */         expand_isNull_3 = false;
                    /* 052 */         expand_value_3 = 1;
                    /* 053 */         break;
                /* 054 */
                /* 055 */       case 1:
                    /* 056 */         expand_isNull_1 = true;
                    /* 057 */         expand_value_1 = ((UTF8String)null);
                    /* 058 */
                    /* 059 */         expand_isNull_2 = expand_exprIsNull_0_0;
                    /* 060 */         expand_value_2 = expand_expr_0_0;
                    /* 061 */
                    /* 062 */         expand_isNull_3 = false;
                    /* 063 */         expand_value_3 = 2;
                    /* 064 */         break;
                /* 065 */       }
            /* 066 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[4] /* numOutputRows */).add(1);
            /* 067 */
            /* 068 */       agg_doConsume_0(expand_expr_2_0, expand_exprIsNull_2_0, expand_value_1, expand_isNull_1, expand_value_2, expand_isNull_2, expand_value_3);
            /* 069 */
            /* 070 */     }
        /* 071 */
        /* 072 */   }
    /* 073 */
    /* 074 */   private void agg_doAggregateWithKeysOutput_0(UnsafeRow agg_keyTerm_0, UnsafeRow agg_bufferTerm_0)
    /* 075 */   throws java.io.IOException {
        /* 076 */     ((org.apache.spark.sql.execution.metric.SQLMetric) references[5] /* numOutputRows */).add(1);
        /* 077 */
        /* 078 */     boolean agg_isNull_12 = agg_keyTerm_0.isNullAt(0);
        /* 079 */     UTF8String agg_value_12 = agg_isNull_12 ?
                /* 080 */     null : (agg_keyTerm_0.getUTF8String(0));
        /* 081 */     boolean agg_isNull_13 = agg_keyTerm_0.isNullAt(1);
        /* 082 */     UTF8String agg_value_13 = agg_isNull_13 ?
                /* 083 */     null : (agg_keyTerm_0.getUTF8String(1));
        /* 084 */     boolean agg_isNull_14 = agg_keyTerm_0.isNullAt(2);
        /* 085 */     UTF8String agg_value_14 = agg_isNull_14 ?
                /* 086 */     null : (agg_keyTerm_0.getUTF8String(2));
        /* 087 */     int agg_value_15 = agg_keyTerm_0.getInt(3);
        /* 088 */     expand_mutableStateArray_0[3].reset();
        /* 089 */
        /* 090 */     expand_mutableStateArray_0[3].zeroOutNullBytes();
        /* 091 */
        /* 092 */     if (agg_isNull_12) {
            /* 093 */       expand_mutableStateArray_0[3].setNullAt(0);
            /* 094 */     } else {
            /* 095 */       expand_mutableStateArray_0[3].write(0, agg_value_12);
            /* 096 */     }
        /* 097 */
        /* 098 */     if (agg_isNull_13) {
            /* 099 */       expand_mutableStateArray_0[3].setNullAt(1);
            /* 100 */     } else {
            /* 101 */       expand_mutableStateArray_0[3].write(1, agg_value_13);
            /* 102 */     }
        /* 103 */
        /* 104 */     if (agg_isNull_14) {
            /* 105 */       expand_mutableStateArray_0[3].setNullAt(2);
            /* 106 */     } else {
            /* 107 */       expand_mutableStateArray_0[3].write(2, agg_value_14);
            /* 108 */     }
        /* 109 */
        /* 110 */     expand_mutableStateArray_0[3].write(3, agg_value_15);
        /* 111 */     append((expand_mutableStateArray_0[3].getRow()));
        /* 112 */
        /* 113 */   }
    /* 114 */
    /* 115 */   private void agg_doConsume_0(UTF8String agg_expr_0_0, boolean agg_exprIsNull_0_0, UTF8String agg_expr_1_0, boolean agg_exprIsNull_1_0, UTF8String agg_expr_2_0, boolean agg_exprIsNull_2_0, int agg_expr_3_0) throws java.io.IOException {
        /* 116 */     UnsafeRow agg_unsafeRowAggBuffer_0 = null;
        /* 117 */
        /* 118 */     // generate grouping key
        /* 119 */     expand_mutableStateArray_0[2].reset();
        /* 120 */
        /* 121 */     expand_mutableStateArray_0[2].zeroOutNullBytes();
        /* 122 */
        /* 123 */     if (agg_exprIsNull_0_0) {
            /* 124 */       expand_mutableStateArray_0[2].setNullAt(0);
            /* 125 */     } else {
            /* 126 */       expand_mutableStateArray_0[2].write(0, agg_expr_0_0);
            /* 127 */     }
        /* 128 */
        /* 129 */     if (agg_exprIsNull_1_0) {
            /* 130 */       expand_mutableStateArray_0[2].setNullAt(1);
            /* 131 */     } else {
            /* 132 */       expand_mutableStateArray_0[2].write(1, agg_expr_1_0);
            /* 133 */     }
        /* 134 */
        /* 135 */     if (agg_exprIsNull_2_0) {
            /* 136 */       expand_mutableStateArray_0[2].setNullAt(2);
            /* 137 */     } else {
            /* 138 */       expand_mutableStateArray_0[2].write(2, agg_expr_2_0);
            /* 139 */     }
        /* 140 */
        /* 141 */     expand_mutableStateArray_0[2].write(3, agg_expr_3_0);
        /* 142 */     int agg_unsafeRowKeyHash_0 = (expand_mutableStateArray_0[2].getRow()).hashCode();
        /* 143 */     if (true) {
            /* 144 */       // try to get the buffer from hash map
            /* 145 */       agg_unsafeRowAggBuffer_0 =
                    /* 146 */       agg_hashMap_0.getAggregationBufferFromUnsafeRow((expand_mutableStateArray_0[2].getRow()), agg_unsafeRowKeyHash_0);
            /* 147 */     }
        /* 148 */     // Can't allocate buffer from the hash map. Spill the map and fallback to sort-based
        /* 149 */     // aggregation after processing all input rows.
        /* 150 */     if (agg_unsafeRowAggBuffer_0 == null) {
            /* 151 */       if (agg_sorter_0 == null) {
                /* 152 */         agg_sorter_0 = agg_hashMap_0.destructAndCreateExternalSorter();
                /* 153 */       } else {
                /* 154 */         agg_sorter_0.merge(agg_hashMap_0.destructAndCreateExternalSorter());
                /* 155 */       }
            /* 156 */
            /* 157 */       // the hash map had be spilled, it should have enough memory now,
            /* 158 */       // try to allocate buffer again.
            /* 159 */       agg_unsafeRowAggBuffer_0 = agg_hashMap_0.getAggregationBufferFromUnsafeRow(
                    /* 160 */         (expand_mutableStateArray_0[2].getRow()), agg_unsafeRowKeyHash_0);
            /* 161 */       if (agg_unsafeRowAggBuffer_0 == null) {
                /* 162 */         // failed to allocate the first page
                /* 163 */         throw new org.apache.spark.memory.SparkOutOfMemoryError("No enough memory for aggregation");
                /* 164 */       }
            /* 165 */     }
        /* 166 */
        /* 167 */     // common sub-expressions
        /* 168 */
        /* 169 */     // evaluate aggregate functions and update aggregation buffers
        /* 170 */
        /* 171 */   }
    /* 172 */
    /* 173 */   private void agg_doAggregateWithKeys_0() throws java.io.IOException {
        /* 174 */     while ( inputadapter_input_0.hasNext()) {
            /* 175 */       InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();
            /* 176 */
            /* 177 */       boolean inputadapter_isNull_0 = inputadapter_row_0.isNullAt(0);
            /* 178 */       UTF8String inputadapter_value_0 = inputadapter_isNull_0 ?
                    /* 179 */       null : (inputadapter_row_0.getUTF8String(0));
            /* 180 */       boolean inputadapter_isNull_1 = inputadapter_row_0.isNullAt(1);
            /* 181 */       UTF8String inputadapter_value_1 = inputadapter_isNull_1 ?
                    /* 182 */       null : (inputadapter_row_0.getUTF8String(1));
            /* 183 */       boolean inputadapter_isNull_2 = inputadapter_row_0.isNullAt(2);
            /* 184 */       UTF8String inputadapter_value_2 = inputadapter_isNull_2 ?
                    /* 185 */       null : (inputadapter_row_0.getUTF8String(2));
            /* 186 */
            /* 187 */       expand_doConsume_0(inputadapter_row_0, inputadapter_value_0, inputadapter_isNull_0, inputadapter_value_1, inputadapter_isNull_1, inputadapter_value_2, inputadapter_isNull_2);
            /* 188 */       // shouldStop check is eliminated
            /* 189 */     }
        /* 190 */
        /* 191 */     agg_mapIter_0 = ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).finishAggregate(agg_hashMap_0, agg_sorter_0, ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* peakMemory */), ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* spillSize */), ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* avgHashProbe */));
        /* 192 */   }
    /* 193 */
    /* 194 */   protected void processNext() throws java.io.IOException {
        /* 195 */     if (!agg_initAgg_0) {
            /* 196 */       agg_initAgg_0 = true;
            /* 197 */
            /* 198 */       agg_hashMap_0 = ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).createHashMap();
            /* 199 */       long wholestagecodegen_beforeAgg_0 = System.nanoTime();
            /* 200 */       agg_doAggregateWithKeys_0();
            /* 201 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[6] /* aggTime */).add((System.nanoTime() - wholestagecodegen_beforeAgg_0) / 1000000);
            /* 202 */     }
        /* 203 */     // output the result
        /* 204 */
        /* 205 */     while ( agg_mapIter_0.next()) {
            /* 206 */       UnsafeRow agg_aggKey_0 = (UnsafeRow) agg_mapIter_0.getKey();
            /* 207 */       UnsafeRow agg_aggBuffer_0 = (UnsafeRow) agg_mapIter_0.getValue();
            /* 208 */       agg_doAggregateWithKeysOutput_0(agg_aggKey_0, agg_aggBuffer_0);
            /* 209 */       if (shouldStop()) return;
            /* 210 */     }
        /* 211 */     agg_mapIter_0.close();
        /* 212 */     if (agg_sorter_0 == null) {
            /* 213 */       agg_hashMap_0.free();
            /* 214 */     }
        /* 215 */   }
    /* 216 */
    /* 217 */ }