package org.apache.spark.sql.test;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.ScriptEvaluator;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

/**
 * 2020/2/15
 */
public class CodeGenTest {

    @Test
    public void expressionTest() throws CompileException, InvocationTargetException {
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
        expressionEvaluator.setParameters(new String[]{"a", "b"}, new Class[]{int.class, int.class});
        expressionEvaluator.setExpressionType(int.class);
        expressionEvaluator.cook("a + b");
        int result = (int)(expressionEvaluator.evaluate(new Object[]{1, 2}));
        System.out.println(result);
    }

    @Test
    public void methodTest() throws CompileException, InvocationTargetException {
        ScriptEvaluator scriptEvaluator = new ScriptEvaluator();
        String method = "static void method(){ System.out.println(\"hello, world\");}; method();";
        scriptEvaluator.cook(method);
        scriptEvaluator.evaluate(new Object[0]);

    }
}
