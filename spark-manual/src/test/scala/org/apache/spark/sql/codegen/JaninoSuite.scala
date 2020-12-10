// scalastyle:off


package org.apache.spark.sql.codegen

import org.codehaus.commons.compiler.CompilerFactoryFactory
import org.codehaus.janino.ExpressionEvaluator
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Print
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratedClass


abstract class Bar {
    def eval(): Unit
}


class JaninoSuite extends SparkFunSuite {

    test("janino sample") {
        val ee = new ExpressionEvaluator()
        ee.cook("3 + 4")
        Print.printConsole(ee.evaluate(null))
    }

    test("janino scriptBody") {
        val scriptBody = s"""
               public static void method1() {
                 System.out.println(1111111) ;
               }
               method1();
        """
        val se = CompilerFactoryFactory.getDefaultCompilerFactory.newScriptEvaluator()
        se.cook(scriptBody)
        se.evaluate(null)
    }

    test("janino classBody") {
        val cbe = CompilerFactoryFactory.getDefaultCompilerFactory.newClassBodyEvaluator()
        cbe.setClassName("org.apache.spark.sql.catalyst.expressions.GeneratedClass")
        cbe.setExtendedClass(classOf[GeneratedClass])
        cbe.setDebuggingInformation(true, true, false)
        val classBody =
            s"""
               |public Foo generate(Object [] references) {
               |   return new Foo(references) ;
               |}
               |class Foo extends ${classOf[Bar].getName} {
               | private Object[] references ;
               | public Foo(Object[] references) {
               |   this.references = references ;
               | }
               | public void eval() {
               |   System.out.println("eval...") ;
               | }
               |}
             """.stripMargin

        Print.printConsole(classBody)

        cbe.cook("generated.java", classBody)
        val c = cbe.getClazz.getConstructor().newInstance().asInstanceOf[GeneratedClass]
        val foo = c.generate(Array.empty).asInstanceOf[Bar]
        foo.eval()
    }

}
