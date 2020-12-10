// scalastyle:off
package org.apache.spark.sql.plugins

import java.util

import com.codahale.metrics.Counter
import org.apache.spark.TaskFailedReason
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}

object MetricPlugin {
	val counter = new Counter()
}

class MetricPlugin extends SparkPlugin {
	/**
		* Return the plugin's driver-side component.
		*
		* @return The driver-side component, or null if one is not needed.
		*/
	override def driverPlugin(): DriverPlugin = null

	/**
		* Return the plugin's executor-side component.
		*
		* @return The executor-side component, or null if one is not needed.
		*/
	override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
		/**
			* Initialize the executor plugin.
			* <p>
			* When a Spark plugin provides an executor plugin, this method will be called during the
			* initialization of the executor process. It will block executor initialization until it
			* returns.
			* <p>
			* Executor plugins that publish metrics should register all metrics with the context's
			* registry ({@link PluginContext#metricRegistry()}) when this method is called. Metrics
			* registered afterwards are not guaranteed to show up.
			*
			* @param ctx       Context information for the executor where the plugin is running.
			* @param extraConf Extra configuration provided by the driver component during its
			*                  initialization.
			*/
		override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
			 val metricRegistry = ctx.metricRegistry()
			 metricRegistry.register("evenMetrics", MetricPlugin.counter)
		}

		/**
			* Clean up and terminate this plugin.
			* <p>
			* This method is called during the executor shutdown phase, and blocks executor shutdown.
			*/
		override def shutdown(): Unit = {
			println("MetricPlugin shutdown")
		}

		/**
			* Perform any action before the task is run.
			* <p>
			* This method is invoked from the same thread the task will be executed.
			* Task-specific information can be accessed via {@link org.apache.spark.TaskContext#get}.
			* <p>
			* Plugin authors should avoid expensive operations here, as this method will be called
			* on every task, and doing something expensive can significantly slow down a job.
			* It is not recommended for a user to call a remote service, for example.
			* <p>
			* Exceptions thrown from this method do not propagate - they're caught,
			* logged, and suppressed. Therefore exceptions when executing this method won't
			* make the job fail.
			*
			* @since 3.1.0
			*/
		override def onTaskStart(): Unit = {
			println("MetricPlugin onTaskStart")
		}

		/**
			* Perform an action after tasks completes without exceptions.
			* <p>
			* As {@link #onTaskStart() onTaskStart} exceptions are suppressed, this method
			* will still be invoked even if the corresponding {@link #onTaskStart} call for this
			* task failed.
			* <p>
			* Same warnings of {@link #onTaskStart() onTaskStart} apply here.
			*
			* @since 3.1.0
			*/
		override def onTaskSucceeded(): Unit = {
			println("MetricPlugin onTaskSucceeded")
		}

		/**
			* Perform an action after tasks completes with exceptions.
			* <p>
			* Same warnings of {@link #onTaskStart() onTaskStart} apply here.
			*
			* @param failureReason the exception thrown from the failed task.
			* @since 3.1.0
			*/
		override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
			println("MetricPlugin onTaskFailed")
		}
	}
}
