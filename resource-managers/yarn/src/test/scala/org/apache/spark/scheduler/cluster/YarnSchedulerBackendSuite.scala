/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.scheduler.cluster

import java.net.URL
import java.util.concurrent.atomic.AtomicReference
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.ui.TestFilter

class YarnSchedulerBackendSuite extends SparkFunSuite with MockitoSugar with LocalSparkContext {

  private var yarnSchedulerBackend: YarnSchedulerBackend = _

  override def afterEach(): Unit = {
    try {
      if (yarnSchedulerBackend != null) {
        yarnSchedulerBackend.stop()
      }
    } finally {
      super.afterEach()
    }
  }

  private class TestTaskSchedulerImpl(sc: SparkContext) extends TaskSchedulerImpl(sc) {
    val blacklistedNodes = new AtomicReference[Set[String]]()
    def setNodeBlacklist(nodeBlacklist: Set[String]): Unit = blacklistedNodes.set(nodeBlacklist)
    override def nodeBlacklist(): Set[String] = blacklistedNodes.get()
  }

  private class TestYarnSchedulerBackend(scheduler: TaskSchedulerImpl, sc: SparkContext)
      extends YarnSchedulerBackend(scheduler, sc) {
    def setHostToLocalTaskCount(hostToLocalTaskCount: Map[String, Int]): Unit = {
      this.rpHostToLocalTaskCount = Map(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID ->
        hostToLocalTaskCount)
    }
  }

  test("RequestExecutors reflects node blacklist and is serializable") {
    sc = new SparkContext("local", "YarnSchedulerBackendSuite")
    // Subclassing the TaskSchedulerImpl here instead of using Mockito. For details see SPARK-26891.
    val sched = new TestTaskSchedulerImpl(sc)
    val yarnSchedulerBackendExtended = new TestYarnSchedulerBackend(sched, sc)
    yarnSchedulerBackend = yarnSchedulerBackendExtended
    val ser = new JavaSerializer(sc.conf).newInstance()
    for {
      blacklist <- IndexedSeq(Set[String](), Set("a", "b", "c"))
      numRequested <- 0 until 10
      hostToLocalCount <- IndexedSeq(
        Map[String, Int](),
        Map("a" -> 1, "b" -> 2)
      )
    } {
      yarnSchedulerBackendExtended.setHostToLocalTaskCount(hostToLocalCount)
      sched.setNodeBlacklist(blacklist)
      val numReq = Map(ResourceProfile.getOrCreateDefaultProfile(sc.getConf) -> numRequested)
      val req = yarnSchedulerBackendExtended.prepareRequestExecutors(numReq)
      assert(req.requestedTotal === numRequested)
      assert(req.nodeBlacklist === blacklist)
      assert(req.hostToLocalTaskCount.keySet.intersect(blacklist).isEmpty)
      // Serialize to make sure serialization doesn't throw an error
      ser.serialize(req)
    }
  }

  test("Respect user filters when adding AM IP filter") {
    val conf = new SparkConf(false)
      .set("spark.ui.filters", classOf[TestFilter].getName())
      .set(s"spark.${classOf[TestFilter].getName()}.param.responseCode",
        HttpServletResponse.SC_BAD_GATEWAY.toString)

    sc = new SparkContext("local", "YarnSchedulerBackendSuite", conf)
    val sched = mock[TaskSchedulerImpl]
    when(sched.sc).thenReturn(sc)

    val url = new URL(sc.uiWebUrl.get)
    // Before adding the "YARN" filter, should get the code from the filter in SparkConf.
    assert(TestUtils.httpResponseCode(url) === HttpServletResponse.SC_BAD_GATEWAY)

    yarnSchedulerBackend = new YarnSchedulerBackend(sched, sc) { }

    yarnSchedulerBackend.addWebUIFilter(classOf[TestFilter2].getName(),
      Map("responseCode" -> HttpServletResponse.SC_NOT_ACCEPTABLE.toString), "")

    sc.ui.get.getDelegatingHandlers.foreach { h =>
      // Two filters above + security filter.
      assert(h.filterCount() === 3)
    }

    // The filter should have been added first in the chain, so we should get SC_NOT_ACCEPTABLE
    // instead of SC_OK.
    assert(TestUtils.httpResponseCode(url) === HttpServletResponse.SC_NOT_ACCEPTABLE)

    // Add a new handler and make sure the added filter is properly registered.
    val servlet = new HttpServlet() {
      override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_CONFLICT)
      }
    }

    sc.ui.get.attachHandler("/new-handler", servlet, "/")

    val newUrl = new URL(sc.uiWebUrl.get + "/new-handler/")
    assert(TestUtils.httpResponseCode(newUrl) === HttpServletResponse.SC_NOT_ACCEPTABLE)

    val bypassUrl = new URL(sc.uiWebUrl.get + "/new-handler/?bypass")
    assert(TestUtils.httpResponseCode(bypassUrl) === HttpServletResponse.SC_CONFLICT)
  }

}

// Just extend the test filter so we can configure two of them.
class TestFilter2 extends TestFilter
