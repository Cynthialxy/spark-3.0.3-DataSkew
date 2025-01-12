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

package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi

//noinspection ScalaStyle
@DeveloperApi
object TaskLocality extends Enumeration {  //继承枚举类，当调用Value方法时，Scala会为每个枚举值自动分配一个整型值（从0开始，依次递增）
  // Process local is expected to be used ONLY within TaskSetManager for now.
  val PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY = Value   //0，1，2，3，4

  type TaskLocality = Value

  def isAllowed(constraint: TaskLocality, condition: TaskLocality): Boolean = {
    condition <= constraint
  }
}
