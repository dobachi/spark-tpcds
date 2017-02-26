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

package net.dobachi.tpcds.gendata

import org.apache.spark.sql.SparkSession

/**
  * Table definition of facts data
  */
class FactsTables(implicit spark: SparkSession) extends Serializable {
  import spark.sqlContext.implicits._

  val definitions = Seq(
    Table("store_sales",
      partitionColumns = "ss_sold_date_sk" :: Nil,
      'ss_sold_date_sk      .int,
      'ss_sold_time_sk      .int,
      'ss_item_sk           .int,
      'ss_customer_sk       .int,
      'ss_cdemo_sk          .int,
      'ss_hdemo_sk          .int,
      'ss_addr_sk           .int,
      'ss_store_sk          .int,
      'ss_promo_sk          .int,
      'ss_ticket_number     .int,
      'ss_quantity          .int,
      'ss_wholesale_cost    .decimal(7,2),
      'ss_list_price        .decimal(7,2),
      'ss_sales_price       .decimal(7,2),
      'ss_ext_discount_amt  .decimal(7,2),
      'ss_ext_sales_price   .decimal(7,2),
      'ss_ext_wholesale_cost.decimal(7,2),
      'ss_ext_list_price    .decimal(7,2),
      'ss_ext_tax           .decimal(7,2),
      'ss_coupon_amt        .decimal(7,2),
      'ss_net_paid          .decimal(7,2),
      'ss_net_paid_inc_tax  .decimal(7,2),
      'ss_net_profit        .decimal(7,2))
  )

}
