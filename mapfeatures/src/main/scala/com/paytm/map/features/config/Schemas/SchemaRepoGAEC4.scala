package com.paytm.map.features.config.Schemas

import com.paytm.map.features.base.AggFeaturesL3EC4
import org.apache.spark.sql.types._

object SchemaRepoGAEC4 {

  private val operatorTypeDerived = ArrayType(StructType(StructField("Operator", StringType, nullable = true) :: StructField("count", LongType, nullable = true) :: Nil), containsNull = true)

  val EC4Cats = AggFeaturesL3EC4.requiredCategoryListL4

  val L3EC4Schema = StructType(
    Seq(
      StructField("customer_id", LongType, nullable = true),
      StructField("dt", DateType, nullable = true)
    ) ++
      EC4Cats.map("CAT" + _).flatMap(cat => Seq(
        StructField(s"L3_EC4_${cat}_first_transaction_date", StringType, nullable = true),
        StructField(s"L3_EC4_${cat}_first_transaction_size", DoubleType, nullable = true),
        StructField(s"L3_EC4_${cat}_last_transaction_date", StringType, nullable = true),
        StructField(s"L3_EC4_${cat}_last_transaction_size", DoubleType, nullable = true),
        StructField(s"L3_EC4_${cat}_preferred_brand_count", operatorTypeDerived, nullable = true),
        StructField(s"L3_EC4_${cat}_preferred_brand_size", operatorTypeDerived, nullable = true),
        StructField(s"L3_EC4_${cat}_total_transaction_count", LongType, nullable = true),
        StructField(s"L3_EC4_${cat}_total_transaction_size", DoubleType, nullable = true)
      ))
  )

  val GAL3EC4Schema = StructType(
    Seq(
      StructField("customer_id", LongType, nullable = true),
      StructField("dt", DateType, nullable = true)
    ) ++
      EC4Cats.map("CAT" + _).flatMap(cat => Seq(
        StructField(s"L3GA_EC4_${cat}_total_product_views_app", LongType, nullable = true),
        StructField(s"L3GA_EC4_${cat}_total_product_views_web", LongType, nullable = true),
        StructField(s"L3GA_EC4_${cat}_total_product_clicks_app", LongType, nullable = true),
        StructField(s"L3GA_EC4_${cat}_total_product_clicks_web", LongType, nullable = true),
        StructField(s"L3GA_EC4_${cat}_total_pdp_sessions_app", LongType, nullable = true),
        StructField(s"L3GA_EC4_${cat}_total_pdp_sessions_web", LongType, nullable = true)
      ))
  )

  val GAL3EC4LPSchema = StructType(
    Seq(
      StructField("customer_id", LongType, nullable = true),
      StructField("dt", DateType, nullable = true)
    ) ++
      EC4Cats.map("CAT" + _).flatMap(cat => Seq(
        StructField(s"L3GA_EC4_${cat}_total_clp_sessions_app", LongType, nullable = true),
        StructField(s"L3GA_EC4_${cat}_total_clp_sessions_web", LongType, nullable = true),
        StructField(s"L3GA_EC4_${cat}_total_glp_sessions_app", LongType, nullable = true),
        StructField(s"L3GA_EC4_${cat}_total_glp_sessions_web", LongType, nullable = true)
      ))
  )

}

