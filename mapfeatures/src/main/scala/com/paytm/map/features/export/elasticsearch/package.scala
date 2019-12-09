package com.paytm.map.features.export

import org.apache.spark.sql.DataFrame

package object elasticsearch {
  /**
   * Implicitly lift a DataFrame with EsNativeDataFrameFunctions.
   *
   * @param dataFrame A DataFrame to lift.
   * @return Enriched DataFrame with EsNativeDataFrameFunctions.
   */
  implicit def esNativeDataFrameFunctions(dataFrame: DataFrame): EsDataFrameFunctions = new EsDataFrameFunctions(dataFrame)
}
