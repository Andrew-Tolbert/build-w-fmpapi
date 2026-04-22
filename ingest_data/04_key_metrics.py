# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest key metrics and financial ratios JSON files from UC Volume into bronze Delta tables.
# Source: UC_VOLUME_PATH/key_metrics/{TICKER}/{ts}_{key_metrics,financial_ratios}.json
# Outputs:
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_key_metrics
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_financial_ratios

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# # Uncomment to wipe tables before re-running
# for t in ["bronze_key_metrics", "bronze_financial_ratios"]:
#     spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{t}")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_key_metrics (
        symbol                                 STRING,
        date                                   DATE,
        fiscalYear                             STRING,
        period                                 STRING,
        reportedCurrency                       STRING,
        marketCap                              DOUBLE,
        enterpriseValue                        DOUBLE,
        evToSales                              DOUBLE,
        evToOperatingCashFlow                  DOUBLE,
        evToFreeCashFlow                       DOUBLE,
        evToEBITDA                             DOUBLE,
        netDebtToEBITDA                        DOUBLE,
        currentRatio                           DOUBLE,
        incomeQuality                          DOUBLE,
        grahamNumber                           DOUBLE,
        grahamNetNet                           DOUBLE,
        taxBurden                              DOUBLE,
        interestBurden                         DOUBLE,
        workingCapital                         DOUBLE,
        investedCapital                        DOUBLE,
        returnOnAssets                         DOUBLE,
        operatingReturnOnAssets                DOUBLE,
        returnOnTangibleAssets                 DOUBLE,
        returnOnEquity                         DOUBLE,
        returnOnInvestedCapital                DOUBLE,
        returnOnCapitalEmployed                DOUBLE,
        earningsYield                          DOUBLE,
        freeCashFlowYield                      DOUBLE,
        capexToOperatingCashFlow               DOUBLE,
        capexToDepreciation                    DOUBLE,
        capexToRevenue                         DOUBLE,
        salesGeneralAndAdministrativeToRevenue DOUBLE,
        researchAndDevelopementToRevenue       DOUBLE,
        stockBasedCompensationToRevenue        DOUBLE,
        intangiblesToTotalAssets               DOUBLE,
        averageReceivables                     DOUBLE,
        averagePayables                        DOUBLE,
        averageInventory                       DOUBLE,
        daysOfSalesOutstanding                 DOUBLE,
        daysOfPayablesOutstanding              DOUBLE,
        daysOfInventoryOutstanding             DOUBLE,
        operatingCycle                         DOUBLE,
        cashConversionCycle                    DOUBLE,
        freeCashFlowToEquity                   DOUBLE,
        freeCashFlowToFirm                     DOUBLE,
        tangibleAssetValue                     DOUBLE,
        netCurrentAssetValue                   DOUBLE,
        ingested_at                            STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_financial_ratios (
        symbol                                    STRING,
        date                                      DATE,
        fiscalYear                                STRING,
        period                                    STRING,
        reportedCurrency                          STRING,
        grossProfitMargin                         DOUBLE,
        ebitMargin                                DOUBLE,
        ebitdaMargin                              DOUBLE,
        operatingProfitMargin                     DOUBLE,
        pretaxProfitMargin                        DOUBLE,
        continuousOperationsProfitMargin          DOUBLE,
        netProfitMargin                           DOUBLE,
        bottomLineProfitMargin                    DOUBLE,
        receivablesTurnover                       DOUBLE,
        payablesTurnover                          DOUBLE,
        inventoryTurnover                         DOUBLE,
        fixedAssetTurnover                        DOUBLE,
        assetTurnover                             DOUBLE,
        currentRatio                              DOUBLE,
        quickRatio                                DOUBLE,
        solvencyRatio                             DOUBLE,
        cashRatio                                 DOUBLE,
        priceToEarningsRatio                      DOUBLE,
        priceToEarningsGrowthRatio                DOUBLE,
        forwardPriceToEarningsGrowthRatio         DOUBLE,
        priceToBookRatio                          DOUBLE,
        priceToSalesRatio                         DOUBLE,
        priceToFreeCashFlowRatio                  DOUBLE,
        priceToOperatingCashFlowRatio             DOUBLE,
        debtToAssetsRatio                         DOUBLE,
        debtToEquityRatio                         DOUBLE,
        debtToCapitalRatio                        DOUBLE,
        longTermDebtToCapitalRatio                DOUBLE,
        financialLeverageRatio                    DOUBLE,
        workingCapitalTurnoverRatio               DOUBLE,
        operatingCashFlowRatio                    DOUBLE,
        operatingCashFlowSalesRatio               DOUBLE,
        freeCashFlowOperatingCashFlowRatio        DOUBLE,
        debtServiceCoverageRatio                  DOUBLE,
        interestCoverageRatio                     DOUBLE,
        shortTermOperatingCashFlowCoverageRatio   DOUBLE,
        operatingCashFlowCoverageRatio            DOUBLE,
        capitalExpenditureCoverageRatio           DOUBLE,
        dividendPaidAndCapexCoverageRatio         DOUBLE,
        dividendPayoutRatio                       DOUBLE,
        dividendYield                             DOUBLE,
        dividendYieldPercentage                   DOUBLE,
        revenuePerShare                           DOUBLE,
        netIncomePerShare                         DOUBLE,
        interestDebtPerShare                      DOUBLE,
        cashPerShare                              DOUBLE,
        bookValuePerShare                         DOUBLE,
        tangibleBookValuePerShare                 DOUBLE,
        shareholdersEquityPerShare                DOUBLE,
        operatingCashFlowPerShare                 DOUBLE,
        capexPerShare                             DOUBLE,
        freeCashFlowPerShare                      DOUBLE,
        netIncomePerEBT                           DOUBLE,
        ebtPerEbit                                DOUBLE,
        priceToFairValue                          DOUBLE,
        debtToMarketCap                           DOUBLE,
        effectiveTaxRate                          DOUBLE,
        enterpriseValueMultiple                   DOUBLE,
        dividendPerShare                          DOUBLE,
        ingested_at                               STRING
    )
    USING DELTA
""")

# COMMAND ----------

base = f"{UC_VOLUME_PATH}/key_metrics"

for suffix, table in [
    ("key_metrics",      "bronze_key_metrics"),
    ("financial_ratios", "bronze_financial_ratios"),
]:
    path = f"{base}/*/*_{suffix}.json"
    target_schema = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.{table}").schema
    df = (
        spark.read
            .option("multiline", "true")
            .schema(target_schema)
            .json(path)
            .withColumn("date", col("date").cast("date"))
    )
    df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.{table}")
    print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.{table}")

# COMMAND ----------

schema(target_schema)
