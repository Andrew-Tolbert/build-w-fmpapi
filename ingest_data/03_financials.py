# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest financial statement JSON files from UC Volume into bronze Delta tables.
# Source: UC_VOLUME_PATH/financials/{TICKER}/{ts}_{type}.json
# Outputs:
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_income_statements
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_balance_sheets
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_cash_flows
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_income_growth
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_balance_growth
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_cashflow_growth

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# # Uncomment to wipe tables before re-running
# for t in ["bronze_income_statements","bronze_balance_sheets","bronze_cash_flows",
#           "bronze_income_growth","bronze_balance_growth","bronze_cashflow_growth"]:
#     spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{t}")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_income_statements (
        date                                     DATE,
        symbol                                   STRING,
        reportedCurrency                         STRING,
        cik                                      STRING,
        filingDate                               STRING,
        acceptedDate                             STRING,
        fiscalYear                               STRING,
        period                                   STRING,
        revenue                                  DOUBLE,
        costOfRevenue                            DOUBLE,
        grossProfit                              DOUBLE,
        researchAndDevelopmentExpenses           DOUBLE,
        generalAndAdministrativeExpenses         DOUBLE,
        sellingAndMarketingExpenses              DOUBLE,
        sellingGeneralAndAdministrativeExpenses  DOUBLE,
        otherExpenses                            DOUBLE,
        operatingExpenses                        DOUBLE,
        costAndExpenses                          DOUBLE,
        netInterestIncome                        DOUBLE,
        interestIncome                           DOUBLE,
        interestExpense                          DOUBLE,
        depreciationAndAmortization              DOUBLE,
        ebitda                                   DOUBLE,
        ebit                                     DOUBLE,
        nonOperatingIncomeExcludingInterest      DOUBLE,
        operatingIncome                          DOUBLE,
        totalOtherIncomeExpensesNet              DOUBLE,
        incomeBeforeTax                          DOUBLE,
        incomeTaxExpense                         DOUBLE,
        netIncomeFromContinuingOperations        DOUBLE,
        netIncomeFromDiscontinuedOperations      DOUBLE,
        otherAdjustmentsToNetIncome              DOUBLE,
        netIncome                                DOUBLE,
        netIncomeDeductions                      DOUBLE,
        bottomLineNetIncome                      DOUBLE,
        eps                                      DOUBLE,
        epsDiluted                               DOUBLE,
        weightedAverageShsOut                    LONG,
        weightedAverageShsOutDil                 LONG,
        ingested_at                              STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_balance_sheets (
        date                                     DATE,
        symbol                                   STRING,
        reportedCurrency                         STRING,
        cik                                      STRING,
        filingDate                               STRING,
        acceptedDate                             STRING,
        fiscalYear                               STRING,
        period                                   STRING,
        cashAndCashEquivalents                   DOUBLE,
        shortTermInvestments                     DOUBLE,
        cashAndShortTermInvestments              DOUBLE,
        netReceivables                           DOUBLE,
        accountsReceivables                      DOUBLE,
        otherReceivables                         DOUBLE,
        inventory                                DOUBLE,
        prepaids                                 DOUBLE,
        otherCurrentAssets                       DOUBLE,
        totalCurrentAssets                       DOUBLE,
        propertyPlantEquipmentNet                DOUBLE,
        goodwill                                 DOUBLE,
        intangibleAssets                         DOUBLE,
        goodwillAndIntangibleAssets              DOUBLE,
        longTermInvestments                      DOUBLE,
        taxAssets                                DOUBLE,
        otherNonCurrentAssets                    DOUBLE,
        totalNonCurrentAssets                    DOUBLE,
        otherAssets                              DOUBLE,
        totalAssets                              DOUBLE,
        totalPayables                            DOUBLE,
        accountPayables                          DOUBLE,
        otherPayables                            DOUBLE,
        accruedExpenses                          DOUBLE,
        shortTermDebt                            DOUBLE,
        capitalLeaseObligationsCurrent           DOUBLE,
        taxPayables                              DOUBLE,
        deferredRevenue                          DOUBLE,
        otherCurrentLiabilities                  DOUBLE,
        totalCurrentLiabilities                  DOUBLE,
        longTermDebt                             DOUBLE,
        capitalLeaseObligationsNonCurrent        DOUBLE,
        deferredRevenueNonCurrent                DOUBLE,
        deferredTaxLiabilitiesNonCurrent         DOUBLE,
        otherNonCurrentLiabilities               DOUBLE,
        totalNonCurrentLiabilities               DOUBLE,
        otherLiabilities                         DOUBLE,
        capitalLeaseObligations                  DOUBLE,
        totalLiabilities                         DOUBLE,
        treasuryStock                            DOUBLE,
        preferredStock                           DOUBLE,
        commonStock                              DOUBLE,
        retainedEarnings                         DOUBLE,
        additionalPaidInCapital                  DOUBLE,
        accumulatedOtherComprehensiveIncomeLoss  DOUBLE,
        otherTotalStockholdersEquity             DOUBLE,
        totalStockholdersEquity                  DOUBLE,
        totalEquity                              DOUBLE,
        minorityInterest                         DOUBLE,
        totalLiabilitiesAndTotalEquity           DOUBLE,
        totalInvestments                         DOUBLE,
        totalDebt                                DOUBLE,
        netDebt                                  DOUBLE,
        ingested_at                              STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_cash_flows (
        date                                          DATE,
        symbol                                        STRING,
        reportedCurrency                              STRING,
        cik                                           STRING,
        filingDate                                    STRING,
        acceptedDate                                  STRING,
        fiscalYear                                    STRING,
        period                                        STRING,
        netIncome                                     DOUBLE,
        depreciationAndAmortization                   DOUBLE,
        deferredIncomeTax                             DOUBLE,
        stockBasedCompensation                        DOUBLE,
        changeInWorkingCapital                        DOUBLE,
        accountsReceivables                           DOUBLE,
        inventory                                     DOUBLE,
        accountsPayables                              DOUBLE,
        otherWorkingCapital                           DOUBLE,
        otherNonCashItems                             DOUBLE,
        netCashProvidedByOperatingActivities          DOUBLE,
        investmentsInPropertyPlantAndEquipment        DOUBLE,
        acquisitionsNet                               DOUBLE,
        purchasesOfInvestments                        DOUBLE,
        salesMaturitiesOfInvestments                  DOUBLE,
        otherInvestingActivities                      DOUBLE,
        netCashProvidedByInvestingActivities          DOUBLE,
        netDebtIssuance                               DOUBLE,
        longTermNetDebtIssuance                       DOUBLE,
        shortTermNetDebtIssuance                      DOUBLE,
        netStockIssuance                              DOUBLE,
        netCommonStockIssuance                        DOUBLE,
        commonStockIssuance                           DOUBLE,
        commonStockRepurchased                        DOUBLE,
        netPreferredStockIssuance                     DOUBLE,
        netDividendsPaid                              DOUBLE,
        commonDividendsPaid                           DOUBLE,
        preferredDividendsPaid                        DOUBLE,
        otherFinancingActivities                      DOUBLE,
        netCashProvidedByFinancingActivities          DOUBLE,
        effectOfForexChangesOnCash                    DOUBLE,
        netChangeInCash                               DOUBLE,
        cashAtEndOfPeriod                             DOUBLE,
        cashAtBeginningOfPeriod                       DOUBLE,
        operatingCashFlow                             DOUBLE,
        capitalExpenditure                            DOUBLE,
        freeCashFlow                                  DOUBLE,
        incomeTaxesPaid                               DOUBLE,
        interestPaid                                  DOUBLE,
        ingested_at                                   STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_income_growth (
        symbol                                       STRING,
        date                                         DATE,
        fiscalYear                                   STRING,
        period                                       STRING,
        reportedCurrency                             STRING,
        growthRevenue                                DOUBLE,
        growthCostOfRevenue                          DOUBLE,
        growthGrossProfit                            DOUBLE,
        growthGrossProfitRatio                       DOUBLE,
        growthResearchAndDevelopmentExpenses         DOUBLE,
        growthGeneralAndAdministrativeExpenses       DOUBLE,
        growthSellingAndMarketingExpenses            DOUBLE,
        growthOtherExpenses                          DOUBLE,
        growthOperatingExpenses                      DOUBLE,
        growthCostAndExpenses                        DOUBLE,
        growthInterestIncome                         DOUBLE,
        growthInterestExpense                        DOUBLE,
        growthDepreciationAndAmortization            DOUBLE,
        growthEBITDA                                 DOUBLE,
        growthOperatingIncome                        DOUBLE,
        growthIncomeBeforeTax                        DOUBLE,
        growthIncomeTaxExpense                       DOUBLE,
        growthNetIncome                              DOUBLE,
        growthEPS                                    DOUBLE,
        growthEPSDiluted                             DOUBLE,
        growthWeightedAverageShsOut                  DOUBLE,
        growthWeightedAverageShsOutDil               DOUBLE,
        growthEBIT                                   DOUBLE,
        growthNonOperatingIncomeExcludingInterest    DOUBLE,
        growthNetInterestIncome                      DOUBLE,
        growthTotalOtherIncomeExpensesNet            DOUBLE,
        growthNetIncomeFromContinuingOperations      DOUBLE,
        growthOtherAdjustmentsToNetIncome            DOUBLE,
        growthNetIncomeDeductions                    DOUBLE,
        ingested_at                                  STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_balance_growth (
        symbol                                       STRING,
        date                                         DATE,
        fiscalYear                                   STRING,
        period                                       STRING,
        reportedCurrency                             STRING,
        growthCashAndCashEquivalents                 DOUBLE,
        growthShortTermInvestments                   DOUBLE,
        growthCashAndShortTermInvestments            DOUBLE,
        growthNetReceivables                         DOUBLE,
        growthInventory                              DOUBLE,
        growthOtherCurrentAssets                     DOUBLE,
        growthTotalCurrentAssets                     DOUBLE,
        growthPropertyPlantEquipmentNet              DOUBLE,
        growthGoodwill                               DOUBLE,
        growthIntangibleAssets                       DOUBLE,
        growthGoodwillAndIntangibleAssets            DOUBLE,
        growthLongTermInvestments                    DOUBLE,
        growthTaxAssets                              DOUBLE,
        growthOtherNonCurrentAssets                  DOUBLE,
        growthTotalNonCurrentAssets                  DOUBLE,
        growthOtherAssets                            DOUBLE,
        growthTotalAssets                            DOUBLE,
        growthAccountPayables                        DOUBLE,
        growthShortTermDebt                          DOUBLE,
        growthTaxPayables                            DOUBLE,
        growthDeferredRevenue                        DOUBLE,
        growthOtherCurrentLiabilities                DOUBLE,
        growthTotalCurrentLiabilities                DOUBLE,
        growthLongTermDebt                           DOUBLE,
        growthDeferredRevenueNonCurrent              DOUBLE,
        growthDeferredTaxLiabilitiesNonCurrent       DOUBLE,
        growthOtherNonCurrentLiabilities             DOUBLE,
        growthTotalNonCurrentLiabilities             DOUBLE,
        growthOtherLiabilities                       DOUBLE,
        growthTotalLiabilities                       DOUBLE,
        growthPreferredStock                         DOUBLE,
        growthCommonStock                            DOUBLE,
        growthRetainedEarnings                       DOUBLE,
        growthAccumulatedOtherComprehensiveIncomeLoss DOUBLE,
        growthOthertotalStockholdersEquity           DOUBLE,
        growthTotalStockholdersEquity                DOUBLE,
        growthMinorityInterest                       DOUBLE,
        growthTotalEquity                            DOUBLE,
        growthTotalLiabilitiesAndStockholdersEquity  DOUBLE,
        growthTotalInvestments                       DOUBLE,
        growthTotalDebt                              DOUBLE,
        growthNetDebt                                DOUBLE,
        growthAccountsReceivables                    DOUBLE,
        growthOtherReceivables                       DOUBLE,
        growthPrepaids                               DOUBLE,
        growthTotalPayables                          DOUBLE,
        growthOtherPayables                          DOUBLE,
        growthAccruedExpenses                        DOUBLE,
        growthCapitalLeaseObligationsCurrent         DOUBLE,
        growthAdditionalPaidInCapital                DOUBLE,
        growthTreasuryStock                          DOUBLE,
        ingested_at                                  STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_cashflow_growth (
        symbol                                            STRING,
        date                                              DATE,
        fiscalYear                                        STRING,
        period                                            STRING,
        reportedCurrency                                  STRING,
        growthNetIncome                                   DOUBLE,
        growthDepreciationAndAmortization                 DOUBLE,
        growthDeferredIncomeTax                           DOUBLE,
        growthStockBasedCompensation                      DOUBLE,
        growthChangeInWorkingCapital                      DOUBLE,
        growthAccountsReceivables                         DOUBLE,
        growthInventory                                   DOUBLE,
        growthAccountsPayables                            DOUBLE,
        growthOtherWorkingCapital                         DOUBLE,
        growthOtherNonCashItems                           DOUBLE,
        growthNetCashProvidedByOperatingActivites         DOUBLE,
        growthInvestmentsInPropertyPlantAndEquipment      DOUBLE,
        growthAcquisitionsNet                             DOUBLE,
        growthPurchasesOfInvestments                      DOUBLE,
        growthSalesMaturitiesOfInvestments                DOUBLE,
        growthOtherInvestingActivites                     DOUBLE,
        growthNetCashUsedForInvestingActivites            DOUBLE,
        growthDebtRepayment                               DOUBLE,
        growthCommonStockIssued                           DOUBLE,
        growthCommonStockRepurchased                      DOUBLE,
        growthDividendsPaid                               DOUBLE,
        growthOtherFinancingActivites                     DOUBLE,
        growthNetCashUsedProvidedByFinancingActivities    DOUBLE,
        growthEffectOfForexChangesOnCash                  DOUBLE,
        growthNetChangeInCash                             DOUBLE,
        growthCashAtEndOfPeriod                           DOUBLE,
        growthCashAtBeginningOfPeriod                     DOUBLE,
        growthOperatingCashFlow                           DOUBLE,
        growthCapitalExpenditure                          DOUBLE,
        growthFreeCashFlow                                DOUBLE,
        growthNetDebtIssuance                             DOUBLE,
        growthLongTermNetDebtIssuance                     DOUBLE,
        growthShortTermNetDebtIssuance                    DOUBLE,
        growthNetStockIssuance                            DOUBLE,
        growthPreferredDividendsPaid                      DOUBLE,
        growthIncomeTaxesPaid                             DOUBLE,
        growthInterestPaid                                DOUBLE,
        ingested_at                                       STRING
    )
    USING DELTA
""")

# COMMAND ----------

base = f"{UC_VOLUME_PATH}/financials"

for suffix, table in [
    ("income_statements", "bronze_income_statements"),
    ("balance_sheets",    "bronze_balance_sheets"),
    ("cash_flows",        "bronze_cash_flows"),
    ("income_growth",     "bronze_income_growth"),
    ("balance_growth",    "bronze_balance_growth"),
    ("cashflow_growth",   "bronze_cashflow_growth"),
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

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_income_statements").orderBy("symbol", "date"))
