from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bigquery_mig.config.ConfigStore import *
from bigquery_mig.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("bigquery_mig").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/bigquery_mig")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/bigquery_mig", config = Config)(pipeline)

if __name__ == "__main__":
    main()
