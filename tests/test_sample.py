import sys
import types
from unittest.mock import MagicMock
import pytest

# Mock awsglue modules
sys.modules['awsglue'] = types.ModuleType('awsglue')
sys.modules['awsglue.transforms'] = types.ModuleType('awsglue.transforms')
sys.modules['awsglue.utils'] = types.ModuleType('awsglue.utils')
sys.modules['awsglue.context'] = types.ModuleType('awsglue.context')
sys.modules['awsglue.utils'].getResolvedOptions = MagicMock()
sys.modules['awsglue.context'].GlueContext = MagicMock()

# Mock pyspark modules
sys.modules['pyspark'] = types.ModuleType('pyspark')
sys.modules['pyspark.context'] = types.ModuleType('pyspark.context')
sys.modules['pyspark.sql'] = types.ModuleType('pyspark.sql')
sys.modules['pyspark.sql.functions'] = types.ModuleType('pyspark.sql.functions')
sys.modules['pyspark.context'].SparkContext = MagicMock()
sys.modules['pyspark.sql'].SparkSession = MagicMock()
sys.modules['pyspark.sql.functions'].col = MagicMock()
sys.modules['pyspark.sql.functions'].lit = MagicMock()
sys.modules['pyspark.conf'] = types.ModuleType('pyspark.conf')
sys.modules['pyspark.conf'].SparkConf = MagicMock()

sys.modules['awsglue'] = types.ModuleType('awsglue')
sys.modules['awsglue.transforms'] = types.ModuleType('awsglue.transforms')
sys.modules['awsglue.utils'] = types.ModuleType('awsglue.utils')
sys.modules['awsglue.context'] = types.ModuleType('awsglue.context')
sys.modules['awsglue.job'] = types.ModuleType('awsglue.job') 
sys.modules['awsglue.utils'].getResolvedOptions = MagicMock()
sys.modules['awsglue.context'].GlueContext = MagicMock()
sys.modules['awsglue.job'].Job = MagicMock() 
sys.modules['pyspark.sql.functions'].to_timestamp = MagicMock()

sys.modules['pyspark.sql.types'] = types.ModuleType('pyspark.sql.types')
sys.modules['pyspark.sql.types'].StructType = MagicMock()
sys.modules['pyspark.sql.types'].StructField = MagicMock()
sys.modules['pyspark.sql.types'].StringType = MagicMock()
sys.modules['pyspark.sql.types'].IntegerType = MagicMock()
sys.modules['pyspark.sql.types'].TimestampType = MagicMock()
sys.modules['pyspark.sql.types'].DoubleType = MagicMock()
sys.modules['pyspark.sql.types'].DateType = MagicMock()

sys.modules['delta'] = types.ModuleType('delta')
sys.modules['delta.tables'] = types.ModuleType('delta.tables')
sys.modules['delta.tables'].DeltaTable = MagicMock()

# Now import the actual code
from glue_scripts.glue import merge_delta
from unittest.mock import patch

@patch("glue_scripts.glue.logger")
@patch("glue_scripts.glue.DeltaTable")
def test_merge_delta_existing_table(mock_delta_table_class, mock_logger):
    mock_df = MagicMock()
    mock_delta_table = MagicMock()
    mock_delta_table_class.isDeltaTable.return_value = True
    mock_delta_table_class.forPath.return_value = mock_delta_table

    merge_delta(
        df=mock_df,
        path="s3://dummy-path",
        key_cols=["id"],
        partition_col="date"
    )

    mock_delta_table.alias.assert_called_with("target")
    mock_logger.info.assert_any_call("MERGE completed for s3://dummy-path")
