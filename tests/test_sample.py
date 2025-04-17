import sys
import types

# Mock awsglue modules
sys.modules['awsglue'] = types.ModuleType('awsglue')
sys.modules['awsglue.transforms'] = types.ModuleType('awsglue.transforms')
sys.modules['awsglue.utils'] = types.ModuleType('awsglue.utils')
sys.modules['awsglue.context'] = types.ModuleType('awsglue.context')

# Now import the actual code
from glue_scripts.glue import merge_delta
import pytest
from unittest.mock import patch, MagicMock

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
