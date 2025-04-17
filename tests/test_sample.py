import pytest
from unittest.mock import patch, MagicMock
from glue_scripts
# Assume merge_delta is in a file called glue.py
from glue import merge_delta

@patch("glue.logger")
@patch("glue.DeltaTable")
def test_merge_delta_existing_table(mock_delta_table_class, mock_logger):
    # Setup
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_delta_table = MagicMock()
    mock_delta_table_class.isDeltaTable.return_value = True
    mock_delta_table_class.forPath.return_value = mock_delta_table

    # Run
    merge_delta(
        df=mock_df,
        path="s3://dummy-path",
        key_cols=["id"],
        partition_col="date"
    )

    # Assert
    mock_delta_table.alias.assert_called_with("target")
    mock_logger.info.assert_any_call("MERGE completed for s3://dummy-path")
