"""SparkUDFClass in dbimport.common.spark_udf2 behaves like the functions in spark_udf."""

import pytest

from dbimport.common import spark_udf, spark_udf2


@pytest.mark.parametrize(
    "value",
    [
        pytest.param([[[1, b"hi"], [2, b"\x00\xff"]]], id="binary-pairs-encoded"),
        pytest.param([[[300, b"hi"]]], id="flag-out-of-range-unchanged"),
        pytest.param([[[1, "text"]]], id="non-bytes-unchanged"),
        pytest.param(5, id="not-iterable-returned"),
    ],
)
def test_spark_udf_class_base64_encode_array_matches_module_function(
    value: object,
) -> None:
    expected = spark_udf.base64_encode_array(value)

    result = spark_udf2.SparkUDFClass().base64_encode_array(value)

    assert result == expected


def test_spark_udf_class_base64_encode_array_binary_pair_is_encoded() -> None:
    result = spark_udf2.SparkUDFClass().base64_encode_array([[[1, b"hi"]]])

    assert result == [[[1, "aGk="]]]
