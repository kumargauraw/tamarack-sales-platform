import datetime as dt
from decimal import Decimal

import pytest

from tamarack_sales.daily_sales import (
    build_daily_sales,
    parse_from_date,
    replace_where_predicate,
    table_name,
)

# ---------- pure Python helpers ----------


def test_table_name_builds_three_part_name():
    assert table_name("dev_tamarack", "gold", "daily_sales") == "dev_tamarack.gold.daily_sales"


@pytest.mark.parametrize("bad", ["", "prod_tamarack; DROP", "a.b", "1abc", None])
def test_table_name_rejects_non_identifiers(bad):
    with pytest.raises(ValueError):
        table_name(bad, "gold", "daily_sales")


def test_parse_from_date_accepts_iso_string_and_date():
    assert parse_from_date(" 2026-10-01 ") == dt.date(2026, 10, 1)
    assert parse_from_date(dt.date(2026, 10, 1)) == dt.date(2026, 10, 1)


@pytest.mark.parametrize("bad", ["", "10/01/2026", "2026-13-01", "2026-10-01' OR 1=1 --", None])
def test_parse_from_date_rejects_bad_values(bad):
    with pytest.raises(ValueError):
        parse_from_date(bad)


def test_replace_where_predicate():
    assert replace_where_predicate(dt.date(2026, 10, 1)) == "sale_date >= '2026-10-01'"


# ---------- Spark transformation ----------

ORDERS_SCHEMA = "order_id INT, order_date TIMESTAMP, store_id INT, status STRING"
LINES_SCHEMA = "order_id INT, product_id INT, quantity INT, unit_price DOUBLE, discount_pct DOUBLE"


def _ts(s):
    return dt.datetime.fromisoformat(s)


def _run(spark, orders, lines, from_date=dt.date(2026, 10, 1)):
    df = build_daily_sales(
        spark.createDataFrame(orders, ORDERS_SCHEMA),
        spark.createDataFrame(lines, LINES_SCHEMA),
        from_date,
    )
    return {(r.sale_date, r.store_id, r.product_id): (r.units, r.revenue) for r in df.collect()}, df


@pytest.mark.spark
def test_only_complete_orders_on_or_after_from_date(spark):
    orders = [
        (1, _ts("2026-10-01 09:00:00"), 10, "COMPLETE"),
        (2, _ts("2026-10-01 10:00:00"), 10, "CANCELLED"),
        (3, _ts("2026-09-30 23:59:59"), 10, "COMPLETE"),
    ]
    lines = [(1, 100, 1, 5.0, None), (2, 100, 1, 5.0, None), (3, 100, 1, 5.0, None)]
    result, _ = _run(spark, orders, lines)
    assert result == {(dt.date(2026, 10, 1), 10, 100): (1, Decimal("5.00"))}


@pytest.mark.spark
def test_revenue_is_rounded_per_line_before_summing(spark):
    # Each line is 1 x 0.335 * (1 - 0%) = 0.335 -> rounds to 0.34 per line, 0.68 total.
    # Rounding the sum instead would give round(0.67, 2) = 0.67.
    orders = [(1, _ts("2026-10-02 08:00:00"), 10, "COMPLETE")]
    lines = [(1, 100, 1, 0.335, 0.0), (1, 100, 1, 0.335, 0.0)]
    result, _ = _run(spark, orders, lines)
    assert result[(dt.date(2026, 10, 2), 10, 100)] == (2, Decimal("0.68"))


@pytest.mark.spark
def test_discount_applied_and_null_discount_is_zero(spark):
    orders = [(1, _ts("2026-10-03 08:00:00"), 10, "COMPLETE")]
    lines = [(1, 100, 2, 10.0, 25.0), (1, 200, 3, 10.0, None)]
    result, _ = _run(spark, orders, lines)
    assert result[(dt.date(2026, 10, 3), 10, 100)] == (2, Decimal("15.00"))
    assert result[(dt.date(2026, 10, 3), 10, 200)] == (3, Decimal("30.00"))


@pytest.mark.spark
def test_groups_by_day_store_product_and_output_schema(spark):
    orders = [
        (1, _ts("2026-10-04 08:00:00"), 10, "COMPLETE"),
        (2, _ts("2026-10-04 20:00:00"), 10, "COMPLETE"),
        (3, _ts("2026-10-04 12:00:00"), 20, "COMPLETE"),
    ]
    lines = [(1, 100, 1, 2.0, None), (2, 100, 4, 2.0, None), (3, 100, 1, 2.0, None)]
    result, df = _run(spark, orders, lines)
    assert result == {
        (dt.date(2026, 10, 4), 10, 100): (5, Decimal("10.00")),
        (dt.date(2026, 10, 4), 20, 100): (1, Decimal("2.00")),
    }
    assert df.columns == ["sale_date", "store_id", "product_id", "units", "revenue", "loaded_at"]
    assert dict(df.dtypes)["revenue"] == "decimal(14,2)"
