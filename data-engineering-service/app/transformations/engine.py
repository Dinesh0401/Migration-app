from __future__ import annotations

import logging
from typing import Any
import pandas as pd

logger = logging.getLogger(__name__)


def _resolve_col(df: pd.DataFrame, col_name: str) -> str:
    """
    Resolves column name against DataFrame columns, checking exact match first,
    then case-insensitively. Returns the resolved name if found, else original.
    """
    if col_name in df.columns:
        return col_name
    col_lower = str(col_name).strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == col_lower:
            return c
    return col_name


class TransformationEngine:
    """
    Generic, dynamic transformation engine for in-memory Pandas DataFrames.
    Executes declarative transformation rules specified in the migration contract.
    Operates on arbitrary schemas and DataFrames without hardcoded table or column logic.
    """

    def apply(self, df: pd.DataFrame, transformations: list[dict[str, Any]] | None) -> pd.DataFrame:
        if df is None:
            raise ValueError("DataFrame is required for transformation")

        if not transformations:
            logger.info("No transformations specified in migration contract. Passing DataFrame through unchanged.")
            return df.copy()

        working_df = df.copy()
        logger.info(f"Applying {len(transformations)} transformation step(s) to DataFrame ({len(working_df)} rows).")

        for idx, step in enumerate(transformations, start=1):
            op_name = step.get("type") or step.get("operation")
            if not op_name:
                raise ValueError(f"Transformation step #{idx} is missing 'type' or 'operation' field: {step}")

            normalized_op = str(op_name).strip().lower()

            if normalized_op == "rename":
                working_df = self._rename(working_df, step)
            elif normalized_op in ("normalize_columns", "normalizecolumns"):
                working_df = self._normalize_columns(working_df, step)
            elif normalized_op == "trim":
                working_df = self._trim(working_df, step)
            elif normalized_op in ("normalize", "clean"):
                working_df = self._normalize(working_df, step)
            elif normalized_op == "cast":
                working_df = self._cast(working_df, step)
            elif normalized_op == "round":
                working_df = self._round(working_df, step)
            elif normalized_op == "default":
                working_df = self._default(working_df, step)
            elif normalized_op in ("null_handling", "nullhandling"):
                working_df = self._null_handling(working_df, step)
            elif normalized_op == "filter":
                working_df = self._filter(working_df, step)
            elif normalized_op in ("value_mapping", "valuemapping", "map"):
                working_df = self._value_mapping(working_df, step)
            elif normalized_op in ("concatenate", "concat", "merge"):
                working_df = self._concatenate(working_df, step)
            elif normalized_op in ("derive", "calculate"):
                working_df = self._derive(working_df, step)
            elif normalized_op == "split":
                working_df = self._split(working_df, step)
            else:
                raise ValueError(f"Unsupported transformation type '{op_name}' at step #{idx}")

        logger.info(f"Completed transformations. Resulting DataFrame has {len(working_df)} rows and {len(working_df.columns)} columns.")
        return working_df

    def _rename(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        if "columns" in step and isinstance(step["columns"], dict):
            resolved_dict = {_resolve_col(df, k): v for k, v in step["columns"].items()}
            return df.rename(columns=resolved_dict)

        case = step.get("case")
        if case:
            if str(case).lower() == "lower":
                return df.rename(columns=lambda c: str(c).strip().lower())
            elif str(case).lower() == "upper":
                return df.rename(columns=lambda c: str(c).strip().upper())

        raw_source = step.get("source") or step.get("source_column")
        target = step.get("target") or step.get("target_column")
        if not raw_source or not target:
            raise ValueError("rename requires 'source' and 'target' (or 'columns' dict)")

        source = _resolve_col(df, raw_source)
        if source not in df.columns:
            raise KeyError(f"Column '{raw_source}' not found for rename. Available columns: {list(df.columns)}")

        return df.rename(columns={source: target})

    def _normalize_columns(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        case = str(step.get("case", "lower")).strip().lower()
        if case == "lower":
            df.columns = [str(c).strip().lower() for c in df.columns]
        elif case == "upper":
            df.columns = [str(c).strip().upper() for c in df.columns]
        elif case in ("snake", "snake_case"):
            df.columns = [
                str(c).strip().lower().replace(" ", "_").replace("-", "_")
                for c in df.columns
            ]
        return df

    def _trim(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        cols = step.get("columns")
        if cols and isinstance(cols, list):
            target_cols = cols
        else:
            source = step.get("source") or step.get("source_column")
            if not source:
                raise ValueError("trim requires 'source' or 'columns' list")
            target_cols = [source]

        target = step.get("target") or step.get("target_column")

        for col in target_cols:
            resolved = _resolve_col(df, col)
            if resolved not in df.columns:
                raise KeyError(f"Column '{col}' not found for trim. Available: {list(df.columns)}")
            dest = target if (target and len(target_cols) == 1) else resolved
            df[dest] = df[resolved].apply(lambda v: v.strip() if isinstance(v, str) else v)

        return df

    def _normalize(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        cols = step.get("columns")
        if cols and isinstance(cols, list):
            target_cols = cols
        else:
            source = step.get("source") or step.get("source_column")
            if not source:
                raise ValueError("normalize requires 'source' or 'columns' list")
            target_cols = [source]

        target = step.get("target") or step.get("target_column")
        case = str(step.get("case", "")).strip().lower()
        do_strip = bool(step.get("strip", True))

        for col in target_cols:
            resolved = _resolve_col(df, col)
            if resolved not in df.columns:
                raise KeyError(f"Column '{col}' not found for normalize. Available: {list(df.columns)}")
            dest = target if (target and len(target_cols) == 1) else resolved

            def _clean_val(v: Any) -> Any:
                if not isinstance(v, str):
                    return v
                val = v.strip() if do_strip else v
                if case == "upper":
                    return val.upper()
                elif case == "lower":
                    return val.lower()
                elif case == "title":
                    return val.title()
                elif case == "capitalize":
                    return val.capitalize()
                return val

            df[dest] = df[resolved].apply(_clean_val)

        return df

    def _cast(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        raw_source = step.get("source") or step.get("source_column")
        target = step.get("target") or step.get("target_column")
        dtype = str(step.get("dtype") or step.get("target_type") or "").strip().lower()

        if not raw_source or not dtype:
            raise ValueError("cast requires 'source' and 'dtype'/'target_type'")

        source = _resolve_col(df, raw_source)
        if source not in df.columns:
            raise KeyError(f"Column '{raw_source}' not found for cast. Available: {list(df.columns)}")

        dest = target or source
        df = df.copy()
        if dtype in ("int", "integer", "bigint", "int64"):
            df[dest] = pd.to_numeric(df[source], errors="coerce").astype("Int64")
        elif dtype in ("float", "numeric", "decimal", "double"):
            df[dest] = pd.to_numeric(df[source], errors="coerce").astype(float)
        elif dtype in ("datetime", "timestamp", "date"):
            df[dest] = pd.to_datetime(df[source], errors="coerce")
        elif dtype in ("str", "string", "varchar", "text"):
            df[dest] = df[source].astype(str).replace({"nan": None, "None": None, "<NA>": None})
        elif dtype in ("bool", "boolean"):
            df[dest] = df[source].astype(bool)
        else:
            df[dest] = df[source].astype(dtype)

        return df

    def _round(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        raw_source = step.get("source") or step.get("source_column")
        target = step.get("target") or step.get("target_column")
        decimals = int(step.get("decimals", 0))

        if not raw_source:
            raise ValueError("round requires 'source'")

        source = _resolve_col(df, raw_source)
        if source not in df.columns:
            raise KeyError(f"Column '{raw_source}' not found for round. Available: {list(df.columns)}")

        dest = target or source
        df = df.copy()
        df[dest] = pd.to_numeric(df[source], errors="coerce").round(decimals)
        return df

    def _default(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        raw_source = step.get("source") or step.get("source_column")
        value = step.get("value")
        if not raw_source or value is None:
            raise ValueError("default requires 'source' and 'value'")

        source = _resolve_col(df, raw_source)
        if source not in df.columns:
            raise KeyError(f"Column '{raw_source}' not found for default")

        df = df.copy()
        df[source] = df[source].fillna(value)
        return df

    def _null_handling(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        raw_source = step.get("source") or step.get("source_column")
        strategy = str(step.get("strategy", "drop")).strip().lower()

        if not raw_source:
            raise ValueError("null_handling requires 'source'")

        source = _resolve_col(df, raw_source)
        if source not in df.columns:
            raise KeyError(f"Column '{raw_source}' not found for null_handling")

        df = df.copy()
        if strategy == "drop":
            df = df.dropna(subset=[source])
        elif strategy == "fill":
            fill_value = step.get("value", "")
            df[source] = df[source].fillna(fill_value)
        else:
            raise ValueError(f"Unsupported null_handling strategy: '{strategy}'. Expected 'drop' or 'fill'.")

        return df

    def _filter(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        raw_col = step.get("column") or step.get("source")
        operator = str(step.get("operator", "eq")).strip().lower()
        value = step.get("value")

        if not raw_col:
            raise ValueError("filter requires 'column'")

        column = _resolve_col(df, raw_col)
        if column not in df.columns:
            raise KeyError(f"Column '{raw_col}' not found for filter")

        df = df.copy()
        op_norm = str(operator).strip().lower()
        if op_norm in ("eq", "==", "="):
            return df[df[column] == value]
        elif op_norm in ("ne", "!=", "<>"):
            return df[df[column] != value]
        elif op_norm in ("gt", ">"):
            return df[df[column] > value]
        elif op_norm in ("lt", "<"):
            return df[df[column] < value]
        elif op_norm in ("gte", ">="):
            return df[df[column] >= value]
        elif op_norm in ("lte", "<="):
            return df[df[column] <= value]
        elif op_norm in ("in",):
            return df[df[column].isin(value if isinstance(value, list) else [value])]
        elif op_norm in ("not_in", "not in"):
            return df[~df[column].isin(value if isinstance(value, list) else [value])]
        elif op_norm in ("is_null", "isnull"):
            return df[df[column].isna()]
        elif op_norm in ("not_null", "notnull"):
            return df[df[column].notna()]
        else:
            raise ValueError(f"Unsupported filter operator: '{operator}'")

    def _value_mapping(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        raw_source = step.get("source") or step.get("source_column")
        target = step.get("target") or step.get("target_column")
        mapping = step.get("mapping", {})
        default = step.get("default")

        if not raw_source or not mapping:
            raise ValueError("value_mapping requires 'source' and 'mapping'")

        source = _resolve_col(df, raw_source)
        if source not in df.columns:
            raise KeyError(f"Column '{raw_source}' not found for value_mapping")

        dest = target or source
        df = df.copy()
        mapped_series = df[source].map(mapping)
        if default is not None:
            df[dest] = mapped_series.fillna(default)
        else:
            df[dest] = mapped_series.fillna(df[source])

        return df

    def _concatenate(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        raw_inputs = step.get("source_columns") or step.get("inputs") or []
        target = step.get("target_column") or step.get("target")
        separator = step.get("separator", " ")

        if not raw_inputs or not target:
            raise ValueError("concatenate requires 'source_columns' (or 'inputs') and 'target_column' (or 'target')")

        inputs = [_resolve_col(df, c) for c in raw_inputs]
        missing = [raw_inputs[i] for i, col in enumerate(inputs) if col not in df.columns]
        if missing:
            raise KeyError(f"Columns not found for concatenate: {missing}. Available: {list(df.columns)}")

        df = df.copy()

        def _concat_row(row: pd.Series) -> str:
            parts = []
            for col in inputs:
                val = row[col]
                if pd.notna(val):
                    val_str = str(val).strip()
                    if val_str and val_str not in ("nan", "None", "<NA>"):
                        parts.append(val_str)
            return separator.join(parts)

        df[target] = df.apply(_concat_row, axis=1)
        return df

    def _derive(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        target = step.get("target") or step.get("target_column")
        expression = str(step.get("expression", "")).strip().lower()
        raw_source = step.get("source") or step.get("source_column")
        condition = step.get("condition")

        if not target:
            raise ValueError("derive requires 'target'")

        df = df.copy()

        if condition and isinstance(condition, dict):
            src_col = raw_source or condition.get("source") or condition.get("column")
            if not src_col:
                raise ValueError("derive with condition requires 'source' or condition 'column'")
            source = _resolve_col(df, src_col)
            if source not in df.columns:
                raise KeyError(f"Column '{src_col}' not found for conditional derive")

            op = str(condition.get("operator", "gte")).strip().lower()
            threshold = condition.get("value")
            then_val = condition.get("then")
            else_val = condition.get("else")

            def _eval_cond(val: Any) -> Any:
                if pd.isna(val):
                    return else_val
                try:
                    num_val = float(val) if isinstance(val, (int, float, str)) and str(val).replace(".", "", 1).replace("-", "", 1).isdigit() else val
                    num_thresh = float(threshold) if isinstance(threshold, (int, float, str)) and str(threshold).replace(".", "", 1).replace("-", "", 1).isdigit() else threshold
                except Exception:
                    num_val = val
                    num_thresh = threshold

                if op in ("gte", ">="):
                    matches = num_val >= num_thresh
                elif op in ("gt", ">"):
                    matches = num_val > num_thresh
                elif op in ("lte", "<="):
                    matches = num_val <= num_thresh
                elif op in ("lt", "<"):
                    matches = num_val < num_thresh
                elif op in ("eq", "=="):
                    matches = num_val == num_thresh
                elif op in ("ne", "!="):
                    matches = num_val != num_thresh
                else:
                    matches = False

                return then_val if matches else else_val

            df[target] = df[source].apply(_eval_cond)
            return df

        source = _resolve_col(df, raw_source) if raw_source else None

        if expression == "lowercase":
            if not source or source not in df.columns:
                raise KeyError(f"Column '{raw_source}' not found for lowercase derive")
            df[target] = df[source].astype(str).str.lower()
        elif expression == "uppercase":
            if not source or source not in df.columns:
                raise KeyError(f"Column '{raw_source}' not found for uppercase derive")
            df[target] = df[source].astype(str).str.upper()
        elif expression == "strip":
            if not source or source not in df.columns:
                raise KeyError(f"Column '{raw_source}' not found for strip derive")
            df[target] = df[source].astype(str).str.strip()
        elif expression == "length":
            if not source or source not in df.columns:
                raise KeyError(f"Column '{raw_source}' not found for length derive")
            df[target] = df[source].astype(str).str.len()
        elif expression == "round":
            if not source or source not in df.columns:
                raise KeyError(f"Column '{raw_source}' not found for round derive")
            decimals = int(step.get("decimals", 0))
            df[target] = pd.to_numeric(df[source], errors="coerce").round(decimals)
        elif expression == "constant":
            df[target] = step.get("value", "")
        elif expression in ("add", "subtract", "multiply", "divide"):
            inputs = step.get("inputs", [])
            val = step.get("value")
            if inputs and len(inputs) == 2:
                col1 = _resolve_col(df, inputs[0])
                col2 = _resolve_col(df, inputs[1])
                s1 = pd.to_numeric(df[col1], errors="coerce")
                s2 = pd.to_numeric(df[col2], errors="coerce")
                if expression == "add":
                    df[target] = s1 + s2
                elif expression == "subtract":
                    df[target] = s1 - s2
                elif expression == "multiply":
                    df[target] = s1 * s2
                elif expression == "divide":
                    df[target] = s1 / s2
            elif source and val is not None:
                s1 = pd.to_numeric(df[source], errors="coerce")
                num_val = float(val)
                if expression == "add":
                    df[target] = s1 + num_val
                elif expression == "subtract":
                    df[target] = s1 - num_val
                elif expression == "multiply":
                    df[target] = s1 * num_val
                elif expression == "divide":
                    df[target] = s1 / num_val
            else:
                raise ValueError(f"derive '{expression}' requires two 'inputs' columns or 'source' and 'value'")
        else:
            raise ValueError(f"Unsupported derive expression: '{expression}'")

        return df

    def _split(self, df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
        raw_source = step.get("source") or step.get("source_column")
        separator = step.get("separator", " ")
        target = step.get("target") or step.get("target_column")
        target_columns = step.get("target_columns")

        if not raw_source:
            raise ValueError("split requires 'source'")

        source = _resolve_col(df, raw_source)
        if source not in df.columns:
            raise KeyError(f"Column '{raw_source}' not found for split")

        df = df.copy()
        split_series = df[source].astype(str).str.split(separator)

        if target_columns and isinstance(target_columns, list):
            for i, col_name in enumerate(target_columns):
                df[col_name] = split_series.apply(lambda parts: parts[i] if len(parts) > i else None)
        elif target:
            index = step.get("index")
            if index is not None and isinstance(index, int):
                df[target] = split_series.apply(lambda parts: parts[index] if len(parts) > index else None)
            else:
                df[target] = split_series
        else:
            raise ValueError("split requires 'target' or 'target_columns'")

        return df
