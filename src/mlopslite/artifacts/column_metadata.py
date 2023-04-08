from dataclasses import dataclass


@dataclass
class ColumnMetadata:
    null_count: int
    unique_count: int
    min_val_num: float | int
    max_val_num: float | int
    unique_val_str: list[str]

    @staticmethod
    def create(column: list) -> "ColumnMetadata":
        # summarize col
        summary = {
            "null_count": len([i for i in column if i is None]),
            "unique_count": len(set([i for i in column if i is not None])),
            "min_value_num": min([i for i in column if i is not None]),
            "max_value_num": max([i for i in column if i is not None]),
            "unique_vals": list(set(column)),
        }
        return ColumnMetadata(**summary)
