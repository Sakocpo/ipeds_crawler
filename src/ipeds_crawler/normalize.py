from __future__ import annotations
from typing import Any, Iterable, List, Sequence, Tuple
import re
import pandas as pd
from difflib import SequenceMatcher

def normalize(data: Any) -> Any:
    def normalize_value(val: Any) -> Any:
        if isinstance(val, str):
            s = val.strip()
            if s in ("-", "--", "---", "–", "—"):
                return None
            elif s.endswith("%"):
                cleaned = s.strip("%").replace(",", "")
                if cleaned.replace(".", "", 1).isdigit():
                    num = float(cleaned) / 100
                    num = round(num, 2)
                    return int(num) if num.is_integer() else num
                else:
                    return s
            elif s.startswith("$"):
                cleaned = s.replace("$", "").replace(",", "")
                if cleaned.replace(".", "", 1).isdigit():
                    num = float(cleaned)
                    num = round(num, 2)
                    return int(num) if num.is_integer() else num
                else:
                    return s
            else:
                cleaned = s.replace(",", "")
                if cleaned.replace(".", "", 1).isdigit():
                    num = float(cleaned)
                    num = round(num, 2)
                    return int(num) if num.is_integer() else num
                else:
                    return s
        elif isinstance(val, (int, float)):
            num = round(val, 2)
            return int(num) if num.is_integer() else num
        else:
            return val

    if isinstance(data, str):
        return normalize_value(data)

    if isinstance(data, list) and all(not isinstance(i, (list, tuple)) for i in data):
        normalized = [normalize_value(i) for i in data]
        return normalized[0] if len(normalized) == 1 else normalized

    if isinstance(data, (list, tuple)):
        normalized = []
        for row in data:
            new_row = [normalize_value(val) for val in row]
            normalized.append(new_row)
        if len(normalized) == 1 and len(normalized[0]) == 1:
            return normalized[0][0]
        elif len(normalized) == 1:
            return normalized[0]
        return normalized

    return normalize_value(data)


def parse_graph(text: str) -> list[Any]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    result: list[Any] = [lines[0]]
    for line in lines[1:]:
        match = re.match(r"(.+?):\s*([\d.]+)%", line)
        if match:
            label, value = match.groups()
            value = str(float(value) / 100)
            result.append([label.strip(), value])
    return result


def _slugify(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("%", "pct")
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


_NUM_RE = re.compile(r"^-?\d+(\.\d+)?$")


def _coerce(v: Any) -> Any:
    if isinstance(v, str):
        t = v.strip()
        if _NUM_RE.fullmatch(t):
            f = float(t)
            return int(f) if f.is_integer() else f
    return v


def block_to_df(rows: list[list[Any]], labels: list[str], *, mode: str, uni: str | None = None) -> pd.DataFrame:
    feats: dict[str, Any] = {}

    if mode == "replace":
        assert len(labels) == len(rows), "replace mode: len(labels) must equal len(rows)"
        for lab, r in zip(labels, rows):
            key = _slugify(lab)
            val = None
            for v in r[1:]:
                if v not in (None, "", "-"):
                    val = v
                    break
            feats[key] = _coerce(val)

    elif mode == "append":
        if len(labels) == 1:
            suffix = _slugify(labels[0])
            for r in rows:
                base = _slugify(r[0]) if r else ""
                val = None
                for v in r[1:]:
                    if v not in (None, "", "-"):
                        val = v
                        break
                feats[f"{base}_{suffix}"] = _coerce(val)
        else:
            for r in rows:
                base = _slugify(r[0]) if r else ""
                for j, lab in enumerate(labels, start=1):
                    key = f"{base}_{_slugify(lab)}"
                    val = r[j] if len(r) > j else None
                    feats[key] = _coerce(val)
    else:
        raise ValueError("mode must be 'replace' or 'append'")

    if uni is not None:
        feats = {"uni": uni, **feats}
    return pd.DataFrame([feats])


def graph_to_df(rows: list[list[Any]], *, mode: str, uni: str | None = None) -> pd.DataFrame:
    feats: dict[str, Any] = {}

    if mode == "append":
        for group in rows:
            base = _slugify(group[0]) if group else ""
            for sublab, val in group[1:]:
                key = f"{base}_{_slugify(sublab)}"
                feats[key] = _coerce(val)
    elif mode == "replace":
        counts: dict[str, int] = {}
        for group in rows:
            for sublab, val in group[1:]:
                base_key = _slugify(sublab)
                counts[base_key] = counts.get(base_key, 0) + 1
                key = base_key if counts[base_key] == 1 else f"{base_key}_{counts[base_key]}"
                feats[key] = _coerce(val)
    else:
        raise ValueError("mode must be 'append' or 'replace'")

    if uni is not None:
        feats = {"uni": uni, **feats}
    return pd.DataFrame([feats])


def get_best_unitid(df: pd.DataFrame, name: str, threshold: float = 0.75) -> tuple[str, str, float] | None:
    if df.empty or not name:
        return None

    df = df.assign(
        sim=df["INSTNM"].apply(lambda s: SequenceMatcher(None, str(s).lower(), name.lower()).ratio())
    )

    best = df.loc[df["sim"].idxmax()]
    if best["sim"] < threshold:
        print(f"Low confidence match ({best['sim']:.2f}) for '{name}' → '{best['INSTNM']}'")
        return None
    return str(best["UNITID"]), best["INSTNM"], float(best["sim"])


def label_dict(base: str, labels: list[str], values: list[Any], position: str = "last") -> dict[str, Any]:
    if position == "first":
        return {f"{base}_{label}": v for label, v in zip(labels, values)}
    elif position == "last":
        return {f"{label}_{base}": v for label, v in zip(labels, values)}
    else:
        raise ValueError("position must be 'first' or 'last'")


def build_labeled_dict(*specs, position: str = "last") -> dict[str, Any]:
    out: dict[str, Any] = {}

    for spec in specs:
        if not (4 <= len(spec) <= 5):
            raise ValueError(
                "Each spec must be (base, labels, values, slc) or "
                "(base, labels, values, slc, position)."
            )

        if len(spec) == 4:
            base, labels, values, slc = spec
            spec_position = position
        else:
            base, labels, values, slc, spec_position = spec

        if spec_position not in ("first", "last"):
            raise ValueError("position must be 'first' or 'last'.")

        if isinstance(labels, str):
            labels = [labels]
        elif not isinstance(labels, (list, tuple)):
            raise TypeError(f"labels must be str, list, or tuple — got {type(labels).__name__}")

        vals = values[slc] if slc is not None else values

        if isinstance(vals, (str, int, float, bool)) or vals is None:
            vals = [vals]
        elif not isinstance(vals, (list, tuple)):
            vals = list(vals)

        if len(vals) < len(labels):
            vals = list(vals) + [None] * (len(labels) - len(vals))

        vals = [None if v == "" else v for v in vals]

        piece: dict[str, Any] = {}
        for lab, v in zip(labels, vals):
            if spec_position == "first":
                key = f"{base}_{lab}" if lab else base
            else:
                key = f"{lab}_{base}" if lab else base
            piece[key] = v

        out.update(piece)

    return out
