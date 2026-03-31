import argparse
from datetime import datetime, timezone
import json
import os
import time
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Any

API_URL = "https://api.encar.com/search/car/list/mobile"
DEFAULT_QUERY = "(And.Hidden.N._.CarType.A.)"
DEFAULT_BATCH_SIZE = 200
REQUEST_TIMEOUT_SECONDS = 90
REQUEST_RETRIES = 3


def build_sr(start: int, end: int) -> str:
    return f"|ModifiedDate|{start}|{end}"


def fetch_batch(
    start: int,
    batch_size: int,
    query: str = DEFAULT_QUERY,
) -> dict[str, Any]:
    end = start + batch_size
    params = {
        "count": "true",
        "q": query,
        "sr": build_sr(start, end),
        "inav": "|Metadata|Sort",
    }
    url = f"{API_URL}?{urlencode(params)}"
    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload_bytes = response.read()
            return json.loads(payload_bytes.decode("utf-8"))
        except (TimeoutError, URLError) as exc:
            last_error = exc
            if attempt < REQUEST_RETRIES:
                time.sleep(1.0 * attempt)
                continue
            break
    raise RuntimeError(f"Failed to fetch ENCAR API after retries: {last_error}")


def extract_items(search_results: Any) -> list[dict[str, Any]]:
    if isinstance(search_results, list):
        return [item for item in search_results if isinstance(item, dict)]

    if isinstance(search_results, dict):
        for key in ("Cars", "items", "results", "SearchResults"):
            value = search_results.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    return []


def print_diagnostics(payload: dict[str, Any]) -> list[dict[str, Any]]:
    print("data.keys():", list(payload.keys()))

    search_results = payload.get("SearchResults")
    print("type(data['SearchResults']):", type(search_results))

    items = extract_items(search_results)
    if isinstance(search_results, dict):
        print("SearchResults dict keys:", list(search_results.keys()))
    elif isinstance(search_results, list):
        print("SearchResults list len:", len(search_results))
        if len(search_results) > 0 and isinstance(search_results[0], dict):
            print("first item keys:", list(search_results[0].keys()))

    print("extracted current batch size:", len(items))
    if items:
        print("first extracted car object exists.")

    print("Count:", payload.get("Count"))
    return items


def extract_ids(items: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for item in items:
        raw_id = item.get("Id")
        if raw_id is None:
            continue
        ids.append(str(raw_id))
    return ids


def normalize_year(raw_year: Any) -> Any:
    if raw_year is None:
        return None
    try:
        year_value = int(float(raw_year))
    except (TypeError, ValueError):
        return raw_year
    if year_value >= 100000:
        return year_value // 100
    return year_value


def extract_photo_url(item: dict[str, Any]) -> Any:
    photos = item.get("Photos")
    if isinstance(photos, list) and photos:
        first_photo = photos[0]
        if isinstance(first_photo, dict):
            location = first_photo.get("location")
            if isinstance(location, str) and location:
                return location

    main_photo = item.get("Photo")
    if isinstance(main_photo, str) and main_photo:
        if main_photo.endswith("_"):
            return f"{main_photo}001.jpg"
        return main_photo

    return None


def normalize_car_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "марка": item.get("Manufacturer"),
        "модель": item.get("Model"),
        "год": normalize_year(item.get("Year")),
        "пробег": item.get("Mileage"),
        "цена": item.get("Price"),
        "фото": extract_photo_url(item),
    }


def save_json_atomic(data: Any, output_path: str) -> None:
    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)
    temp_path = f"{output_path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    for _ in range(5):
        try:
            os.replace(temp_path, output_path)
            return
        except PermissionError:
            time.sleep(0.2)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    if os.path.exists(temp_path):
        os.remove(temp_path)


def collect_all_batches(
    batch_size: int,
    max_batches: int | None,
    query: str,
    output_path: str | None,
    metadata_path: str | None = None,
) -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []
    normalized_items: list[dict[str, Any]] = []
    start = 0
    total_count: int | None = None
    batch_index = 0
    previous_batch_ids: set[str] = set()

    while True:
        payload = fetch_batch(start=start, batch_size=batch_size, query=query)
        if total_count is None and isinstance(payload.get("Count"), int):
            total_count = payload["Count"]
            print(f"API total Count: {total_count}")

        items = extract_items(payload.get("SearchResults"))
        batch_ids = extract_ids(items)
        batch_unique_ids = set(batch_ids)
        batch_duplicates = len(batch_ids) - len(batch_unique_ids)
        overlap_prev = previous_batch_ids.intersection(batch_unique_ids)
        first_id = batch_ids[0] if batch_ids else None
        last_id = batch_ids[-1] if batch_ids else None

        print(f"batch {batch_index}: sr={build_sr(start, start + batch_size)}, items={len(items)}")
        print(
            f"  ids total={len(batch_ids)}, unique={len(batch_unique_ids)}, "
            f"duplicates={batch_duplicates}, overlap_with_prev={len(overlap_prev)}"
        )
        print(f"  first_id={first_id}, last_id={last_id}")

        if not items:
            print("No more items in batch, stop.")
            break

        all_items.extend(items)
        normalized_items.extend(normalize_car_item(item) for item in items)
        previous_batch_ids = batch_unique_ids
        batch_index += 1
        start += batch_size

        if max_batches is not None and batch_index >= max_batches:
            print(f"Reached max_batches={max_batches}, stop.")
            break

        if total_count is not None and start >= total_count:
            print("Collected up to Count boundary, stop.")
            break

    print(f"Total collected objects: {len(all_items)}")
    all_ids = extract_ids(all_items)
    unique_ids = set(all_ids)
    print(f"ID check -> total: {len(all_ids)}")
    print(f"ID check -> unique: {len(unique_ids)}")
    print(f"ID check -> duplicates: {len(all_ids) - len(unique_ids)}")
    if normalized_items:
        print("first normalized car object prepared.")

    if output_path:
        save_json_atomic(normalized_items, output_path)
        print(f"Saved JSON to: {output_path}")
    if metadata_path:
        metadata = {
            "total_on_site": total_count,
            "collected_in_file": len(normalized_items),
            "batch_size": batch_size,
            "max_batches": max_batches,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        save_json_atomic(metadata, metadata_path)
        print(f"Saved metadata to: {metadata_path}")

    return normalized_items


def _collect_batches_for_query(
    query: str,
    batch_size: int,
    max_batches: int | None,
    verbose: bool = True,
) -> tuple[list[dict[str, Any]], int | None]:
    all_items: list[dict[str, Any]] = []
    start = 0
    total_count: int | None = None
    batch_index = 0

    while True:
        payload = fetch_batch(start=start, batch_size=batch_size, query=query)
        if total_count is None and isinstance(payload.get("Count"), int):
            total_count = payload["Count"]
            if verbose:
                print(f"API total Count for query: {total_count}")

        items = extract_items(payload.get("SearchResults"))
        if verbose:
            print(f"batch {batch_index}: sr={build_sr(start, start + batch_size)}, items={len(items)}")
        if not items:
            break

        all_items.extend(items)
        batch_index += 1
        start += batch_size

        if max_batches is not None and batch_index >= max_batches:
            break
        if total_count is not None and start >= total_count:
            break

    return all_items, total_count


def discover_manufacturer_queries(base_query: str = DEFAULT_QUERY) -> list[str]:
    return discover_queries_by_field(base_query, "Manufacturer")


def discover_queries_by_field(query: str, field_name: str) -> list[str]:
    payload = fetch_batch(start=0, batch_size=1, query=query)
    inav = payload.get("iNav")
    if not isinstance(inav, dict):
        return []

    found: set[str] = set()
    stack: list[Any] = [inav]
    token = f".{field_name}."
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
        elif isinstance(current, str):
            if token in current and "CarType.A" in current:
                found.add(current)

    return sorted(found)


def collect_query_recursive(
    query: str,
    batch_size: int,
    depth: int = 0,
    max_depth: int = 2,
) -> list[dict[str, Any]]:
    items, total = _collect_batches_for_query(
        query=query,
        batch_size=batch_size,
        max_batches=None,
        verbose=False,
    )

    if total is None or len(items) >= total:
        return items
    if depth >= max_depth:
        return items

    next_field = "Model" if depth == 0 else "Badge"
    child_queries = discover_queries_by_field(query, next_field)
    if not child_queries:
        return items

    print(
        f"Query truncated (got {len(items)} of {total}). "
        f"Splitting by {next_field}: {len(child_queries)} segments."
    )
    merged: list[dict[str, Any]] = []
    for child_query in child_queries:
        merged.extend(
            collect_query_recursive(
                query=child_query,
                batch_size=batch_size,
                depth=depth + 1,
                max_depth=max_depth,
            )
        )
    return merged


def collect_all_cars_segmented(
    output_path: str,
    metadata_path: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[dict[str, Any]]:
    base_payload = fetch_batch(start=0, batch_size=1, query=DEFAULT_QUERY)
    total_on_site = base_payload.get("Count") if isinstance(base_payload.get("Count"), int) else None
    manufacturer_queries = discover_manufacturer_queries(DEFAULT_QUERY)

    if not manufacturer_queries:
        print("Manufacturer segments were not discovered. Fallback to base query.")
        return collect_all_batches(
            batch_size=batch_size,
            max_batches=None,
            query=DEFAULT_QUERY,
            output_path=output_path,
            metadata_path=metadata_path,
        )

    dedup_by_id: dict[str, dict[str, Any]] = {}
    without_id_count = 0
    raw_collected = 0

    print(f"Segmented collection started. Manufacturers: {len(manufacturer_queries)}")
    for index, manufacturer_query in enumerate(manufacturer_queries, start=1):
        segment_items = collect_query_recursive(
            query=manufacturer_query,
            batch_size=batch_size,
            depth=0,
            max_depth=2,
        )
        segment_total_payload = fetch_batch(start=0, batch_size=1, query=manufacturer_query)
        segment_total = segment_total_payload.get("Count")
        raw_collected += len(segment_items)
        print(
            f"[{index}/{len(manufacturer_queries)}] segment count={segment_total}, "
            f"fetched={len(segment_items)}"
        )

        for item in segment_items:
            raw_id = item.get("Id")
            if raw_id is None:
                without_id_count += 1
                continue
            dedup_by_id[str(raw_id)] = normalize_car_item(item)

    result = list(dedup_by_id.values())
    save_json_atomic(result, output_path)
    print(f"Saved JSON to: {output_path}")
    print(
        f"Segmented done. total_on_site={total_on_site}, raw_collected={raw_collected}, "
        f"unique_by_id={len(result)}, without_id={without_id_count}"
    )

    if metadata_path:
        metadata = {
            "total_on_site": total_on_site,
            "collected_in_file": len(result),
            "raw_collected": raw_collected,
            "manufacturers_count": len(manufacturer_queries),
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        save_json_atomic(metadata, metadata_path)
        print(f"Saved metadata to: {metadata_path}")

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="ENCAR API diagnostic + batch collector")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Limit number of batches. By default collects all batches.",
    )
    parser.add_argument("--query", type=str, default=DEFAULT_QUERY)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    first_payload = fetch_batch(
        start=0,
        batch_size=args.batch_size,
        query=args.query,
    )

    print("=== Diagnostic for first batch ===")
    print_diagnostics(first_payload)
    print()
    print("=== Batch pagination check ===")
    collect_all_batches(
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        query=args.query,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
