import pandas as pd
import logging
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Generator
import requests
import time
from tqdm import tqdm

logger = logging.getLogger(__name__)



PREZIP_BASE_URL = "https://transtats.bts.gov/PREZIP"
DB1B_FILENAME_TEMPLATE = (
    "Origin_and_Destination_Survey_DB1BMarket_{year}_{quarter}.zip"
)

CHUNK_SIZE_ROWS = 500_000


DB1B_COLUMNS_OF_INTEREST = [
    "ItinID",
    "MktID",
    "MktCoupons",
    "Year",
    "Quarter",
    "OriginAirportID",
    "Origin",
    "OriginCountry",
    "OriginState",
    "DestAirportID",
    "Dest",
    "DestCountry",
    "DestState",
    "AirportGroup",
    "TkCarrierChange",
    "TkCarrierGroup",
    "OpCarrierChange",
    "OpCarrierGroup",
    "RPCarrier",
    "TkCarrier",
    "OpCarrier",
    "BulkFare",
    "Passengers",
    "MktFare",
    "MktDistance",
    "MktDistanceGroup",
    "MktMilesFlown",
    "NonStopMiles",
    "ItinGeoType",
    "MktGeoType",
]

DB1B_CSV_DTYPES = {
    "ItinID": "int64",
    "MktID": "int64",
    "MktCoupons": "int8",
    "Year": "int16",
    "Quarter": "int8",
    "OriginAirportID": "int32",
    "Origin": "str",
    "OriginCountry": "str",
    "OriginState": "str",
    "DestAirportID": "int32",
    "Dest": "str",
    "DestCountry": "str",
    "DestState": "str",
    "AirportGroup": "str",
    "TkCarrierChange": "float64",
    "TkCarrierGroup": "str",
    "OpCarrierChange": "float64",
    "OpCarrierGroup": "str",
    "RPCarrier": "str",
    "TkCarrier": "str",
    "OpCarrier": "str",
    "BulkFare": "float64",
    "Passengers": "float64",
    "MktFare": "float64",
    "MktDistance": "float64",
    "MktDistanceGroup": "int8",
    "MktMilesFlown": "float64",
    "NonStopMiles": "float64",
    "ItinGeoType": "int8",
    "MktGeoType": "int8",
}

DB1B_PARQUET_SCHEMA = pa.schema([
    ("ItinID", pa.int64()),
    ("MktID", pa.int64()),
    ("MktCoupons", pa.int8()),
    ("Year", pa.int16()),
    ("Quarter", pa.int8()),
    ("OriginAirportID", pa.int32()),
    ("Origin", pa.string()),
    ("OriginCountry", pa.string()),
    ("OriginState", pa.string()),
    ("DestAirportID", pa.int32()),
    ("Dest", pa.string()),
    ("DestCountry", pa.string()),
    ("DestState", pa.string()),
    ("AirportGroup", pa.string()),
    ("TkCarrierChange", pa.float64()),
    ("TkCarrierGroup", pa.string()),
    ("OpCarrierChange", pa.float64()),
    ("OpCarrierGroup", pa.string()),
    ("RPCarrier", pa.string()),
    ("TkCarrier", pa.string()),
    ("OpCarrier", pa.string()),
    ("BulkFare", pa.float64()),
    ("Passengers", pa.float64()),
    ("MktFare", pa.float64()),
    ("MktDistance", pa.float64()),
    ("MktDistanceGroup", pa.int8()),
    ("MktMilesFlown", pa.float64()),
    ("NonStopMiles", pa.float64()),
    ("ItinGeoType", pa.int8()),
    ("MktGeoType", pa.int8()),
])



@dataclass
class Quarter:
    """Represents a single year-quarter combination."""

    year: int
    quarter: int

    def __post_init__(self) -> None:
        if not 2000 <= self.year <= 2030:
            raise ValueError(f"Year out of range: {self.year}")
        if self.quarter not in (1, 2, 3, 4):
            raise ValueError(f"Invalid quarter: {self.quarter}")

    def __str__(self) -> str:
        return f"{self.year}_Q{self.quarter}"

    def __lt__(self, other: "Quarter") -> bool:
        return (self.year, self.quarter) < (other.year, other.quarter)

    @property
    def filename(self) -> str:
        return DB1B_FILENAME_TEMPLATE.format(
            year=self.year, quarter=self.quarter
        )

    @property
    def url(self) -> str:
        return f"{PREZIP_BASE_URL}/{self.filename}"


@dataclass
class QuarterStatus:
    """Pipeline status for a single quarter."""

    quarter: Quarter
    has_zip: bool = False
    has_csv: bool = False
    has_parquet: bool = False
    has_processed: bool = False
    zip_size_mb: float = 0.0
    csv_size_mb: float = 0.0
    parquet_size_mb: float = 0.0
    parquet_rows: int = 0
    parquet_valid: bool = False

    @property
    def stage(self) -> str:
        """Human-readable current pipeline stage."""
        if self.has_processed:
            return "processed"
        if self.has_parquet:
            return "parquet"
        if self.has_csv:
            return "csv"
        if self.has_zip:
            return "raw"
        return "not_started"

    @property
    def next_action(self) -> str:
        """What needs to happen next for this quarter."""
        if self.has_processed:
            return "complete"
        if self.has_parquet and self.parquet_valid:
            return "process_features"
        if self.has_parquet and not self.parquet_valid:
            return "reconvert"
        if self.has_csv:
            return "convert_to_parquet"
        if self.has_zip:
            return "extract_csv"
        return "download"


@dataclass
class DownloadResult:
    """Outcome of a single quarter download attempt."""

    quarter: Quarter
    success: bool
    zip_path: Path | None = None
    csv_path: Path | None = None
    size_mb: float = 0.0
    error: str | None = None


@dataclass
class ConversionResult:
    """Outcome of a CSV-to-Parquet conversion."""

    quarter: Quarter
    success: bool
    parquet_path: Path | None = None
    csv_size_mb: float = 0.0
    parquet_size_mb: float = 0.0
    row_count: int = 0
    compression_ratio: float = 0.0
    error: str | None = None


@dataclass
class PipelineReport:
    """Summary of a full pipeline run."""

    start_time: float
    end_time: float = 0.0
    quarters_processed: int = 0
    quarters_skipped: int = 0
    quarters_failed: int = 0
    total_downloaded_mb: float = 0.0
    total_converted_rows: int = 0
    total_parquet_mb: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def elapsed_minutes(self) -> float:
        return (self.end_time - self.start_time) / 60

    def print_report(self) -> None:
        print(f"\n{'=' * 50}")
        print("PIPELINE RUN REPORT")
        print(f"{'=' * 50}")
        print(f"  Duration:           {self.elapsed_minutes:.1f} minutes")
        print(f"  Processed:          {self.quarters_processed}")
        print(f"  Skipped:            {self.quarters_skipped}")
        print(f"  Failed:             {self.quarters_failed}")
        print(f"  Downloaded:         {self.total_downloaded_mb:.1f} MB")
        print(f"  Rows converted:     {self.total_converted_rows:,}")
        print(f"  Parquet on disk:    {self.total_parquet_mb:.1f} MB")
        if self.errors:
            print(f"  Errors:")
            for err in self.errors:
                print(f"    - {err}")
        print(f"{'=' * 50}")


@dataclass
class BTSDataHandler:
    """Manages download, extraction, and conversion of BTS DB1B Market data.

    Directory structure created:
        data/bts/raw/        -- downloaded ZIP files
        data/bts/csv/        -- extracted CSV files
        data/bts/parquet/    -- converted Parquet files (separate subtask)
        data/bts/processed/  -- feature-engineered files (separate subtask)
    """

    base_dir: Path = field(default_factory=lambda: Path("data/bts"))
    request_timeout: int = 120
    max_retries: int = 3
    retry_delay: float = 5.0
    chunk_size: int = 8192  # bytes per streaming chunk

    def __post_init__(self) -> None:
        self.raw_dir = self.base_dir / "raw"
        self.csv_dir = self.base_dir / "csv"
        self.parquet_dir = self.base_dir / "parquet"
        self.processed_dir = self.base_dir / "processed"

        for directory in (
            self.raw_dir,
            self.csv_dir,
            self.parquet_dir,
            self.processed_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)

        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": "FlightScanner-Research/1.0"}
        )

    @staticmethod
    def enumerate_quarters(
        start: Quarter, end: Quarter
    ) -> list[Quarter]:
        """Generate list of quarters from start to end (inclusive).

        Args:
            start: First quarter in range.
            end: Last quarter in range.

        Returns:
            Ordered list of Quarter objects.

        Example:
            >>> BTSDataHandler.enumerate_quarters(
            ...     Quarter(2024, 1), Quarter(2025, 2)
            ... )
            [Quarter(2024,1), Quarter(2024,2), ..., Quarter(2025,2)]
        """
        if end < start:
            raise ValueError(f"End {end} is before start {start}")

        quarters = []
        year, qtr = start.year, start.quarter
        while (year, qtr) <= (end.year, end.quarter):
            quarters.append(Quarter(year, qtr))
            qtr += 1
            if qtr > 4:
                qtr = 1
                year += 1
        return quarters

    def _download_file(self, url: str, dest: Path) -> Path:
        """Download a file with streaming, retries, resume, and progress bar."""
        for attempt in range(1, self.max_retries + 1):
            try:
                headers = {}
                existing_size = dest.stat().st_size if dest.exists() else 0
                if existing_size > 0:
                    headers["Range"] = f"bytes={existing_size}-"
                    logger.info(
                        "Resuming download from byte %d", existing_size
                    )

                response = self._session.get(
                    url,
                    stream=True,
                    timeout=self.request_timeout,
                    headers=headers,
                )
                response.raise_for_status()

                total_size = int(response.headers.get("Content-Length", 0))
                mode = (
                    "ab"
                    if existing_size > 0 and response.status_code == 206
                    else "wb"
                )

                with (
                    open(dest, mode) as f,
                    tqdm(
                        total=total_size + existing_size,
                        initial=existing_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=dest.name[:40],
                        leave=False,
                    ) as pbar,
                ):
                    for chunk in response.iter_content(
                        chunk_size=self.chunk_size
                    ):
                        f.write(chunk)
                        pbar.update(len(chunk))

                final_mb = dest.stat().st_size / (1024 * 1024)
                logger.info(
                    "Download complete: %.1f MB -> %s", final_mb, dest
                )
                return dest

            except (
                requests.ConnectionError,
                requests.Timeout,
                requests.HTTPError,
            ) as exc:
                logger.warning(
                    "Attempt %d/%d failed for %s: %s",
                    attempt,
                    self.max_retries,
                    url,
                    exc,
                )
                if attempt < self.max_retries:
                    wait = self.retry_delay * attempt
                    logger.info("Retrying in %.1f seconds...", wait)
                    time.sleep(wait)
                else:
                    raise

    def _extract_zip(self, zip_path: Path) -> Path:
        """Extract CSV from a DB1B Market ZIP file.

        Each ZIP typically contains a single CSV file.

        Args:
            zip_path: Path to the ZIP file.

        Returns:
            Path to the extracted CSV file.

        Raises:
            zipfile.BadZipFile: If the ZIP is corrupted.
            FileNotFoundError: If no CSV found inside ZIP.
        """
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [
                name for name in zf.namelist() if name.endswith(".csv")
            ]
            if not csv_names:
                raise FileNotFoundError(
                    f"No CSV file found in {zip_path}"
                )

            csv_name = csv_names[0]
            if len(csv_names) > 1:
                logger.warning(
                    "Multiple CSVs in %s, extracting first: %s",
                    zip_path,
                    csv_name,
                )

            zf.extract(csv_name, self.csv_dir)
            extracted_path = self.csv_dir / csv_name
            size_mb = extracted_path.stat().st_size / (1024 * 1024)
            logger.info(
                "Extracted %s (%.1f MB)", extracted_path, size_mb
            )
            return extracted_path

    def download_quarter(
        self, quarter: Quarter, skip_existing: bool = True
    ) -> DownloadResult:
        """Download and extract a single quarter of DB1B Market data.

        Args:
            quarter: The year-quarter to download.
            skip_existing: If True, skip download when CSV already exists.

        Returns:
            DownloadResult with outcome details.
        """
        csv_path = self._find_existing_csv(quarter)
        if skip_existing and csv_path is not None:
            size_mb = csv_path.stat().st_size / (1024 * 1024)
            logger.info("Already exists, skipping: %s", csv_path)
            return DownloadResult(
                quarter=quarter,
                success=True,
                csv_path=csv_path,
                size_mb=size_mb,
            )

        zip_dest = self.raw_dir / quarter.filename
        try:
            self._download_file(quarter.url, zip_dest)
            csv_path = self._extract_zip(zip_dest)
            size_mb = csv_path.stat().st_size / (1024 * 1024)

            return DownloadResult(
                quarter=quarter,
                success=True,
                zip_path=zip_dest,
                csv_path=csv_path,
                size_mb=size_mb,
            )

        except (requests.HTTPError, zipfile.BadZipFile, OSError) as exc:
            logger.error("Failed to download %s: %s", quarter, exc)
            # Clean up partial downloads
            if zip_dest.exists():
                zip_dest.unlink()
            return DownloadResult(
                quarter=quarter,
                success=False,
                error=str(exc),
            )

    def download_range(
        self,
        start: Quarter,
        end: Quarter,
        skip_existing: bool = True,
    ) -> list[DownloadResult]:
        """Download multiple quarters with progress tracking."""
        quarters = self.enumerate_quarters(start, end)
        logger.info(
            "Downloading %d quarters: %s to %s",
            len(quarters),
            start,
            end,
        )

        results = []
        for quarter in tqdm(
            quarters, desc="Downloading quarters", unit="quarter"
        ):
            result = self.download_quarter(
                quarter, skip_existing=skip_existing
            )
            results.append(result)

            if not result.success:
                logger.warning("Failed: %s -- %s", quarter, result.error)

            if quarter != quarters[-1]:
                time.sleep(1.0)

        succeeded = sum(1 for r in results if r.success)
        total_mb = sum(r.size_mb for r in results if r.success)
        logger.info(
            "Download complete: %d/%d succeeded (%.1f MB total)",
            succeeded,
            len(results),
            total_mb,
        )
        return results


    def _find_existing_csv(self, quarter: Quarter) -> Path | None:
        """Check if CSV for a given quarter already exists."""
        exact_pattern = f"_{quarter.year}_{quarter.quarter}"

        # Primary: glob for DB1BMarket files with exact year_quarter
        for csv_file in self.csv_dir.glob("*.csv"):
            if "DB1B" in csv_file.name.upper() and exact_pattern in csv_file.name:
                return csv_file
        return None


    def list_available_quarters(self) -> dict[str, list[Quarter]]:
        """Report which quarters are available locally at each stage.

        Returns:
            Dictionary with keys 'raw', 'csv', 'parquet', 'processed'
            mapping to lists of detected Quarter objects.
        """
        stages = {
            "raw": (self.raw_dir, "*.zip"),
            "csv": (self.csv_dir, "*.csv"),
            "parquet": (self.parquet_dir, "*.parquet"),
            "processed": (self.processed_dir, "*.parquet"),
        }
        result = {}
        for stage_name, (directory, pattern) in stages.items():
            quarters = []
            for file_path in sorted(directory.glob(pattern)):
                q = self._parse_quarter_from_filename(file_path.name)
                if q is not None:
                    quarters.append(q)
            result[stage_name] = quarters
        return result

    @staticmethod
    def _parse_quarter_from_filename(filename: str) -> Quarter | None:
        """Extract year and quarter from a BTS filename."""
        import re

        match = re.search(r"(\d{4})[_\-](\d)", filename)
        if match:
            try:
                return Quarter(int(match.group(1)), int(match.group(2)))
            except ValueError:
                return None
        return None

    def cleanup_zips(self, keep_latest: int = 0) -> int:
        """Remove downloaded ZIP files to reclaim disk space.

        Args:
            keep_latest: Number of most recent ZIPs to retain.
                         0 means delete all.

        Returns:
            Number of files deleted.
        """
        zips = sorted(self.raw_dir.glob("*.zip"))
        to_delete = zips if keep_latest == 0 else zips[:-keep_latest]
        for zip_path in to_delete:
            size_mb = zip_path.stat().st_size / (1024 * 1024)
            zip_path.unlink()
            logger.info("Deleted %s (%.1f MB)", zip_path.name, size_mb)
        return len(to_delete)



    # --- CSV to Parquet Conversion ---
    def convert_quarter_to_parquet(
        self,
        quarter: Quarter,
        skip_existing: bool = True,
        delete_csv: bool = False,
    ) -> ConversionResult:
        """Convert a single quarter CSV to Parquet with progress tracking."""
        parquet_path = self._quarter_parquet_path(quarter)

        if skip_existing and parquet_path.exists():
            size_mb = parquet_path.stat().st_size / (1024 * 1024)
            row_count = pq.read_metadata(parquet_path).num_rows
            logger.info(
                "Parquet already exists, skipping: %s (%d rows, %.1f MB)",
                parquet_path,
                row_count,
                size_mb,
            )
            return ConversionResult(
                quarter=quarter,
                success=True,
                parquet_path=parquet_path,
                parquet_size_mb=size_mb,
                row_count=row_count,
            )

        csv_path = self._find_existing_csv(quarter)
        if csv_path is None:
            return ConversionResult(
                quarter=quarter,
                success=False,
                error=f"No CSV found for {quarter}",
            )

        csv_size_mb = csv_path.stat().st_size / (1024 * 1024)
        csv_size_bytes = csv_path.stat().st_size
        logger.info(
            "Converting %s (%.1f MB CSV) -> Parquet", quarter, csv_size_mb
        )

        try:
            total_rows = 0
            writer = None
            start_time = time.time()

            reader = pd.read_csv(
                csv_path,
                chunksize=CHUNK_SIZE_ROWS,
                usecols=lambda col: col.strip() in DB1B_COLUMNS_OF_INTEREST,
                dtype={
                    k: v
                    for k, v in DB1B_CSV_DTYPES.items()
                    if k in DB1B_COLUMNS_OF_INTEREST
                },
                low_memory=False,
                encoding="utf-8",
                on_bad_lines="warn",
            )

            # Estimate total chunks from file size
            # ~250 bytes per row in CSV is a reasonable estimate for DB1B
            estimated_rows = csv_size_bytes // 250
            estimated_chunks = max(1, estimated_rows // CHUNK_SIZE_ROWS)

            with tqdm(
                total=estimated_chunks,
                desc=f"Converting {quarter}",
                unit="chunk",
                leave=False,
            ) as pbar:
                for chunk_idx, chunk in enumerate(reader):
                    chunk.columns = chunk.columns.str.strip()

                    available_cols = [
                        col for col in DB1B_COLUMNS_OF_INTEREST
                        if col in chunk.columns
                    ]
                    chunk = chunk[available_cols]
                    chunk = self._clean_chunk(chunk)

                    table = pa.Table.from_pandas(
                        chunk,
                        schema=DB1B_PARQUET_SCHEMA,
                        preserve_index=False,
                    )

                    if writer is None:
                        writer = pq.ParquetWriter(
                            parquet_path,
                            schema=DB1B_PARQUET_SCHEMA,
                            compression="snappy",
                            version="2.6",
                        )

                    writer.write_table(table)
                    total_rows += len(chunk)

                    pbar.update(1)
                    pbar.set_postfix(
                        rows=f"{total_rows:,}",
                        rate=f"{total_rows / (time.time() - start_time):,.0f} rows/s",
                    )

                    # Update total if estimate was off
                    if chunk_idx + 1 > estimated_chunks:
                        pbar.total = chunk_idx + 2
                        pbar.refresh()

            if writer is not None:
                writer.close()

            elapsed = time.time() - start_time
            parquet_size_mb = parquet_path.stat().st_size / (1024 * 1024)
            ratio = csv_size_mb / parquet_size_mb if parquet_size_mb > 0 else 0

            logger.info(
                "Conversion complete: %s | %d rows | "
                "%.1f MB -> %.1f MB (%.1fx) | %.1fs (%.0f rows/s)",
                quarter,
                total_rows,
                csv_size_mb,
                parquet_size_mb,
                ratio,
                elapsed,
                total_rows / elapsed if elapsed > 0 else 0,
            )

            if delete_csv:
                csv_path.unlink()
                logger.info("Deleted CSV: %s", csv_path)

            return ConversionResult(
                quarter=quarter,
                success=True,
                parquet_path=parquet_path,
                csv_size_mb=csv_size_mb,
                parquet_size_mb=parquet_size_mb,
                row_count=total_rows,
                compression_ratio=ratio,
            )

        except Exception as exc:
            logger.error("Conversion failed for %s: %s", quarter, exc)
            if parquet_path.exists():
                parquet_path.unlink()
            if writer is not None:
                writer.close()
            return ConversionResult(
                quarter=quarter,
                success=False,
                error=str(exc),
            )

    def convert_range_to_parquet(
        self,
        start: Quarter,
        end: Quarter,
        skip_existing: bool = True,
        delete_csv: bool = False,
    ) -> list[ConversionResult]:
        """Convert multiple quarters with progress tracking."""
        quarters = self.enumerate_quarters(start, end)
        logger.info("Converting %d quarters to Parquet", len(quarters))

        results = []
        for quarter in tqdm(
            quarters, desc="Converting quarters", unit="quarter"
        ):
            result = self.convert_quarter_to_parquet(
                quarter,
                skip_existing=skip_existing,
                delete_csv=delete_csv,
            )
            results.append(result)

        succeeded = [r for r in results if r.success]
        total_csv = sum(r.csv_size_mb for r in succeeded)
        total_parquet = sum(r.parquet_size_mb for r in succeeded)
        total_rows = sum(r.row_count for r in succeeded)

        logger.info(
            "Batch conversion: %d/%d succeeded | %d total rows | "
            "%.1f MB CSV -> %.1f MB Parquet",
            len(succeeded),
            len(results),
            total_rows,
            total_csv,
            total_parquet,
        )
        return results


    @staticmethod
    def _clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
        """Apply basic cleaning to a chunk of DB1B data."""
        initial_rows = len(chunk)

        # Drop phantom column from trailing comma
        phantom_cols = [c for c in chunk.columns if c.startswith("Unnamed")]
        if phantom_cols:
            chunk = chunk.drop(columns=phantom_cols)

        if "MktFare" in chunk.columns:
            chunk = chunk[chunk["MktFare"] > 0]

        if "Passengers" in chunk.columns:
            chunk = chunk[chunk["Passengers"] > 0]

        # Strip whitespace from string columns
        str_cols = chunk.select_dtypes(include=["object"]).columns
        for col in str_cols:
            chunk[col] = chunk[col].str.strip()

        removed = initial_rows - len(chunk)
        if removed > 0:
            logger.debug(
                "Cleaned chunk: removed %d/%d rows (%.1f%%)",
                removed,
                initial_rows,
                100 * removed / initial_rows,
            )

        return chunk

    def verify_parquet(self, quarter: Quarter) -> dict:
        """Verify integrity of a converted Parquet file.

        Checks row count, schema, null counts, and basic statistics.

        Args:
            quarter: The quarter to verify.

        Returns:
            Dictionary with verification results.
        """
        parquet_path = (
            self.parquet_dir / f"db1b_market_{quarter.year}_{quarter.quarter}.parquet"
        )

        if not parquet_path.exists():
            return {"exists": False, "quarter": str(quarter)}

        metadata = pq.read_metadata(parquet_path)
        schema = pq.read_schema(parquet_path)

        # Read only fare column to compute quick statistics
        fare_table = pq.read_table(
            parquet_path, columns=["MktFare", "Passengers"]
        )
        fare_df = fare_table.to_pandas()

        return {
            "exists": True,
            "quarter": str(quarter),
            "num_rows": metadata.num_rows,
            "num_columns": schema.__len__(),
            "num_row_groups": metadata.num_row_groups,
            "size_mb": parquet_path.stat().st_size / (1024 * 1024),
            "columns": schema.names,
            "mkt_fare_mean": round(fare_df["MktFare"].mean(), 2),
            "mkt_fare_median": round(fare_df["MktFare"].median(), 2),
            "mkt_fare_min": round(fare_df["MktFare"].min(), 2),
            "mkt_fare_max": round(fare_df["MktFare"].max(), 2),
            "total_passengers": int(fare_df["Passengers"].sum()),
            "null_counts": {
                col: fare_table.column(col).null_count
                for col in fare_table.column_names
            },
        }


    # --- Batch Processing Methods --- 
    def load_quarter(
        self,
        quarter: Quarter,
        columns: list[str] | None = None,
        filters: list[tuple] | None = None,
    ) -> pd.DataFrame:
        """Load a single quarter of Parquet data with column selection.

        Args:
            quarter: The year-quarter to load.
            columns: Specific columns to load. If None, loads all columns
                    in DB1B_COLUMNS_OF_INTEREST.
            filters: PyArrow predicate pushdown filters. Each tuple is
                    (column, operator, value), e.g., ("MktFare", ">", 50).

        Returns:
            DataFrame with the requested data.

        Raises:
            FileNotFoundError: If Parquet file does not exist.

        Example:
            >>> handler = BTSDataHandler()
            >>> df = handler.load_quarter(
            ...     Quarter(2024, 1),
            ...     columns=["Origin", "Dest", "MktFare", "Passengers"],
            ...     filters=[("MktFare", ">", 50)],
            ... )
        """
        parquet_path = self._quarter_parquet_path(quarter)
        if not parquet_path.exists():
            raise FileNotFoundError(
                f"No Parquet file for {quarter}: {parquet_path}"
            )

        cols = columns or DB1B_COLUMNS_OF_INTEREST
        available = pq.read_schema(parquet_path).names
        cols = [c for c in cols if c in available]

        if not cols:
            raise ValueError("No valid columns requested")

        table = pq.read_table(parquet_path, columns=cols, filters=filters)
        df = table.to_pandas()

        # Convert low-cardinality string columns to category for memory savings
        str_cols = df.select_dtypes(include=["object"]).columns
        for col in str_cols:
            if df[col].nunique() < 1000:
                df[col] = df[col].astype("category")

        logger.info(
            "Loaded %s: %d rows x %d cols (%.1f MB)",
            quarter,
            len(df),
            len(df.columns),
            df.memory_usage(deep=True).sum() / (1024 * 1024),
        )
        return df

    def iter_quarters(
        self,
        start: Quarter,
        end: Quarter,
        columns: list[str] | None = None,
        filters: list[tuple] | None = None,
    ) -> Generator[tuple[Quarter, pd.DataFrame], None, None]:
        """Iterate over quarters one at a time, yielding DataFrames.

        Memory-efficient: only one quarter is in memory at a time.
        Silently skips quarters without Parquet files.

        Args:
            start: First quarter in range.
            end: Last quarter in range (inclusive).
            columns: Columns to load per quarter.
            filters: PyArrow predicate pushdown filters.

        Yields:
            Tuple of (Quarter, DataFrame) for each available quarter.

        Example:
            >>> for quarter, df in handler.iter_quarters(
            ...     Quarter(2024, 1), Quarter(2025, 2),
            ...     columns=["Origin", "Dest", "MktFare"],
            ... ):
            ...     print(f"{quarter}: {len(df)} rows, mean fare ${df['MktFare'].mean():.2f}")
        """
        quarters = self.enumerate_quarters(start, end)
        available = 0

        for quarter in quarters:
            parquet_path = self._quarter_parquet_path(quarter)
            if not parquet_path.exists():
                logger.debug("Skipping %s (no Parquet file)", quarter)
                continue

            df = self.load_quarter(quarter, columns=columns, filters=filters)
            available += 1
            yield quarter, df

        if available == 0:
            logger.warning(
                "No Parquet files found for range %s to %s", start, end
            )

    def batch_aggregate(
        self,
        start: Quarter,
        end: Quarter,
        group_cols: list[str],
        agg_dict: dict[str, str | list[str]],
        filters: list[tuple] | None = None,
    ) -> pd.DataFrame:
        """Aggregate across multiple quarters without loading all at once.

        Processes one quarter at a time, computes partial aggregates,
        then combines. Supports sum, count, and mean (via sum + count).

        Args:
            start: First quarter.
            end: Last quarter (inclusive).
            group_cols: Columns to group by.
            agg_dict: Aggregation spec, e.g., {"MktFare": ["mean", "count"]}.
            filters: PyArrow predicate pushdown filters.

        Returns:
            Combined aggregated DataFrame.

        Example:
            >>> route_stats = handler.batch_aggregate(
            ...     Quarter(2024, 1), Quarter(2025, 2),
            ...     group_cols=["Origin", "Dest"],
            ...     agg_dict={"MktFare": ["mean", "count", "std"],
            ...               "Passengers": "sum"},
            ... )
        """
        needed_cols = list(set(group_cols + list(agg_dict.keys())))
        partial_results = []

        for quarter, df in self.iter_quarters(
            start, end, columns=needed_cols, filters=filters
        ):
            partial = df.groupby(group_cols, observed=True).agg(agg_dict)
            partial.columns = [
                f"{col}_{func}" if isinstance(func, str) else f"{col}_{func}"
                for col, func in partial.columns
            ]
            partial["_quarter_count"] = 1
            partial_results.append(partial)

        if not partial_results:
            return pd.DataFrame()

        combined = pd.concat(partial_results).groupby(level=group_cols).sum()
        logger.info(
            "Batch aggregate: %d groups across %d quarters",
            len(combined),
            len(partial_results),
        )
        return combined

    def get_route_data(
        self,
        start: Quarter,
        end: Quarter,
        origins: list[str] | None = None,
        destinations: list[str] | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Extract data for specific routes across all available quarters.

        Filters at the PyArrow level for efficiency -- only matching rows
        are read from disk.

        Args:
            start: First quarter.
            end: Last quarter (inclusive).
            origins: Filter by origin airport codes (e.g., ["JFK", "LAX"]).
            destinations: Filter by destination codes (e.g., ["SFO", "MIA"]).
            columns: Columns to load.

        Returns:
            Combined DataFrame for matching routes.

        Example:
            >>> jfk_routes = handler.get_route_data(
            ...     Quarter(2024, 1), Quarter(2025, 2),
            ...     origins=["JFK", "EWR", "LGA"],
            ...     columns=["Origin", "Dest", "MktFare", "MktDistance",
            ...              "RPCarrier", "Passengers", "Year", "Quarter"],
            ... )
        """
        filters = []
        if origins:
            filters.append(("Origin", "in", origins))
        if destinations:
            filters.append(("Dest", "in", destinations))

        frames = []
        for quarter, df in self.iter_quarters(
            start, end, columns=columns, filters=filters or None
        ):
            frames.append(df)

        if not frames:
            logger.warning("No data found for the specified route filters")
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        logger.info(
            "Route data: %d rows across %d quarters | "
            "Origins: %s | Destinations: %s",
            len(result),
            len(frames),
            origins or "all",
            destinations or "all",
        )
        return result

    def create_sample(
        self,
        start: Quarter,
        end: Quarter,
        fraction: float = 0.1,
        random_state: int = 42,
    ) -> Path:
        """Create a persistent stratified sample for local development.

        Samples each quarter independently, preserving the temporal
        distribution. Saves to data/bts/parquet/db1b_sample.parquet.

        Args:
            start: First quarter.
            end: Last quarter (inclusive).
            fraction: Fraction of rows to sample (default 10%).
            random_state: Seed for reproducibility.

        Returns:
            Path to the sample Parquet file.

        Example:
            >>> sample_path = handler.create_sample(
            ...     Quarter(2024, 1), Quarter(2025, 2), fraction=0.1
            ... )
        """
        sample_path = self.parquet_dir / "db1b_sample.parquet"
        writer = None
        total_rows = 0

        try:
            for quarter, df in self.iter_quarters(start, end):
                sampled = df.sample(
                    frac=fraction, random_state=random_state
                )
                table = pa.Table.from_pandas(sampled, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(
                        sample_path,
                        schema=table.schema,
                        compression="snappy",
                    )

                writer.write_table(table)
                total_rows += len(sampled)
                logger.info(
                    "Sampled %s: %d -> %d rows",
                    quarter,
                    len(df),
                    len(sampled),
                )

        finally:
            if writer is not None:
                writer.close()

        size_mb = sample_path.stat().st_size / (1024 * 1024)
        logger.info(
            "Sample created: %d rows (%.1f%%) -> %s (%.1f MB)",
            total_rows,
            fraction * 100,
            sample_path,
            size_mb,
        )
        return sample_path

    def load_sample(
        self, columns: list[str] | None = None
    ) -> pd.DataFrame:
        """Load the previously created sample file.

        Args:
            columns: Specific columns to load.

        Returns:
            DataFrame with sample data.
        """
        sample_path = self.parquet_dir / "db1b_sample.parquet"
        if not sample_path.exists():
            raise FileNotFoundError(
                "No sample file found. Run create_sample() first."
            )

        cols = columns or DB1B_COLUMNS_OF_INTEREST
        available = pq.read_schema(sample_path).names
        cols = [c for c in cols if c in available]

        df = pq.read_table(sample_path, columns=cols).to_pandas()

        str_cols = df.select_dtypes(include=["object"]).columns
        for col in str_cols:
            if df[col].nunique() < 1000:
                df[col] = df[col].astype("category")

        logger.info(
            "Loaded sample: %d rows x %d cols (%.1f MB)",
            len(df),
            len(df.columns),
            df.memory_usage(deep=True).sum() / (1024 * 1024),
        )
        return df

    def memory_estimate(
        self,
        quarter: Quarter,
        columns: list[str] | None = None,
    ) -> dict:
        """Estimate memory usage before loading a quarter.

        Reads Parquet metadata without loading data.

        Args:
            quarter: The quarter to estimate.
            columns: Columns that would be loaded.

        Returns:
            Dictionary with size estimates.
        """
        parquet_path = self._quarter_parquet_path(quarter)
        if not parquet_path.exists():
            return {"exists": False, "quarter": str(quarter)}

        metadata = pq.read_metadata(parquet_path)
        schema = pq.read_schema(parquet_path)

        cols = columns or DB1B_COLUMNS_OF_INTEREST
        available = [c for c in cols if c in schema.names]

        disk_mb = parquet_path.stat().st_size / (1024 * 1024)
        # Rough estimate: Parquet in-memory is ~3-5x disk size
        estimated_memory_mb = disk_mb * 14.0
        # Adjust for column selection
        col_fraction = len(available) / len(schema.names) if schema.names else 1
        estimated_memory_mb *= col_fraction

        return {
            "quarter": str(quarter),
            "num_rows": metadata.num_rows,
            "total_columns": len(schema.names),
            "requested_columns": len(available),
            "disk_mb": round(disk_mb, 1),
            "estimated_memory_mb": round(estimated_memory_mb, 1),
            "fits_in_ram": estimated_memory_mb < 20_000,  # 20 GB threshold
        }

    def _quarter_parquet_path(self, quarter: Quarter) -> Path:
        """Consistent path for a quarter's Parquet file."""
        return (
            self.parquet_dir
            / f"db1b_market_{quarter.year}_{quarter.quarter}.parquet"
        )
        
    # --- Auto-Detection of Existing Files ---
    def detect_pipeline_status(
        self,
        start: Quarter,
        end: Quarter,
    ) -> list[QuarterStatus]:
        """Detect the pipeline state of every quarter in a range.

        Scans all four directories, validates Parquet integrity,
        and reports what work remains for each quarter.

        Args:
            start: First quarter to check.
            end: Last quarter to check (inclusive).

        Returns:
            List of QuarterStatus objects.

        Example:
            >>> handler = BTSDataHandler()
            >>> status = handler.detect_pipeline_status(
            ...     Quarter(2024, 1), Quarter(2025, 2)
            ... )
            >>> handler.print_status(status)
        """
        quarters = self.enumerate_quarters(start, end)
        statuses = []

        for quarter in quarters:
            qs = QuarterStatus(quarter=quarter)

            # Check raw ZIP
            zip_path = self.raw_dir / quarter.filename
            if zip_path.exists():
                qs.has_zip = True
                qs.zip_size_mb = zip_path.stat().st_size / (1024 * 1024)

            # Check CSV
            csv_path = self._find_existing_csv(quarter)
            if csv_path is not None:
                qs.has_csv = True
                qs.csv_size_mb = csv_path.stat().st_size / (1024 * 1024)

            # Check Parquet
            parquet_path = self._quarter_parquet_path(quarter)
            if parquet_path.exists():
                qs.has_parquet = True
                qs.parquet_size_mb = (
                    parquet_path.stat().st_size / (1024 * 1024)
                )
                qs.parquet_valid = self._validate_parquet(parquet_path)
                if qs.parquet_valid:
                    qs.parquet_rows = pq.read_metadata(parquet_path).num_rows

            # Check processed
            processed_path = (
                self.processed_dir
                / f"db1b_features_{quarter.year}_{quarter.quarter}.parquet"
            )
            if processed_path.exists():
                qs.has_processed = True

            statuses.append(qs)

        return statuses

    def _validate_parquet(self, path: Path) -> bool:
        """Check if a Parquet file is readable and non-empty.

        Reads only metadata -- does not load data into memory.

        Args:
            path: Path to the Parquet file.

        Returns:
            True if file is valid and contains rows.
        """
        try:
            metadata = pq.read_metadata(path)
            if metadata.num_rows == 0:
                return False

            schema = pq.read_schema(path)
            if "MktFare" not in schema.names:
                logger.warning("Missing MktFare column in %s", path)
                return False

            return True
        except Exception as exc:
            logger.warning("Invalid Parquet file %s: %s", path, exc)
            return False

    def print_status(self, statuses: list[QuarterStatus]) -> None:
        """Print a formatted pipeline status table.

        Args:
            statuses: List from detect_pipeline_status().
        """
        print(
            f"\n{'Quarter':<12} {'ZIP':>6} {'CSV':>9} "
            f"{'Parquet':>10} {'Valid':>7} {'Rows':>12} {'Stage':<12} {'Next Action'}"
        )
        print("-" * 90)

        for qs in statuses:
            zip_str = f"{qs.zip_size_mb:.0f}MB" if qs.has_zip else "--"
            csv_str = f"{qs.csv_size_mb:.0f}MB" if qs.has_csv else "--"
            pq_str = f"{qs.parquet_size_mb:.0f}MB" if qs.has_parquet else "--"
            valid_str = "yes" if qs.parquet_valid else ("FAIL" if qs.has_parquet else "--")
            rows_str = f"{qs.parquet_rows:,}" if qs.parquet_rows > 0 else "--"

            print(
                f"{str(qs.quarter):<12} {zip_str:>6} {csv_str:>9} "
                f"{pq_str:>10} {valid_str:>7} {rows_str:>12} "
                f"{qs.stage:<12} {qs.next_action}"
            )

        # Summary
        total = len(statuses)
        complete_parquet = sum(
            1 for s in statuses if s.has_parquet and s.parquet_valid
        )
        needs_download = sum(1 for s in statuses if s.stage == "not_started")
        needs_convert = sum(1 for s in statuses if s.next_action == "convert_to_parquet")
        total_rows = sum(s.parquet_rows for s in statuses)

        print(f"\nSummary: {complete_parquet}/{total} quarters ready")
        if needs_download > 0:
            print(f"  Needs download: {needs_download}")
        if needs_convert > 0:
            print(f"  Needs conversion: {needs_convert}")
        print(f"  Total rows in Parquet: {total_rows:,}")

    # --- Full Pipeline Runner ---
    def run_pipeline(
        self,
        start: Quarter,
        end: Quarter,
        delete_csv: bool = False,
        delete_zip: bool = True,
    ) -> list[QuarterStatus]:
        """Run the full pipeline with progress tracking and summary report."""
        report = PipelineReport(start_time=time.time())
        statuses = self.detect_pipeline_status(start, end)

        logger.info("Pipeline status before run:")
        self.print_status(statuses)

        actionable = [
            qs for qs in statuses
            if qs.next_action not in ("complete", "process_features")
        ]
        report.quarters_skipped = len(statuses) - len(actionable)

        for qs in tqdm(
            actionable, desc="Pipeline progress", unit="quarter"
        ):
            quarter = qs.quarter

            # Step 1: Download if needed
            if qs.stage == "not_started":
                result = self.download_quarter(quarter, skip_existing=False)
                if not result.success:
                    report.quarters_failed += 1
                    report.errors.append(f"{quarter}: download -- {result.error}")
                    continue
                report.total_downloaded_mb += result.size_mb

            # Step 2: Extract ZIP if needed
            if qs.next_action == "extract_csv" or (
                qs.has_zip and not qs.has_csv
            ):
                zip_path = self.raw_dir / quarter.filename
                if zip_path.exists():
                    try:
                        self._extract_zip(zip_path)
                    except (zipfile.BadZipFile, FileNotFoundError) as exc:
                        report.quarters_failed += 1
                        report.errors.append(f"{quarter}: extract -- {exc}")
                        continue

            # Step 3: Convert to Parquet
            if qs.next_action in (
                "download", "extract_csv", "convert_to_parquet", "reconvert"
            ):
                result = self.convert_quarter_to_parquet(
                    quarter, skip_existing=False, delete_csv=delete_csv
                )
                if not result.success:
                    report.quarters_failed += 1
                    report.errors.append(f"{quarter}: convert -- {result.error}")
                    continue
                report.total_converted_rows += result.row_count
                report.total_parquet_mb += result.parquet_size_mb

            # Step 4: Cleanup ZIP
            if delete_zip:
                zip_path = self.raw_dir / quarter.filename
                if zip_path.exists():
                    zip_path.unlink()

            report.quarters_processed += 1

        report.end_time = time.time()

        final = self.detect_pipeline_status(start, end)
        logger.info("Pipeline status after run:")
        self.print_status(final)
        report.print_report()

        return final
