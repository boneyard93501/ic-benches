#!/usr/bin/env python3
"""Deterministic dataset generator for S3 benchmarking."""

import hashlib
import json
import os
import random
import sys
from pathlib import Path
from typing import Dict, List, Any

import click
import tomli
import structlog

logger = structlog.get_logger()


def calculate_sha256(filepath: Path, chunk_size: int = 8192) -> str:
    """Calculate SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(chunk_size):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def generate_file_sizes(
    total_size_gb: float,
    file_count: int,
    min_size_mb: int,
    max_size_mb: int,
    distribution: str,
    seed: int,
) -> List[int]:
    """Generate list of file sizes based on distribution."""
    random.seed(seed)
    total_bytes = int(total_size_gb * 1024 * 1024 * 1024)
    min_bytes = min_size_mb * 1024 * 1024
    max_bytes = max_size_mb * 1024 * 1024
    
    sizes = []
    
    if distribution == "fixed":
        # All files same size
        size_per_file = total_bytes // file_count
        size_per_file = max(min_bytes, min(max_bytes, size_per_file))
        sizes = [size_per_file] * file_count
        
    elif distribution == "random":
        # Random sizes within bounds
        remaining = total_bytes
        for i in range(file_count - 1):
            avg_remaining = remaining // (file_count - i)
            # Constrain to prevent exhausting budget
            max_allowed = min(max_bytes, remaining - (file_count - i - 1) * min_bytes)
            min_allowed = max(min_bytes, remaining - (file_count - i - 1) * max_bytes)
            min_allowed = min(min_allowed, max_allowed)
            
            size = random.randint(min_allowed, max_allowed)
            sizes.append(size)
            remaining -= size
        # Last file gets remainder
        sizes.append(max(min_bytes, min(max_bytes, remaining)))
        
    elif distribution == "mixed":
        # Mix of small, medium, large files
        small = min_bytes
        large = max_bytes
        medium = (small + large) // 2
        
        # 60% small, 30% medium, 10% large
        n_small = int(file_count * 0.6)
        n_medium = int(file_count * 0.3)
        n_large = file_count - n_small - n_medium
        
        sizes = (
            [small] * n_small +
            [medium] * n_medium +
            [large] * n_large
        )
        random.shuffle(sizes)
        
        # Adjust to meet total size target
        current_total = sum(sizes)
        if current_total < total_bytes:
            # Scale up proportionally
            factor = total_bytes / current_total
            sizes = [int(s * factor) for s in sizes]
        elif current_total > total_bytes:
            # Scale down proportionally
            factor = total_bytes / current_total
            sizes = [max(min_bytes, int(s * factor)) for s in sizes]
    
    else:
        raise ValueError(f"Unknown distribution: {distribution}")
    
    return sizes


def generate_file(filepath: Path, size: int, seed: int, file_index: int) -> str:
    """Generate a file with deterministic random content."""
    # Use seed + file_index for unique but deterministic content per file
    rng = random.Random(seed + file_index)
    
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    chunk_size = 1024 * 1024  # 1MB chunks
    bytes_written = 0
    
    with open(filepath, "wb") as f:
        while bytes_written < size:
            remaining = size - bytes_written
            chunk_bytes = min(chunk_size, remaining)
            # Generate deterministic random bytes
            chunk = bytes(rng.randint(0, 255) for _ in range(chunk_bytes))
            f.write(chunk)
            bytes_written += chunk_bytes
    
    return calculate_sha256(filepath)


def verify_manifest(manifest_path: Path, data_path: Path) -> bool:
    """Verify existing dataset against manifest."""
    try:
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
        
        logger.info(
            "Found existing manifest",
            seed=manifest["seed"],
            file_count=len(manifest["files"]),
        )
        
        # Check all files exist with correct checksums
        for file_info in manifest["files"]:
            filepath = data_path / file_info["path"]
            if not filepath.exists():
                logger.warning("Missing file", path=file_info["path"])
                return False
            
            actual_size = filepath.stat().st_size
            if actual_size != file_info["size"]:
                logger.warning(
                    "Size mismatch",
                    path=file_info["path"],
                    expected=file_info["size"],
                    actual=actual_size,
                )
                return False
            
            actual_checksum = calculate_sha256(filepath)
            if actual_checksum != file_info["checksum"]:
                logger.warning(
                    "Checksum mismatch",
                    path=file_info["path"],
                    expected=file_info["checksum"],
                    actual=actual_checksum,
                )
                return False
        
        logger.info("Dataset verification successful")
        return True
        
    except Exception as e:
        logger.error("Failed to verify manifest", error=str(e))
        return False


@click.command()
@click.option(
    "--config",
    default="config.toml",
    help="Configuration file",
    type=click.Path(exists=True),
)
@click.option(
    "--force",
    is_flag=True,
    help="Force regeneration even if dataset exists",
)
@click.option(
    "--data-path",
    default=None,
    help="Override data path from config",
    type=click.Path(),
)
def generate(config: str, force: bool, data_path: str):
    """Generate deterministic test dataset for S3 benchmarking."""
    # Load configuration
    with open(config, "rb") as f:
        cfg = tomli.load(f)
    
    dataset_cfg = cfg["dataset"]
    seed = dataset_cfg["seed"]
    total_size_gb = dataset_cfg["total_size_gb"]
    file_count = dataset_cfg["file_count"]
    min_size_mb = dataset_cfg["min_file_size_mb"]
    max_size_mb = dataset_cfg["max_file_size_mb"]
    distribution = dataset_cfg["size_distribution"]
    directory_depth = dataset_cfg["directory_depth"]
    files_per_directory = dataset_cfg["files_per_directory"]
    
    # Use override path or config path
    if data_path:
        data_path = Path(data_path)
    else:
        data_path = Path(dataset_cfg["data_path"])
    
    # Check if we have permissions for the data path
    if data_path.parts[0] == '/' and not os.access(data_path.parent, os.W_OK):
        # If it's an absolute path we can't write to, use local directory
        logger.warning(
            f"Cannot write to {data_path}, using local directory",
            original_path=str(data_path),
        )
        data_path = Path("./data/s3-bench")
    
    manifest_path = data_path / "manifest.json"
    
    # Check for existing dataset
    if manifest_path.exists() and not force:
        with open(manifest_path, "r") as f:
            existing_manifest = json.load(f)
        
        if existing_manifest["seed"] == seed:
            logger.info(
                "Dataset with same seed exists, verifying integrity",
                seed=seed,
            )
            if verify_manifest(manifest_path, data_path):
                logger.info("Using existing dataset")
                return
            else:
                logger.warning("Verification failed, regenerating dataset")
        else:
            logger.info(
                "Seed changed, regenerating dataset",
                old_seed=existing_manifest["seed"],
                new_seed=seed,
            )
    
    # Generate file sizes
    logger.info(
        "Generating dataset",
        seed=seed,
        total_size_gb=total_size_gb,
        file_count=file_count,
        distribution=distribution,
        data_path=str(data_path),
    )
    
    file_sizes = generate_file_sizes(
        total_size_gb,
        file_count,
        min_size_mb,
        max_size_mb,
        distribution,
        seed,
    )
    
    # Create directory structure and generate files
    data_path.mkdir(parents=True, exist_ok=True)
    
    random.seed(seed)  # Reset seed for directory structure
    
    manifest = {
        "seed": seed,
        "total_size_gb": total_size_gb,
        "file_count": file_count,
        "distribution": distribution,
        "files": [],
    }
    
    for i, size in enumerate(file_sizes):
        # Distribute files across directories
        dir_index = i // files_per_directory
        dir_path = data_path
        
        # Create nested directory structure
        for depth in range(min(directory_depth, dir_index + 1)):
            dir_name = f"dir_{seed}_{dir_index}_{depth}"
            dir_path = dir_path / dir_name
        
        filename = f"file_{seed}_{i:06d}.bin"
        filepath = dir_path / filename
        relative_path = filepath.relative_to(data_path)
        
        logger.info(
            "Generating file",
            index=i + 1,
            total=file_count,
            size_mb=size / (1024 * 1024),
            path=str(relative_path),
        )
        
        checksum = generate_file(filepath, size, seed, i)
        
        manifest["files"].append({
            "path": str(relative_path),
            "size": size,
            "checksum": checksum,
        })
    
    # Write manifest
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    
    # Calculate total size
    total_size = sum(f["size"] for f in manifest["files"])
    logger.info(
        "Dataset generation complete",
        total_files=len(manifest["files"]),
        total_size_gb=total_size / (1024 ** 3),
        manifest_path=str(manifest_path),
    )


if __name__ == "__main__":
    generate()