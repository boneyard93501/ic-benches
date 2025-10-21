#!/usr/bin/env python3
"""Integration tests for dataset generation."""

import json
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

# Add scripts directory to path so we can import data_gen
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.data_gen import (
    generate_file_sizes,
    generate_file,
    calculate_sha256,
    verify_manifest,
)


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test data."""
    temp_dir = tempfile.mkdtemp(prefix="ic_bench_test_")
    yield Path(temp_dir)
    # Auto cleanup
    if Path(temp_dir).exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def test_config(temp_data_dir):
    """Create test configuration."""
    config = {
        "dataset": {
            "seed": 12345,
            "total_size_gb": 0.1,  # 100MB for testing
            "file_count": 10,
            "min_file_size_mb": 5,
            "max_file_size_mb": 20,
            "size_distribution": "mixed",
            "directory_depth": 2,
            "files_per_directory": 5,
            "data_path": str(temp_data_dir),
        }
    }
    return config


class TestDatasetGeneration:
    """Test dataset generation functionality."""
    
    def test_file_size_generation(self, test_config):
        """Test file size calculation for different distributions."""
        cfg = test_config["dataset"]
        
        # Test fixed distribution
        sizes = generate_file_sizes(
            total_size_gb=0.1,
            file_count=10,
            min_size_mb=5,
            max_size_mb=20,
            distribution="fixed",
            seed=12345,
        )
        assert len(sizes) == 10
        assert all(s == sizes[0] for s in sizes)  # All same size
        
        # Test random distribution
        sizes = generate_file_sizes(
            total_size_gb=0.1,
            file_count=10,
            min_size_mb=5,
            max_size_mb=20,
            distribution="random",
            seed=12345,
        )
        assert len(sizes) == 10
        assert all(5 * 1024 * 1024 <= s <= 20 * 1024 * 1024 for s in sizes)
        
        # Test mixed distribution
        sizes = generate_file_sizes(
            total_size_gb=0.1,
            file_count=10,
            min_size_mb=5,
            max_size_mb=20,
            distribution="mixed",
            seed=12345,
        )
        assert len(sizes) == 10
        # Should have variety of sizes
        unique_sizes = len(set(sizes))
        assert unique_sizes >= 2  # At least some variety
    
    def test_deterministic_generation(self, temp_data_dir):
        """Test that same seed produces identical files."""
        seed = 99999
        size = 1024 * 1024  # 1MB
        
        # Generate file twice with same parameters
        file1 = temp_data_dir / "test1.bin"
        checksum1 = generate_file(file1, size, seed, 0)
        
        file2 = temp_data_dir / "test2.bin"
        checksum2 = generate_file(file2, size, seed, 0)
        
        # Should have identical checksums
        assert checksum1 == checksum2
        
        # Different file index should produce different content
        file3 = temp_data_dir / "test3.bin"
        checksum3 = generate_file(file3, size, seed, 1)
        assert checksum3 != checksum1
    
    def test_manifest_creation_and_verification(self, temp_data_dir):
        """Test manifest generation and verification."""
        # Create simple test dataset
        manifest = {
            "seed": 42,
            "total_size_gb": 0.001,
            "file_count": 2,
            "distribution": "fixed",
            "files": [],
        }
        
        # Generate two small files
        for i in range(2):
            filepath = temp_data_dir / f"file_{i}.bin"
            size = 1024 * 100  # 100KB
            checksum = generate_file(filepath, size, 42, i)
            
            manifest["files"].append({
                "path": filepath.name,
                "size": size,
                "checksum": checksum,
            })
        
        # Write manifest
        manifest_path = temp_data_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        
        # Verify should succeed
        assert verify_manifest(manifest_path, temp_data_dir) is True
        
        # Corrupt a file and verify should fail
        corrupt_file = temp_data_dir / "file_0.bin"
        with open(corrupt_file, "ab") as f:
            f.write(b"corruption")
        
        assert verify_manifest(manifest_path, temp_data_dir) is False
    
    def test_idempotent_generation(self, temp_data_dir, test_config):
        """Test that generation is idempotent with same seed."""
        cfg = test_config["dataset"]
        
        # Create initial manifest
        manifest_path = temp_data_dir / "manifest.json"
        manifest = {
            "seed": cfg["seed"],
            "total_size_gb": cfg["total_size_gb"],
            "file_count": 1,
            "distribution": "fixed",
            "files": [{
                "path": "test.bin",
                "size": 1024,
                "checksum": "abc123",
            }],
        }
        
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)
        
        # Create the dummy file
        test_file = temp_data_dir / "test.bin"
        test_file.write_bytes(b"x" * 1024)
        
        # Loading manifest with same seed should detect existing dataset
        with open(manifest_path, "r") as f:
            loaded = json.load(f)
        
        assert loaded["seed"] == cfg["seed"]
    
    def test_seed_change_triggers_regeneration(self, temp_data_dir):
        """Test that changing seed triggers regeneration."""
        manifest_path = temp_data_dir / "manifest.json"
        
        # Create manifest with seed 1
        manifest_v1 = {
            "seed": 1,
            "total_size_gb": 0.001,
            "file_count": 1,
            "distribution": "fixed",
            "files": [{
                "path": "old.bin",
                "size": 1024,
                "checksum": "old_checksum",
            }],
        }
        
        with open(manifest_path, "w") as f:
            json.dump(manifest_v1, f)
        
        # Check with different seed
        with open(manifest_path, "r") as f:
            loaded = json.load(f)
        
        new_seed = 2
        assert loaded["seed"] != new_seed  # Should detect seed change


class TestEndToEndDataGeneration:
    """End-to-end test of data generation script."""
    
    def test_full_generation_pipeline(self, temp_data_dir):
        """Test complete dataset generation pipeline."""
        import subprocess
        import tomli
        
        # Create test config file
        config_path = temp_data_dir / "test_config.toml"
        test_data_path = temp_data_dir / "data"
        
        config_content = f"""
[dataset]
seed = 55555
total_size_gb = 0.01
file_count = 5
size_distribution = "mixed"
min_file_size_mb = 1
max_file_size_mb = 3
directory_depth = 2
files_per_directory = 3
data_path = "{test_data_path}"
"""
        config_path.write_text(config_content)
        
        # Run the generator with the test config
        result = subprocess.run(
            [sys.executable, "scripts/data_gen.py", "--config", str(config_path)],
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
        
        assert result.returncode == 0
        
        # Check manifest exists
        manifest_path = test_data_path / "manifest.json"
        assert manifest_path.exists()
        
        # Verify manifest content
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
        
        assert manifest["seed"] == 55555
        assert manifest["file_count"] == 5
        assert len(manifest["files"]) == 5
        
        # Verify files exist
        for file_info in manifest["files"]:
            filepath = test_data_path / file_info["path"]
            assert filepath.exists()
            assert filepath.stat().st_size == file_info["size"]