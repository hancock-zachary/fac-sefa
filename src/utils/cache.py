# src/utils/cache.py
"""Cache management utilities and configuration"""
#%%
#
from datetime import datetime
import json
import os
from pathlib import Path
import sqlite3
from typing import Dict, List, Optional, Set, Union


#%%
#
class CacheConfig:
    """Centralized cache configuration and path management"""
    def __init__(self, project_root: str | None = None) -> None:
        # Auto-detect project root if not provided
        if project_root is None:  # Assumes this file is in src/utils/
            current_file = Path(__file__).resolve()
            self.project_root = current_file.parent.parent.parent
        else:
            self.project_root = Path(project_root)
        
        # Define directory structure
        self.data_dir = self.project_root / "data"
        self.logs_dir = self.project_root / "logs"
        self.cache_dir = self.data_dir / "cache"
        self.processed_dir = self.data_dir / "processed"
        self.raw_data_dir = self.data_dir / "raw"
        
        # Create directories if they don't exist
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """
        Purpose:
            Create necessary directories if they don't exist
        """
        for directory in [self.data_dir, self.cache_dir, self.raw_data_dir, self.processed_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_cache_path(self, cache_name: str, cache_type: str = "json") -> Path:
        """
        Purpose:
            Get standardized cache file path
        Args:
            cache_name: Name of the cache (e.g., 'fac_report_ids', 'census_data')
            cache_type: Type of cache file ('json', 'db', 'txt')
        Returns:
            Path object for the cache file
        """
        extension_map = {  # Rename conventional extensions for clarity.
            "json": ".json"
            , "db": ".db"
            , "sqlite": ".db"
            , "text": ".txt"
            , "txt": ".txt"
        }
        extension = extension_map.get(cache_type.lower(), f".{cache_type}")
        filename = f"{cache_name}_cache{extension}"
        return self.cache_dir / filename
    
    def get_processed_data_path(self, filename: str) -> Path:
        """
        Purpose:
            Get path for processed data files
        Returns:
            Path object for the processed data file
        """
        return self.processed_dir / filename
    
    def get_raw_data_path(self, filename: str) -> Path:
        """
        Purpose:
            Get path for raw data files
        Returns:
            Path object for the raw data file
        """
        return self.raw_data_dir / filename
    
    def get_log_path(self, log_name: str) -> Path:
        """
        Purpose:
            Get path for log files
        Returns:
            Path object for the log file
        """
        return self.logs_dir / f"{log_name}.log"
    
    def list_cache_files(self) -> list:
        """
        Purpose:
            List all cache files in the cache directory
        Returns:
            List of cache file names
        """
        if not self.cache_dir.exists():
            return []
        return [f.name for f in self.cache_dir.iterdir() if f.is_file()]
    
    def get_cache_info(self) -> dict:
        """
        Purpose:
            Get information about all cache files
        Returns:
            Dictionary with cache file names as keys and their size and modification time as values
        """
        cache_info = {}
        if self.cache_dir.exists():
            for cache_file in self.cache_dir.iterdir():
                if cache_file.is_file():
                    stat = cache_file.stat()
                    cache_info[cache_file.name] = {
                        "size_mb": round(stat.st_size / 1024 / 1024, 2),
                        "modified": stat.st_mtime,
                        "path": str(cache_file)
                    }
        
        return cache_info
    
    def clear_cache(self, cache_pattern: Optional[str] = None):
        """
        Clear cache files
        
        Args:
            cache_pattern: Pattern to match (e.g., 'fac_*'). If None, prompts for confirmation.
        """
        import glob
        
        if cache_pattern:
            pattern_path = self.cache_dir / cache_pattern
            files_to_delete = glob.glob(str(pattern_path))
        else:
            files_to_delete = [str(f) for f in self.cache_dir.iterdir() if f.is_file()]
        
        if not files_to_delete:
            print("No cache files found to delete.")
            return
        
        print(f"Found {len(files_to_delete)} cache files:")
        for file_path in files_to_delete:
            print(f"  - {Path(file_path).name}")
        
        if cache_pattern is None:
            confirm = input("Delete all these files? (y/N): ")
            if confirm.lower() != 'y':
                print("Cache clearing cancelled.")
                return
        
        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                print(f"Deleted: {Path(file_path).name}")
            except Exception as e:
                print(f"Error deleting {Path(file_path).name}: {e}")


#%%
# Global cache configuration instance
cache_config = CacheConfig()


#%%
# Convenience functions for easy importing
def get_cache_path(cache_name: str, cache_type: str = "json") -> Path:
    """Convenience function to get cache path"""
    return cache_config.get_cache_path(cache_name, cache_type)

def get_raw_data_path(filename: str) -> Path:
    """Convenience function to get raw data path"""
    return cache_config.get_raw_data_path(filename)

def get_processed_data_path(filename: str) -> Path:
    """Convenience function to get processed data path"""
    return cache_config.get_processed_data_path(filename)