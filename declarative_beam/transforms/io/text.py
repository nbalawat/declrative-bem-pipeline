"""
Text I/O transforms for the declarative beam pipeline framework.

This module provides transforms for reading from and writing to text files.
"""

from typing import Any, Dict, List, Optional, Union

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("ReadFromText")
class ReadFromTextTransform(BaseTransform):
    """
    Read from a text file or files.

    Parameters:
        file_pattern: The file pattern to read from.
        min_bundle_size: Minimum size in bytes for a file to be read in a single bundle.
        compression_type: The compression type of the file(s).
        strip_trailing_newlines: Whether to strip trailing newlines.
        coder: The coder to use for decoding the file(s).
        validate: Whether to validate the file(s).
        skip_header_lines: Number of header lines to skip.
        with_filename: Whether to include the filename in the output.
    """

    PARAMETERS = {
        "file_pattern": {
            "type": "string",
            "description": "The file pattern to read from",
            "required": True,
        },
        "min_bundle_size": {
            "type": "integer",
            "description": "Minimum size in bytes for a file to be read in a single bundle",
            "required": False,
        },
        "compression_type": {
            "type": "string",
            "description": "The compression type of the file(s)",
            "required": False,
        },
        "strip_trailing_newlines": {
            "type": "boolean",
            "description": "Whether to strip trailing newlines",
            "required": False,
        },
        "coder": {
            "type": "string",
            "description": "The coder to use for decoding the file(s)",
            "required": False,
        },
        "validate": {
            "type": "boolean",
            "description": "Whether to validate the file(s)",
            "required": False,
        },
        "skip_header_lines": {
            "type": "integer",
            "description": "Number of header lines to skip",
            "required": False,
        },
    }

    def build_transform(
        self, side_inputs: Optional[Dict[str, Any]] = None
    ) -> beam.PTransform:
        """Build the Apache Beam PTransform for this transform.

        Args:
            side_inputs: A dictionary of side inputs (not used for this transform).

        Returns:
            An Apache Beam PTransform.
        """
        file_pattern = self.config.get("file_pattern")
        min_bundle_size = self.config.get("min_bundle_size")
        
        # Handle compression_type properly
        compression_type_str = self.config.get("compression_type")
        if compression_type_str:
            from apache_beam.io.filesystem import CompressionTypes
            compression_type_map = {
                "auto": CompressionTypes.AUTO,
                "uncompressed": CompressionTypes.UNCOMPRESSED,
                "gzip": CompressionTypes.GZIP,
                "bzip2": CompressionTypes.BZIP2,
                "deflate": CompressionTypes.DEFLATE,
            }
            compression_type = compression_type_map.get(compression_type_str.lower(), CompressionTypes.AUTO)
        else:
            compression_type = None
            
        strip_trailing_newlines = self.config.get("strip_trailing_newlines", True)
        validate = self.config.get("validate", True)
        skip_header_lines = self.config.get("skip_header_lines", 0)

        # Create kwargs dict to handle optional parameters
        kwargs = {
            "file_pattern": file_pattern,
            "strip_trailing_newlines": strip_trailing_newlines,
            "validate": validate,
            "skip_header_lines": skip_header_lines
        }
        
        # Only add optional parameters if they're not None
        if min_bundle_size is not None:
            kwargs["min_bundle_size"] = min_bundle_size
        if compression_type is not None:
            kwargs["compression_type"] = compression_type
            
        return ReadFromText(**kwargs)


@TransformRegistry.register("WriteToText")
class WriteToTextTransform(BaseTransform):
    """
    Write to a text file or files.

    Parameters:
        file_path_prefix: The prefix of the file(s) to write to.
        file_name_suffix: The suffix of the file(s) to write to.
        append_trailing_newlines: Whether to append trailing newlines.
        num_shards: The number of shards to use when writing.
        shard_name_template: The template for shard names.
        compression_type: The compression type to use when writing.
        header: The header to write to the file(s).
        footer: The footer to write to the file(s).
        coder: The coder to use for encoding the file(s).
    """

    PARAMETERS = {
        "file_path_prefix": {
            "type": "string",
            "description": "The prefix of the file(s) to write to",
            "required": True,
        },
        "file_name_suffix": {
            "type": "string",
            "description": "The suffix of the file(s) to write to",
            "required": False,
        },
        "append_trailing_newlines": {
            "type": "boolean",
            "description": "Whether to append trailing newlines",
            "required": False,
        },
        "num_shards": {
            "type": "integer",
            "description": "The number of shards to use when writing",
            "required": False,
        },
        "shard_name_template": {
            "type": "string",
            "description": "The template for shard names",
            "required": False,
        },
        "compression_type": {
            "type": "string",
            "description": "The compression type to use when writing",
            "required": False,
        },
        "header": {
            "type": "string",
            "description": "The header to write to the file(s)",
            "required": False,
        },
        "footer": {
            "type": "string",
            "description": "The footer to write to the file(s)",
            "required": False,
        },
        "coder": {
            "type": "string",
            "description": "The coder to use for encoding the file(s)",
            "required": False,
        },
    }

    def build_transform(
        self, side_inputs: Optional[Dict[str, Any]] = None
    ) -> beam.PTransform:
        """Build the Apache Beam PTransform for this transform.

        Args:
            side_inputs: A dictionary of side inputs (not used for this transform).

        Returns:
            An Apache Beam PTransform.
        """
        file_path_prefix = self.config.get("file_path_prefix")
        file_name_suffix = self.config.get("file_name_suffix")
        append_trailing_newlines = self.config.get("append_trailing_newlines", True)
        num_shards = self.config.get("num_shards")
        shard_name_template = self.config.get("shard_name_template")
        
        # Handle compression_type properly
        compression_type_str = self.config.get("compression_type")
        if compression_type_str:
            from apache_beam.io.filesystem import CompressionTypes
            compression_type_map = {
                "auto": CompressionTypes.AUTO,
                "uncompressed": CompressionTypes.UNCOMPRESSED,
                "gzip": CompressionTypes.GZIP,
                "bzip2": CompressionTypes.BZIP2,
                "deflate": CompressionTypes.DEFLATE,
            }
            compression_type = compression_type_map.get(compression_type_str.lower(), CompressionTypes.AUTO)
        else:
            compression_type = None
            
        header = self.config.get("header")
        footer = self.config.get("footer")

        # Create kwargs dict to handle optional parameters
        kwargs = {
            "file_path_prefix": file_path_prefix,
            "append_trailing_newlines": append_trailing_newlines,
        }
        
        # Only add optional parameters if they're not None
        if file_name_suffix is not None:
            kwargs["file_name_suffix"] = file_name_suffix
        if num_shards is not None:
            kwargs["num_shards"] = num_shards
        if shard_name_template is not None:
            kwargs["shard_name_template"] = shard_name_template
        if compression_type is not None:
            kwargs["compression_type"] = compression_type
        if header is not None:
            kwargs["header"] = header
        if footer is not None:
            kwargs["footer"] = footer
            
        return WriteToText(**kwargs)
