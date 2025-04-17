"""
XML I/O transforms for the declarative beam pipeline framework.

This module provides transforms for reading and parsing XML files.
"""

from typing import Any, Dict, Optional

import apache_beam as beam
import xml.etree.ElementTree as ET
import logging

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry

logger = logging.getLogger(__name__)


@TransformRegistry.register("ReadFromXml")
class ReadFromXmlTransform(BaseTransform):
    """
    Read from an XML file and parse it into dictionaries.

    Parameters:
        file_pattern: The file pattern to read from.
        record_element_tag: The XML tag name of the elements representing individual records.
        include_attributes: Whether to include element attributes in the output dictionary.
        attribute_prefix: Prefix for attribute keys in the output dictionary.
    """

    PARAMETERS = {
        "file_pattern": {
            "type": "string",
            "description": "The file pattern to read from",
            "required": True,
        },
        "record_element_tag": {
            "type": "string",
            "description": "The XML tag name of the elements representing individual records",
            "required": True,
        },
        "include_attributes": {
            "type": "boolean",
            "description": "Include element attributes in the output dictionary",
            "required": False,
        },
        "attribute_prefix": {
            "type": "string",
            "description": "Prefix for attribute keys in the output dictionary",
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
        record_element_tag = self.config.get("record_element_tag")
        include_attributes = self.config.get("include_attributes", False)
        attribute_prefix = self.config.get("attribute_prefix", "@")

        # Create a composite transform
        class ReadFromXmlComposite(beam.PTransform):
            def expand(self, pcoll):
                return (
                    pcoll
                    | beam.Create([file_pattern])  # Start with the file pattern
                    | beam.ParDo(_ReadXmlFileDoFn())  # Read the XML file
                    | beam.ParDo(
                        _ParseXmlFileDoFn(
                            record_element_tag=record_element_tag,
                            include_attributes=include_attributes,
                            attribute_prefix=attribute_prefix,
                        )
                    )
                )

        return ReadFromXmlComposite()


class _ReadXmlFileDoFn(beam.DoFn):
    """A DoFn to read an XML file and output its contents."""
    
    def process(self, file_pattern: str):
        """
        Read an XML file and output its contents as a string.
        
        Args:
            file_pattern: The file pattern to read from.
        """
        import glob
        import os
        
        # Handle relative paths by checking if the pattern is absolute
        if not os.path.isabs(file_pattern):
            # Try to resolve relative to the current working directory
            cwd = os.getcwd()
            abs_pattern = os.path.join(cwd, file_pattern)
            logger.info(f"Converting relative path '{file_pattern}' to absolute path '{abs_pattern}'")
            file_pattern = abs_pattern
        
        # Find all files matching the pattern
        matching_files = glob.glob(file_pattern)
        
        if not matching_files:
            logger.error(f"No files found matching pattern: {file_pattern}")
            return
            
        logger.info(f"Found {len(matching_files)} files matching pattern: {file_pattern}")
        
        # Process each file
        for file_path in matching_files:
            try:
                # Use ElementTree to parse the file directly
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                # Convert the parsed XML to a string
                xml_string = ET.tostring(root, encoding='utf-8').decode('utf-8')
                
                logger.info(f"Successfully read XML file: {file_path}")
                yield xml_string
                
            except ET.ParseError as e:
                logger.error(f"Failed to parse XML file {file_path}: {e}")
            except Exception as e:
                logger.error(f"Error reading XML file {file_path}: {e}")


class _ParseXmlFileDoFn(beam.DoFn):
    """A DoFn to parse an XML string and yield individual records."""

    def __init__(
        self, record_element_tag: str, include_attributes: bool = False, attribute_prefix: str = "@"
    ):
        self._record_tag = record_element_tag
        self._include_attributes = include_attributes
        self._attribute_prefix = attribute_prefix

    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Converts an XML element (and its children) to a dictionary."""
        record_dict = {}
        
        # Include attributes if configured
        if self._include_attributes:
            for name, value in element.attrib.items():
                attr_key = f"{self._attribute_prefix}{name}"
                record_dict[attr_key] = value
        
        # Handle child elements
        for child in element:
            # If the child has children, recursively convert them
            if len(child) > 0:
                child_dict = self._element_to_dict(child)
                if child.tag in record_dict:
                    if isinstance(record_dict[child.tag], list):
                        record_dict[child.tag].append(child_dict)
                    else:
                        record_dict[child.tag] = [record_dict[child.tag], child_dict]
                else:
                    record_dict[child.tag] = child_dict
            else:
                # Handle potential duplicate tags
                if child.tag in record_dict:
                    if isinstance(record_dict[child.tag], list):
                        record_dict[child.tag].append(child.text)
                    else:
                        record_dict[child.tag] = [record_dict[child.tag], child.text]
                else:
                    record_dict[child.tag] = child.text
                
                # Include attributes of child elements if configured
                if self._include_attributes and child.attrib:
                    if child.tag not in record_dict:
                        record_dict[child.tag] = {}
                    
                    if isinstance(record_dict[child.tag], dict):
                        for name, value in child.attrib.items():
                            attr_key = f"{self._attribute_prefix}{name}"
                            record_dict[child.tag][attr_key] = value
                    elif isinstance(record_dict[child.tag], list) and isinstance(record_dict[child.tag][-1], dict):
                        for name, value in child.attrib.items():
                            attr_key = f"{self._attribute_prefix}{name}"
                            record_dict[child.tag][-1][attr_key] = value

        return record_dict

    def process(self, xml_content: str):
        """
        Parses an entire XML file, finds elements matching the record tag,
        and yields each as a dictionary.

        Args:
            xml_content: The entire XML file content as a string.
        """
        try:
            # Parse the entire XML file
            root = ET.fromstring(xml_content)
            
            # Find all elements matching the record tag within the parsed structure
            matching_elements = root.findall(f".//{self._record_tag}")
            
            # If the root element itself is the record tag, include it
            if root.tag == self._record_tag and root not in matching_elements:
                matching_elements.insert(0, root)
            
            if not matching_elements:
                logger.warning(
                    f"No elements found with tag '{self._record_tag}' in XML file"
                )
                return
                
            logger.info(f"Found {len(matching_elements)} {self._record_tag} elements in XML file")
            
            # Process each matching element
            for record_element in matching_elements:
                yield self._element_to_dict(record_element)

        except ET.ParseError as e:
            logger.error(
                f"Failed to parse XML file: {e}.",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Error processing XML file: {e}.",
                exc_info=True,
            )


@TransformRegistry.register("ParseXml")
class ParseXmlTransform(BaseTransform):
    """
    Parse XML strings into dictionaries.

    Parameters:
        record_element_tag: The XML tag name of the elements representing individual records.
        include_attributes: Whether to include element attributes in the output dictionary.
        attribute_prefix: Prefix for attribute keys in the output dictionary.
    """

    PARAMETERS = {
        "record_element_tag": {
            "type": "string",
            "description": "The XML tag name of the elements representing individual records",
            "required": True,
        },
        "include_attributes": {
            "type": "boolean",
            "description": "Include element attributes in the output dictionary",
            "required": False,
        },
        "attribute_prefix": {
            "type": "string",
            "description": "Prefix for attribute keys in the output dictionary",
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
        record_element_tag = self.config.get("record_element_tag")
        include_attributes = self.config.get("include_attributes", False)
        attribute_prefix = self.config.get("attribute_prefix", "@")

        return beam.ParDo(
            _ParseXmlFileDoFn(
                record_element_tag=record_element_tag,
                include_attributes=include_attributes,
                attribute_prefix=attribute_prefix,
            )
        )
