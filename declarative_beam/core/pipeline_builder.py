"""
Pipeline builder for the declarative beam pipeline framework.

This module provides functionality for building Apache Beam pipelines from YAML configurations.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import PCollection, PBegin, PDone, TaggedOutput

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry
from declarative_beam.core.yaml_processor import load_yaml_config, validate_pipeline_config

logger = logging.getLogger(__name__)


class PipelineBuilder:
    """Builder for Apache Beam pipelines from YAML configurations."""

    def __init__(self, config_path: str) -> None:
        """Initialize the pipeline builder.

        Args:
            config_path: The path to the YAML configuration file.
        """
        self.config_path = config_path
        self.config = load_yaml_config(config_path)
        validate_pipeline_config(self.config)

        # Initialize transforms dictionary
        self.transforms: Dict[str, BaseTransform] = {}
        self._init_transforms()

    def _init_transforms(self) -> None:
        """Initialize the transforms from the configuration."""
        for transform_config in self.config.get("transforms", []):
            name = transform_config.get("name")
            if not name:
                raise ValueError("Transform is missing a name")

            transform_type = transform_config.get("type")
            if not transform_type:
                raise ValueError(f"Transform '{name}' is missing a type")

            # Get the transform class from the registry
            try:
                transform_class = TransformRegistry.get_transform(transform_type)
            except ValueError as e:
                raise ValueError(f"Error initializing transform '{name}': {e}")

            # Initialize the transform
            inputs = transform_config.get("inputs", [])
            outputs = transform_config.get("outputs", [])
            side_inputs = transform_config.get("side_inputs", [])

            transform = transform_class(
                name=name,
                config=transform_config,
                inputs=inputs,
                outputs=outputs,
                side_inputs=side_inputs,
            )

            # Validate the transform configuration
            transform.validate_config()

            # Add the transform to the dictionary
            self.transforms[name] = transform

    def _validate_dependencies(self) -> None:
        """Validate that all transform dependencies are satisfied.

        Raises:
            ValueError: If a transform dependency is not satisfied.
        """
        # Check that all inputs and side inputs are produced by some transform
        all_outputs: Set[str] = set()
        for transform in self.transforms.values():
            all_outputs.update(transform.get_output_names())

        # Check inputs and side inputs for each transform
        for transform in self.transforms.values():
            for input_name in transform.get_input_dependencies():
                if input_name not in all_outputs:
                    raise ValueError(
                        f"Transform '{transform.name}' depends on input '{input_name}' "
                        f"which is not produced by any transform"
                    )

            for side_input in transform.get_side_input_dependencies():
                if side_input not in all_outputs:
                    raise ValueError(
                        f"Transform '{transform.name}' depends on side input '{side_input}' "
                        f"which is not produced by any transform"
                    )

    def _build_pipeline_graph(
        self, pipeline: beam.Pipeline
    ) -> Dict[str, Union[PCollection, List[PCollection]]]:
        """Build the pipeline graph.

        Args:
            pipeline: The Apache Beam pipeline to build the graph in.

        Returns:
            A dictionary mapping output names to PCollections.
        """
        # Dictionary to store PCollections by output name
        pcollections: Dict[str, Union[PCollection, List[PCollection]]] = {}

        # Sort transforms to ensure dependencies are processed first
        sorted_transforms = self._topological_sort()
        logger.info(f"Sorted transforms: {sorted_transforms}")

        # Process each transform in order
        for transform_name in sorted_transforms:
            transform = self.transforms[transform_name]
            logger.info(f"Building transform: {transform_name}")

            # Get input PCollections
            input_pcolls: List[PCollection] = []
            for input_name in transform.get_input_dependencies():
                if input_name not in pcollections:
                    raise ValueError(
                        f"Transform '{transform_name}' depends on input '{input_name}' "
                        f"which is not yet available"
                    )
                input_pcolls.append(cast(PCollection, pcollections[input_name]))

            # Get side input PCollections
            side_input_pcolls: Dict[str, PCollection] = {}
            for side_input in transform.get_side_input_dependencies():
                if side_input not in pcollections:
                    raise ValueError(
                        f"Transform '{transform_name}' depends on side input '{side_input}' "
                        f"which is not yet available"
                    )
                # Convert the PCollection to a view transform for side inputs
                # This creates a singleton dictionary view for the side input
                pcoll = cast(PCollection, pcollections[side_input])
                side_input_pcolls[side_input] = beam.pvalue.AsDict(pcoll)

            # Build the transform
            beam_transform = transform.build_transform(side_inputs=side_input_pcolls)

            # Apply the transform to the input PCollections
            if not input_pcolls:
                # This is a root transform (e.g., Read)
                result = pipeline | f"{transform_name}" >> beam_transform
            else:
                # This is a transform that takes inputs
                if len(input_pcolls) == 1:
                    # Single input
                    result = input_pcolls[0] | f"{transform_name}" >> beam_transform
                else:
                    # Multiple inputs - this would typically be a Flatten or similar
                    result = input_pcolls | f"{transform_name}" >> beam_transform

            # Store the output PCollections
            if len(transform.get_output_names()) == 1:
                # Single output
                output_name = transform.get_output_names()[0]
                pcollections[output_name] = result
            elif len(transform.get_output_names()) > 1:
                # Multiple outputs
                output_tag_mapping = transform.get_output_tag_mapping()
                
                # For transforms that return a PCollection with multiple outputs
                # The result is already a PCollectionTuple with tags
                try:
                    for output_name, tag in output_tag_mapping.items():
                        pcollections[output_name] = result[tag]
                except Exception as e:
                    logger.error(f"Error accessing tag {tag} from result: {e}")
                    logger.error(f"Available tags: {result.keys() if hasattr(result, 'keys') else 'unknown'}")
                    raise

        return pcollections

    def _topological_sort(self) -> List[str]:
        """Sort transforms topologically to ensure dependencies are processed first.

        Returns:
            A list of transform names in topological order.
        """
        # Build a graph of dependencies
        graph: Dict[str, List[str]] = {name: [] for name in self.transforms}
        output_producers: Dict[str, str] = {}
        
        # First, identify which transform produces each output
        for name, transform in self.transforms.items():
            for output_name in transform.get_output_names():
                output_producers[output_name] = name
        
        logger.info(f"Output producers: {output_producers}")
        
        # Then build the dependency graph
        for name, transform in self.transforms.items():
            for input_name in transform.get_input_dependencies():
                # Find which transform produces this input
                if input_name in output_producers:
                    producer = output_producers[input_name]
                    graph[name].append(producer)
                    logger.info(f"Added dependency: {name} depends on {producer} for input {input_name}")
                else:
                    logger.warning(f"No producer found for input {input_name} required by {name}")

            # Add dependencies for side inputs
            for side_input in transform.get_side_input_dependencies():
                # Find which transform produces this side input
                if side_input in output_producers:
                    producer = output_producers[side_input]
                    graph[name].append(producer)
                    logger.info(f"Added dependency: {name} depends on {producer} for side input {side_input}")
                else:
                    logger.warning(f"No producer found for side input {side_input} required by {name}")

        # Perform topological sort
        visited: Set[str] = set()
        temp_visited: Set[str] = set()
        result: List[str] = []

        def visit(node: str) -> None:
            """Visit a node in the graph.

            Args:
                node: The node to visit.

            Raises:
                ValueError: If there is a cycle in the graph.
            """
            if node in temp_visited:
                cycle_nodes = ", ".join(temp_visited)
                raise ValueError(f"Cycle detected in pipeline graph: {cycle_nodes}")
            if node not in visited:
                temp_visited.add(node)
                for neighbor in graph[node]:
                    visit(neighbor)
                temp_visited.remove(node)
                visited.add(node)
                result.append(node)

        # Visit all nodes
        for node in graph:
            if node not in visited:
                visit(node)

        # The result is already in reverse topological order (dependencies first)
        # so we don't need to reverse it
        return result

    def build_pipeline(self) -> beam.Pipeline:
        """Build the Apache Beam pipeline.

        Returns:
            The built Apache Beam pipeline.
        """
        # Validate dependencies
        self._validate_dependencies()

        # Create pipeline options
        pipeline_options_dict = self.config.get("pipeline_options", {})
        pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)

        # Create the pipeline
        pipeline = beam.Pipeline(options=pipeline_options)

        # Build the pipeline graph
        self._build_pipeline_graph(pipeline)

        return pipeline
