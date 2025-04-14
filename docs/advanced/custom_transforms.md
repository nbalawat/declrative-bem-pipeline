# Creating Custom Transforms

This guide explains how to create custom transforms for the Declarative Beam Pipeline framework.

## Overview

The framework is designed to be extensible, allowing you to create custom transforms to meet your specific needs. Custom transforms can be used to implement specialized logic, integrate with external systems, or optimize performance for specific use cases.

## Basic Structure of a Transform

All transforms in the framework inherit from the `BaseTransform` class and are registered with the `TransformRegistry`. Here's the basic structure:

```python
from typing import Any, Dict, Optional

import apache_beam as beam

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("MyCustomTransform")
class MyCustomTransform(BaseTransform):
    """
    Description of what your transform does.
    
    Parameters:
        param1: Description of parameter 1
        param2: Description of parameter 2
    """
    
    # Define the parameters this transform accepts
    PARAMETERS = {
        'param1': {
            'type': 'string',
            'description': 'Description of parameter 1',
            'required': True
        },
        'param2': {
            'type': 'number',
            'description': 'Description of parameter 2',
            'required': False
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        """
        Build and return the Apache Beam PTransform.
        
        Args:
            side_inputs: Optional dictionary of side inputs
            
        Returns:
            A beam.PTransform instance
        """
        # Get configuration parameters
        param1 = self.config.get('param1')
        param2 = self.config.get('param2', 0)  # Default value if not provided
        
        # Validate parameters
        if not param1:
            raise ValueError(f"Transform '{self.name}' requires 'param1'")
        
        # Create and return the Apache Beam PTransform
        return beam.Map(lambda element: self._process_element(element, param1, param2))
    
    def _process_element(self, element, param1, param2):
        """
        Process a single element.
        
        Args:
            element: The input element
            param1: Parameter 1 value
            param2: Parameter 2 value
            
        Returns:
            The processed element
        """
        # Implement your custom logic here
        return element  # Replace with your actual implementation
```

## Step-by-Step Guide

### 1. Create a New Module

Create a new Python module in the appropriate package. For example, if you're creating a custom processing transform, you might create a file at `declarative_beam/transforms/processing/custom.py`.

### 2. Define Your Transform Class

Define your transform class, inheriting from `BaseTransform`:

```python
from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry

@TransformRegistry.register("MyCustomTransform")
class MyCustomTransform(BaseTransform):
    """Documentation for your transform."""
    
    # Define parameters
    PARAMETERS = { ... }
    
    def build_transform(self, side_inputs=None):
        # Implement your transform
        ...
```

### 3. Implement the `build_transform` Method

The `build_transform` method is the core of your transform. It should:

1. Extract and validate configuration parameters
2. Create and return an Apache Beam PTransform

```python
def build_transform(self, side_inputs=None):
    # Get configuration parameters
    param1 = self.config.get('param1')
    
    # Validate parameters
    if not param1:
        raise ValueError(f"Transform '{self.name}' requires 'param1'")
    
    # Create and return a PTransform
    return beam.Map(lambda x: x * 2)  # Example implementation
```

### 4. Register Your Transform

Use the `@TransformRegistry.register` decorator to register your transform with a unique name:

```python
@TransformRegistry.register("MyCustomTransform")
class MyCustomTransform(BaseTransform):
    ...
```

### 5. Import Your Transform Module

Make sure your transform module is imported when the framework starts. You can do this by adding an import statement in your package's `__init__.py` file:

```python
# In declarative_beam/transforms/__init__.py
from declarative_beam.transforms.processing import custom
```

## Example: Creating a Custom Aggregation Transform

Here's an example of a custom transform that computes the average value for each key:

```python
import apache_beam as beam
from apache_beam.transforms.combiners import MeanCombineFn

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("AverageByKey")
class AverageByKey(BaseTransform):
    """
    Compute the average value for each key.
    
    Parameters:
        value_field: The field containing the value to average
    """
    
    PARAMETERS = {
        'value_field': {
            'type': 'string',
            'description': 'The field containing the value to average',
            'required': True
        }
    }
    
    def build_transform(self, side_inputs=None):
        value_field = self.config.get('value_field')
        
        if not value_field:
            raise ValueError(f"Transform '{self.name}' requires 'value_field'")
        
        class ExtractValueFn(beam.DoFn):
            def process(self, element):
                key, value = element
                if isinstance(value, dict) and value_field in value:
                    try:
                        numeric_value = float(value[value_field])
                        yield (key, numeric_value)
                    except (ValueError, TypeError):
                        # Skip non-numeric values
                        pass
        
        return beam.PTransform(
            lambda pcoll: (
                pcoll
                | beam.ParDo(ExtractValueFn())
                | beam.CombinePerKey(MeanCombineFn())
            )
        )
```

## Example: Creating a Custom I/O Transform

Here's an example of a custom transform that reads data from a MongoDB collection:

```python
import apache_beam as beam
from apache_beam.io.mongodbio import ReadFromMongoDB

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("ReadFromMongoDB")
class ReadFromMongoDBTransform(BaseTransform):
    """
    Read data from a MongoDB collection.
    
    Parameters:
        uri: MongoDB connection URI
        db: Database name
        collection: Collection name
        filter: Optional filter query
    """
    
    PARAMETERS = {
        'uri': {
            'type': 'string',
            'description': 'MongoDB connection URI',
            'required': True
        },
        'db': {
            'type': 'string',
            'description': 'Database name',
            'required': True
        },
        'collection': {
            'type': 'string',
            'description': 'Collection name',
            'required': True
        },
        'filter': {
            'type': 'object',
            'description': 'Filter query',
            'required': False
        }
    }
    
    def build_transform(self, side_inputs=None):
        uri = self.config.get('uri')
        db = self.config.get('db')
        collection = self.config.get('collection')
        filter_query = self.config.get('filter', {})
        
        if not uri or not db or not collection:
            raise ValueError(
                f"Transform '{self.name}' requires 'uri', 'db', and 'collection'"
            )
        
        return ReadFromMongoDB(
            uri=uri,
            db=db,
            collection=collection,
            filter_query=filter_query
        )
```

## Testing Custom Transforms

It's important to test your custom transforms to ensure they work correctly. Here's an example of how to test a custom transform:

```python
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from declarative_beam.transforms.processing.custom import MyCustomTransform


class MyCustomTransformTest(unittest.TestCase):
    def test_my_custom_transform(self):
        # Create test data
        input_data = [{'id': '1', 'value': '10'}, {'id': '2', 'value': '20'}]
        expected_output = [{'id': '1', 'value': '10', 'processed': True}, 
                          {'id': '2', 'value': '20', 'processed': True}]
        
        # Create a test pipeline
        with TestPipeline() as p:
            # Create an input PCollection
            input_pcoll = p | beam.Create(input_data)
            
            # Create the transform
            transform = MyCustomTransform(
                name='TestTransform',
                config={'param1': 'test_value'},
                inputs=['input'],
                outputs=['output']
            )
            
            # Apply the transform
            output_pcoll = input_pcoll | transform.build_transform()
            
            # Assert that the output matches the expected output
            assert_that(output_pcoll, equal_to(expected_output))
```

## Best Practices

When creating custom transforms, follow these best practices:

1. **Document your transform**: Provide clear documentation for your transform, including parameters and examples.
2. **Validate parameters**: Check that required parameters are provided and have valid values.
3. **Handle errors gracefully**: Catch and handle exceptions appropriately.
4. **Write tests**: Test your transform with various inputs to ensure it works correctly.
5. **Follow the single responsibility principle**: Each transform should do one thing well.
6. **Use type hints**: Add type hints to your code to improve readability and catch errors.
7. **Optimize for performance**: Consider performance implications, especially for large datasets.
8. **Make transforms reusable**: Design transforms to be reusable across different pipelines.

## Registering Transforms in Different Packages

If you're creating transforms in a separate package, you can register them with the framework by importing the package and calling the `register_transforms` function:

```python
from declarative_beam.core.transform_registry import TransformRegistry

# Register transforms from your custom package
import my_custom_package.transforms
TransformRegistry.register_transforms(my_custom_package.transforms)
```

## Conclusion

Creating custom transforms allows you to extend the Declarative Beam Pipeline framework to meet your specific needs. By following the patterns and best practices described in this guide, you can create robust, reusable transforms that integrate seamlessly with the framework.
