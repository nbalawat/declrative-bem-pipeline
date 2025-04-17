# Advanced Topics

This section covers advanced topics and customization options for the Declarative Beam Pipeline framework.

## YAML Pipeline Schema (Updated)

All pipeline YAML files must now include a top-level `runner` section and place transform parameters at the top level of each transform. Example:

```yaml
runner:
  type: DirectRunner
  options: {}

transforms:
  - name: ExampleTransform
    type: SomeType
    param1: value1
    outputs: [example_output]
```

See the reference docs for details.

## Contents

- [Custom Transforms](custom_transforms.md): Creating your own transforms
- [Pipeline Optimization](pipeline_optimization.md): Optimizing pipeline performance
- [Advanced Windowing](advanced_windowing.md): Advanced windowing techniques
- [Testing Strategies](testing_strategies.md): Strategies for testing pipelines
- [Error Handling](error_handling.md): Advanced error handling techniques
- [Monitoring and Logging](monitoring_logging.md): Monitoring and logging pipelines
- [Dataflow Integration](dataflow_integration.md): Running pipelines on Google Cloud Dataflow
- [Docker Deployment](docker_deployment.md): Deploying pipelines with Docker
