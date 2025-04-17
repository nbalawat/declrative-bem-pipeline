# XML Transforms

The XML transforms allow you to read and parse XML data in your pipelines.

## ReadFromXml

The `ReadFromXml` transform reads XML files and parses them into dictionaries.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_pattern` | string | Yes | The file pattern to read from |
| `record_element_tag` | string | Yes | The XML tag name of the elements representing individual records |
| `include_attributes` | boolean | No | Whether to include element attributes in the output dictionary (default: false) |
| `attribute_prefix` | string | No | Prefix for attribute keys in the output dictionary (default: "@") |

### Example

```yaml
- name: ReadFromXml
  type: ReadFromXml
  file_pattern: data/*.xml
  record_element_tag: product
  include_attributes: true
  attribute_prefix: "@"
  outputs:
    - products
```

## ParseXml

The `ParseXml` transform parses XML strings into dictionaries.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `record_element_tag` | string | Yes | The XML tag name of the elements representing individual records |
| `include_attributes` | boolean | No | Whether to include element attributes in the output dictionary (default: false) |
| `attribute_prefix` | string | No | Prefix for attribute keys in the output dictionary (default: "@") |

### Example

```yaml
- name: ParseXml
  type: ParseXml
  record_element_tag: product
  include_attributes: true
  attribute_prefix: "@"
  inputs:
    - xml_strings
  outputs:
    - parsed_products
```

## Implementation Details

The XML parsing is implemented using Python's built-in `xml.etree.ElementTree` module. The parser converts XML elements and their attributes into nested dictionaries.

For elements with multiple child elements with the same tag, the parser will convert them into a list. For example:

```xml
<product>
  <feature>5G</feature>
  <feature>128GB Storage</feature>
</product>
```

Will be parsed as:

```python
{
  "feature": ["5G", "128GB Storage"]
}
```

Attributes are included in the output dictionary if `include_attributes` is set to `true`. They are prefixed with the value of `attribute_prefix` (default: "@"). For example:

```xml
<product id="1" category="electronics">
  <name>Smartphone</name>
</product>
```

Will be parsed as:

```python
{
  "@id": "1",
  "@category": "electronics",
  "name": "Smartphone"
}
```
