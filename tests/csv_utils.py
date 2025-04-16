
def parse_csv_line(line):
    """Parse a CSV line into a dictionary."""
    values = line.strip().split(',')
    if len(values) >= 3:
        return {
            'id': values[0],
            'name': values[1],
            'value': values[2]
        }
    return None
