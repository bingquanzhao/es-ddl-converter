#!/usr/bin/env python3
"""Download real-world ES mappings and clean them into test fixtures.

Sources:
  - elastic/ecs (legacy template)
  - wazuh/wazuh (security monitoring template)
  - elastic/rally-tracks (geonames, nyc_taxis, nested, dense_vector)
  - jaegertracing/jaeger (span template)
  - elastic/kibana (sample data: ecommerce, flights, logs)

Usage:
    python scripts/fetch_realworld_fixtures.py
"""

import json
import os
import re
import sys
import urllib.request

OUTPUT_DIR = os.path.join(
    os.path.dirname(__file__), "..", "tests", "fixtures", "realworld"
)

SOURCES = [
    {
        "name": "ecs_template",
        "url": "https://raw.githubusercontent.com/elastic/ecs/main/generated/elasticsearch/legacy/template.json",
        "kind": "index_template",
    },
    {
        "name": "wazuh_template",
        "url": "https://raw.githubusercontent.com/wazuh/wazuh/master/extensions/elasticsearch/7.x/wazuh-template.json",
        "kind": "index_template",
    },
    {
        "name": "rally_geonames",
        "url": "https://raw.githubusercontent.com/elastic/rally-tracks/master/geonames/index.json",
        "kind": "rally",
    },
    {
        "name": "rally_nyc_taxis",
        "url": "https://raw.githubusercontent.com/elastic/rally-tracks/master/nyc_taxis/index.json",
        "kind": "rally",
    },
    {
        "name": "rally_nested",
        "url": "https://raw.githubusercontent.com/elastic/rally-tracks/master/nested/index.json",
        "kind": "rally",
    },
    {
        "name": "rally_dense_vector",
        "url": "https://raw.githubusercontent.com/elastic/rally-tracks/master/dense_vector/index.json",
        "kind": "rally",
    },
    {
        "name": "jaeger_span",
        "url": "https://raw.githubusercontent.com/jaegertracing/jaeger/v1.53.0/plugin/storage/es/mappings/jaeger-span-7.json",
        "kind": "jaeger",
    },
    {
        "name": "kibana_sample_ecommerce",
        "url": "https://raw.githubusercontent.com/elastic/kibana/7.9/src/plugins/home/server/services/sample_data/data_sets/ecommerce/field_mappings.ts",
        "kind": "kibana_ts",
    },
    {
        "name": "kibana_sample_flights",
        "url": "https://raw.githubusercontent.com/elastic/kibana/7.9/src/plugins/home/server/services/sample_data/data_sets/flights/field_mappings.ts",
        "kind": "kibana_ts",
    },
    {
        "name": "kibana_sample_logs",
        "url": "https://raw.githubusercontent.com/elastic/kibana/7.9/src/plugins/home/server/services/sample_data/data_sets/logs/field_mappings.ts",
        "kind": "kibana_ts",
    },
]


def fetch_url(url):
    """Download a URL and return its text content."""
    print("  Fetching {}".format(url))
    req = urllib.request.Request(url, headers={"User-Agent": "es-ddl-converter-fixture-fetcher"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def strip_jinja2(text):
    """Remove Jinja2 template tags from Rally track index files."""
    # Remove comment tags: {# ... #}
    text = re.sub(r"\{#.*?#\}", "", text, flags=re.DOTALL)
    # Remove block tags: {% ... %}
    text = re.sub(r"\{%.*?%\}", "", text, flags=re.DOTALL)
    # Handle quoted string defaults: "{{ var | default('val') }}" -> "val"
    text = re.sub(
        r"""\"\{\{[^}]*\|\s*default\(['\"]([^'\"]*?)['\"]\)[^}]*\}\}\"""",
        lambda m: '"{}"'.format(m.group(1)),
        text,
    )
    # Handle quoted numeric/bool defaults: "{{ var | default(5) }}" -> "5"
    text = re.sub(
        r"""\"\{\{[^}]*\|\s*default\((\w+)\)[^}]*\}\}\"""",
        lambda m: '"{}"'.format(m.group(1)),
        text,
    )
    # Handle bare variable tags with defaults (not in quotes)
    # e.g. {{ source_enabled | default(true) | tojson }} -> true
    text = re.sub(
        r"\{\{[^}]*\|\s*default\((\w+)\)[^}]*\}\}",
        lambda m: m.group(1),
        text,
    )
    # Catch-all: remaining "{{ ... }}" (quoted) -> ""
    text = re.sub(r'"\{\{.*?\}\}"', '""', text)
    # Catch-all: remaining {{ ... }} (bare) -> ""
    text = re.sub(r"\{\{.*?\}\}", '""', text)
    # Strip leading lines before the first { that starts JSON
    lines = text.split("\n")
    start_idx = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("{"):
            start_idx = i
            break
    text = "\n".join(lines[start_idx:])
    return text


def strip_go_templates(text):
    """Replace Go template variables in Jaeger mappings with defaults."""
    # Remove conditional blocks: {{- if .Var }}...{{- end }}
    # These often wrap optional JSON sections (aliases, ILM)
    text = re.sub(r"\{\{-?\s*if\s[^}]*\}\}", "", text)
    text = re.sub(r"\{\{-?\s*end\s*-?\}\}", "", text)
    # Replace known variables with defaults
    replacements = {
        r"\{\{-?\s*\.Shards\s*-?\}\}": "5",
        r"\{\{-?\s*\.Replicas\s*-?\}\}": "1",
        r"\{\{-?\s*\.PrioritySpanTemplate\s*-?\}\}": "0",
        r"\{\{-?\s*\.SpanIndexPrefix\s*-?\}\}": "jaeger-span",
        r"\{\{-?\s*\.UseILM\s*-?\}\}": "false",
        r"\{\{-?\s*\.ILMPolicyName\s*-?\}\}": "jaeger-ilm-policy",
        r"\{\{-?\s*\.IndexPrefix\s*-?\}\}": "",
    }
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)
    # Catch-all for any remaining Go template vars
    text = re.sub(r"\{\{-?.*?-?\}\}", '""', text)
    return text


def convert_ts_field_mappings(text):
    """Convert Kibana TypeScript field_mappings.ts to JSON properties dict.

    The file looks like: ``export const fieldMappings = { ... };``
    The object body is valid JSON except for missing quotes on keys,
    single-quoted strings, and trailing commas.
    """
    # Extract the object body between the first { and last }
    start = text.index("{")
    end = text.rindex("}") + 1
    body = text[start:end]
    # Add quotes around bare keys:  key: -> "key":
    body = re.sub(r"(\s)(\w+)\s*:", r'\1"\2":', body)
    # Replace single-quoted strings with double-quoted: 'value' -> "value"
    body = re.sub(r"'([^']*)'", r'"\1"', body)
    # Remove trailing commas before } or ]
    body = re.sub(r",\s*([}\]])", r"\1", body)
    return json.loads(body)


def extract_mappings_from_template(data):
    """Extract mappings from an index template JSON."""
    # Index template format: {"index_patterns": [...], "mappings": {...}, ...}
    if "mappings" in data:
        mappings = data["mappings"]
        # Some templates have type wrapper: {"mappings": {"_doc": {"properties": ...}}}
        if "properties" in mappings:
            return mappings
        for key, val in mappings.items():
            if isinstance(val, dict) and "properties" in val:
                return val
    # Component template format: {"template": {"mappings": {...}}}
    if "template" in data and isinstance(data["template"], dict):
        tmpl = data["template"]
        if "mappings" in tmpl:
            return extract_mappings_from_template(tmpl)
    raise ValueError("Cannot extract mappings from template")


def _count_properties(obj):
    """Count total fields recursively in a mapping structure."""
    count = 0
    if isinstance(obj, dict):
        if "properties" in obj:
            for k, v in obj["properties"].items():
                count += 1
                count += _count_properties(v)
        for k, v in obj.items():
            if k != "properties" and isinstance(v, dict) and "properties" in v:
                count += _count_properties(v)
    return count


def process_source(source):
    """Download, clean, and save one fixture."""
    name = source["name"]
    url = source["url"]
    kind = source["kind"]

    text = fetch_url(url)

    if kind == "kibana_ts":
        properties = convert_ts_field_mappings(text)
        output = {name: {"mappings": {"properties": properties}}}
        # Write output directly and return early
        out_path = os.path.join(OUTPUT_DIR, "{}.json".format(name))
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        field_count = _count_properties(output)
        size_kb = os.path.getsize(out_path) / 1024
        print("  -> {} ({} fields, {:.1f} KB)".format(out_path, field_count, size_kb))
        return out_path

    if kind == "rally":
        text = strip_jinja2(text)
    elif kind == "jaeger":
        text = strip_go_templates(text)

    # Fix trailing commas that Jinja2 stripping may leave
    text = re.sub(r",\s*([}\]])", r"\1", text)

    data = json.loads(text)

    # Normalize to {index_name: {mappings: {...}}} format
    if kind == "index_template":
        mapping_body = extract_mappings_from_template(data)
        output = {name: {"mappings": mapping_body}}
    elif kind == "rally":
        # Rally index.json has {"settings": {...}, "mappings": {...}}
        if "mappings" in data:
            mappings = data["mappings"]
            if "properties" in mappings:
                output = {name: {"mappings": mappings}}
            else:
                # Check for type wrappers
                for key, val in mappings.items():
                    if isinstance(val, dict) and "properties" in val:
                        output = {name: {"mappings": val}}
                        break
                else:
                    raise ValueError("Cannot find properties in Rally mapping for {}".format(name))
        else:
            raise ValueError("No mappings in Rally index.json for {}".format(name))
    elif kind == "jaeger":
        # Jaeger format: {"index_patterns": "...", "mappings": {...}}
        if "mappings" in data:
            mappings = data["mappings"]
            if "properties" in mappings:
                output = {name: {"mappings": mappings}}
            else:
                for key, val in mappings.items():
                    if isinstance(val, dict) and "properties" in val:
                        output = {name: {"mappings": val}}
                        break
                else:
                    raise ValueError("Cannot find properties in Jaeger mapping for {}".format(name))
        else:
            raise ValueError("No mappings in Jaeger JSON for {}".format(name))
    else:
        raise ValueError("Unknown source kind: {}".format(kind))

    # Write output
    out_path = os.path.join(OUTPUT_DIR, "{}.json".format(name))
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    field_count = _count_properties(output)
    size_kb = os.path.getsize(out_path) / 1024
    print("  -> {} ({} fields, {:.1f} KB)".format(out_path, field_count, size_kb))
    return out_path


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Downloading real-world ES mapping fixtures...")
    print("Output: {}\n".format(os.path.abspath(OUTPUT_DIR)))

    results = []
    errors = []
    for source in SOURCES:
        print("[{}]".format(source["name"]))
        try:
            path = process_source(source)
            results.append((source["name"], path))
        except Exception as e:
            print("  ERROR: {}".format(e))
            errors.append((source["name"], str(e)))
        print()

    print("=" * 60)
    print("Done: {}/{} fixtures generated".format(len(results), len(SOURCES)))
    if errors:
        print("Errors:")
        for name, err in errors:
            print("  {}: {}".format(name, err))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
