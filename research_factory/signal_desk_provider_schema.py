"""Provider-only schema lowering; canonical local validation remains unchanged."""
from copy import deepcopy
from .signal_desk_rubric_reference_packets import digest


def lower(schema):
    result = deepcopy(schema); removed = []
    def visit(node, path):
        if isinstance(node, dict):
            if "uniqueItems" in node:
                if node["uniqueItems"] is not True or node.get("type") != "array":
                    raise ValueError("unexpected uniqueItems declaration")
                removed.append(path + ["uniqueItems"]); del node["uniqueItems"]
            for key, value in node.items(): visit(value, path + [key])
        elif isinstance(node, list):
            for index, value in enumerate(node): visit(value, path + [index])
    visit(result, [])
    return result, {"version": "signal-desk-provider-schema-v1", "canonical_schema_sha256": digest(schema),
        "provider_schema_sha256": digest(result), "removed_provider_keywords": removed,
        "local_uniqueness_validation_mandatory": True, "semantic_contract_changed": False}
