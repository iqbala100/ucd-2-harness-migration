#!/usr/bin/env python3
"""
UCD → Harness YAML Converter
----------------------------
Converts an IBM UrbanCode Deploy (UCD) export *folder or zip* into a set of
Harness YAMLs (services, environments, and a basic pipeline per UCD application).

Usage:
  python ucd_to_harness.py --input <ucd_export_dir_or_zip> --output <out_dir> \
    --project-id PROJ --org-id ORG [--deployment-type Kubernetes|Ssh|NativeHelm] [--dry-run]

Notes & Assumptions:
- Best-effort parsing of UCD exports. UCD exports vary by version & options.
- Mapping:
    * UCD Application -> Harness Pipeline (1 pipeline per application)
    * UCD Component  -> Harness Service (1 service per component)
    * UCD Environment -> Harness Environment
    * UCD Processes (application/component) -> Pipeline steps (ShellScript placeholders)
- You can edit the generated YAMLs later in Harness to refine infra, connectors, etc.
"""

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Try to import PyYAML (preferred). Fallback to a tiny dumper.
try:
    import yaml  # type: ignore
    HAVE_PYYAML = True
except Exception:
    HAVE_PYYAML = False

def _simple_yaml_dump(data: Any) -> str:
    """Very small YAML dumper fallback (not full-featured)."""
    import json as _json
    # This is a naive conversion; for high-fidelity YAML, install PyYAML.
    return _json.dumps(data, indent=2)

def to_yaml(data: Any) -> str:
    if HAVE_PYYAML:
        # Ensure stable keys and readable multiline strings
        class LiteralStr(str): pass
        def _repr_literal(dumper, data):
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        yaml.add_representer(LiteralStr, _repr_literal)
        return yaml.safe_dump(data, sort_keys=False, default_flow_style=False, width=100000)
    else:
        return _simple_yaml_dump(data)

def sanitize_identifier(name: str) -> str:
    base = re.sub(r'[^A-Za-z0-9_]+', '_', name.strip())
    base = re.sub(r'^_+|_+$', '', base)
    return base or f"id_{uuid.uuid4().hex[:8]}"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def unzip_if_needed(input_path: Path) -> Path:
    if input_path.is_dir():
        return input_path
    if input_path.is_file() and input_path.suffix.lower() == ".zip":
        tmp = Path(tempfile.mkdtemp(prefix="ucd_export_"))
        with shutil.unpack_archive(str(input_path), str(tmp), format="zip"):
            # Some archives store all content under a single top folder; normalize
            # to the single folder if there is exactly one child dir
            subdirs = [d for d in tmp.iterdir() if d.is_dir()]
            if len(subdirs) == 1 and not any(p.suffix for p in tmp.iterdir() if p.is_file()):
                return subdirs[0]
            return tmp
    raise FileNotFoundError(f"Input '{input_path}' is neither a directory nor a .zip file.")

def read_all_json(root: Path) -> List[Tuple[Path, Dict[str, Any]]]:
    items: List[Tuple[Path, Dict[str, Any]]] = []
    for p in root.rglob("*.json"):
        try:
            with p.open("r", encoding="utf-8") as f:
                obj = json.load(f)

            # If the file is a dict, keep as-is
            if isinstance(obj, dict):
                items.append((p, obj))

            # If the file is a list, expand each dict element
            elif isinstance(obj, list):
                for i, entry in enumerate(obj):
                    if isinstance(entry, dict):
                        items.append((p.with_name(f"{p.stem}_{i}{p.suffix}"), entry))
                    else:
                        print(f"[warn] Skipping non-dict element in list: {p}[{i}]")

            else:
                print(f"[warn] Skipping non-dict JSON root: {p}")

        except Exception as e:
            print(f"[warn] Failed to parse JSON: {p} -> {e}", file=sys.stderr)

    return items


def detect_kind(obj: Dict[str, Any], path: Path) -> str:
    # Heuristics across UCD export variants
    for key in ("objectType", "class", "entityType", "type"):
        if key in obj and isinstance(obj[key], str):
            v = obj[key].lower()
            if "application" in v and "template" not in v:
                return "application"
            if "component" in v and "template" not in v:
                return "component"
            if "environment" in v:
                return "environment"
            if "process" in v:
                return "process"

    # Path-based hints
    pstr = str(path).lower()
    if "applications" in pstr and "process" not in pstr:
        return "application"
    if "components" in pstr and "template" not in pstr:
        return "component"
    if "environments" in pstr:
        return "environment"
    if "process" in pstr or "processes" in pstr:
        return "process"

    # Key-based hints
    if "components" in obj and "environments" in obj:
        return "application"
    if {"component", "componentName"} & obj.keys():
        return "component"
    if {"environment", "environmentName"} & obj.keys():
        return "environment"
    if {"steps", "rootActivity"} & obj.keys():
        return "process"

    return "unknown"

def pull_name(obj: Dict[str, Any]) -> str:
    for k in ("name", "application", "applicationName", "component", "componentName", "environment", "environmentName", "displayName"):
        if k in obj and isinstance(obj[k], str) and obj[k].strip():
            return obj[k]
    # Fallback: find any 'name' nested
    for k, v in obj.items():
        if isinstance(v, dict) and "name" in v and isinstance(v["name"], str):
            return v["name"]
    return f"unnamed-{uuid.uuid4().hex[:6]}"

def flatten_process_steps(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract a linear list of steps from a UCD process (best effort).
    UCD processes can be graphs; we output a readable sequence of placeholders.
    """
    steps: List[Dict[str, Any]] = []

    def visit(node: Dict[str, Any]):
        if not isinstance(node, dict): return
        # Common UCD fields
        step_name = node.get("name") or node.get("commandName") or node.get("type") or "Step"
        props = node.get("properties") or node.get("propDefs") or {}
        desc = node.get("description") or ""
        steps.append({
            "name": str(step_name),
            "description": str(desc),
            "properties": props if isinstance(props, dict) else {},
        })
        # Explore children
        for child_key in ("children", "next", "steps", "activities"):
            child = node.get(child_key)
            if isinstance(child, list):
                for c in child:
                    visit(c)
            elif isinstance(child, dict):
                visit(child)

    # Entry points in various export shapes
    if isinstance(obj.get("rootActivity"), dict):
        visit(obj["rootActivity"])
    elif isinstance(obj.get("steps"), list):
        for s in obj["steps"]:
            visit(s)
    elif isinstance(obj.get("activities"), list):
        for s in obj["activities"]:
            visit(s)
    else:
        # No obvious structure; treat whole object as one step
        visit(obj)

    # Deduplicate by order/name combo to avoid loops
    seen = set()
    uniq = []
    for s in steps:
        k = (s["name"], json.dumps(s["properties"], sort_keys=True))
        if k not in seen:
            seen.add(k)
            uniq.append(s)
    return uniq

def build_service_yaml(name: str, project: str, org: str, deployment_type: str) -> Dict[str, Any]:
    identifier = sanitize_identifier(name)
    return {
        "service": {
            "name": name,
            "identifier": identifier,
            "orgIdentifier": org,
            "projectIdentifier": project,
            "serviceDefinition": {
                "type": deployment_type,
                "spec": {
                    # Users should fill containers/manifests/artifacts as needed.
                }
            }
        }
    }

def build_environment_yaml(name: str, project: str, org: str) -> Dict[str, Any]:
    identifier = sanitize_identifier(name)
    return {
        "environment": {
            "name": name,
            "identifier": identifier,
            "orgIdentifier": org,
            "projectIdentifier": project,
            "type": "PreProduction",
            "tags": {},
            "variables": []
        }
    }

def step_to_shellscript_yaml(step: Dict[str, Any]) -> Dict[str, Any]:
    # Convert a UCD step placeholder into a generic Harness ShellScript step.
    name = step.get("name", "Step")
    ident = sanitize_identifier(name)[:50]
    properties = step.get("properties", {})
    comment_lines = ["# UCD Step Placeholder", f"# Original Name: {name}"]
    if properties:
        for k, v in list(properties.items())[:20]:
            comment_lines.append(f"# {k}: {v}")
        if len(properties) > 20:
            comment_lines.append("# ... (truncated)")
    script = "\n".join(comment_lines + ["echo \"Executing placeholder for UCD step: {}\"".format(name)])
    return {
        "step": {
            "type": "ShellScript",
            "name": name[:100],
            "identifier": ident,
            "spec": {
                "shell": "Bash",
                "onDelegate": True,
                "source": {"type": "Inline", "spec": {"script": script}},
                "environmentVariables": [],
                "outputVariables": []
            },
            "timeout": "10m"
        }
    }

def build_pipeline_yaml(app_name: str, env_name: str, service_names: List[str], process_steps: List[Dict[str, Any]], project: str, org: str, deployment_type: str) -> Dict[str, Any]:
    app_id = sanitize_identifier(app_name)
    env_id = sanitize_identifier(env_name)
    stages = []

    # One Deployment stage per service (simple sequence). Attach steps from process, if any.
    for svc in service_names or ["PlaceholderService"]:
        svc_id = sanitize_identifier(svc)
        exec_steps = [step_to_shellscript_yaml(s) for s in process_steps] or [
            step_to_shellscript_yaml({"name": "Deploy Placeholder"})
        ]
        stages.append({
            "stage": {
                "name": f"Deploy {svc}",
                "identifier": sanitize_identifier(f"Deploy_{svc}")[:63],
                "description": f"Auto-generated from UCD application '{app_name}'",
                "type": "Deployment",
                "spec": {
                    "deploymentType": deployment_type,
                    "service": {"serviceRef": svc_id},
                    "environment": {"environmentRef": env_id},
                    "execution": {
                        "steps": exec_steps
                    }
                }
            }
        })

    return {
        "pipeline": {
            "name": app_name,
            "identifier": app_id,
            "orgIdentifier": org,
            "projectIdentifier": project,
            "stages": stages
        }
    }

def main():
    ap = argparse.ArgumentParser(description="Convert IBM UCD export (dir/zip) to Harness YAMLs.")
    ap.add_argument("--input", required=True, help="Path to UCD export directory or ZIP file")
    ap.add_argument("--output", required=True, help="Output directory for Harness YAMLs")
    ap.add_argument("--project-id", default="UCD_MIGRATED", help="Harness Project identifier")
    ap.add_argument("--org-id", default="default", help="Harness Org identifier")
    ap.add_argument("--deployment-type", default="Kubernetes", choices=["Kubernetes", "Ssh", "NativeHelm", "ServerlessAwsLambda", "AzureWebApp"], help="Harness deployment type to seed")
    ap.add_argument("--dry-run", action="store_true", help="Scan and print what would be generated, but do not write files")
    args = ap.parse_args()

    in_path = Path(args.input).expanduser().resolve()
    out_dir = Path(args.output).expanduser().resolve()
    if not args.dry_run:
        ensure_dir(out_dir)

    export_root = unzip_if_needed(in_path)
    items = read_all_json(export_root)

    applications = {}
    components = {}
    environments = {}
    processes = []  # (name, json)

    # Index objects by kind and name
    for p, obj in items:
        kind = detect_kind(obj, p)
        name = pull_name(obj)
        if kind == "application":
            applications[name] = obj
        elif kind == "component":
            components.setdefault(name, obj)
        elif kind == "environment":
            environments.setdefault(name, obj)
        elif kind == "process":
            processes.append((name, obj))

    # Associate process lists by app/component where possible (best-effort string matching)
    app_process_map: Dict[str, List[Dict[str, Any]]] = {k: [] for k in applications.keys()}
    comp_process_map: Dict[str, List[Dict[str, Any]]] = {k: [] for k in components.keys()}
    for pname, pobj in processes:
        # Try to find owner by common fields or text
        owner = None
        text = json.dumps(pobj).lower()
        # Explicit fields
        for key in ("application", "applicationName"):
            if key in pobj and isinstance(pobj[key], str):
                owner = ("app", pobj[key])
                break
        for key in ("component", "componentName"):
            if key in pobj and isinstance(pobj[key], str):
                owner = ("comp", pobj[key])
                break
        # Heuristic via name or text
        if owner is None:
            for a in applications.keys():
                if a.lower() in (pname.lower() + " " + text):
                    owner = ("app", a); break
        if owner is None:
            for c in components.keys():
                if c.lower() in (pname.lower() + " " + text):
                    owner = ("comp", c); break

        steps = flatten_process_steps(pobj)
        if owner and owner[0] == "app":
            app_process_map.setdefault(owner[1], []).extend(steps)
        elif owner and owner[0] == "comp":
            comp_process_map.setdefault(owner[1], []).extend(steps)

    # Derive service names per app if available; otherwise use all components
    app_services: Dict[str, List[str]] = {app: [] for app in applications.keys()}
    # If application JSON lists components, try to use it
    for app_name, aobj in applications.items():
        comps = []
        for key in ("components", "componentList", "applicationComponents"):
            v = aobj.get(key)
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        nm = it.get("name") or it.get("component") or it.get("componentName")
                        if nm: comps.append(str(nm))
                    elif isinstance(it, str):
                        comps.append(it)
        # Fallback to all components if none detected
        if not comps:
            comps = list(components.keys())
        app_services[app_name] = comps

    # Pick/derive one environment per app (if none, create placeholder)
    app_env: Dict[str, str] = {}
    env_names = list(environments.keys()) or ["PlaceholderEnv"]
    for app in applications.keys() or ["PlaceholderApp"]:
        # Prefer env whose name contains app name
        match = next((e for e in env_names if app.lower() in e.lower()), None)
        app_env[app] = match or env_names[0]

    # DRY RUN: summary
    if args.dry_run:
        print("== Detected ==")
        print(f"Applications: {list(applications.keys()) or ['(none)']}")
        print(f"Components:   {list(components.keys()) or ['(none)']}")
        print(f"Environments: {list(environments.keys()) or ['(none)']}")
        print("\n== Associations ==")
        for app, svcs in app_services.items():
            print(f"App '{app}' -> Services: {svcs or ['(none)']} | Env: {app_env.get(app)} | AppSteps: {len(app_process_map.get(app, []))}")
        for comp, steps in comp_process_map.items():
            print(f"Component '{comp}' -> Steps: {len(steps)}")
        return

    # Write output tree
    root = out_dir / ".harness"
    dirs = {
        "services": root / "services",
        "environments": root / "environments",
        "pipelines": root / "pipelines",
    }
    for d in dirs.values():
        ensure_dir(d)

    # Services from components
    for comp_name in (components.keys() or ["PlaceholderService"]):
        svc_yaml = build_service_yaml(comp_name, args.project_id, args.org_id, args.deployment_type)
        (dirs["services"] / f"{sanitize_identifier(comp_name)}.yaml").write_text(to_yaml(svc_yaml), encoding="utf-8")

    # Environments
    for env_name in (environments.keys() or ["PlaceholderEnv"]):
        env_yaml = build_environment_yaml(env_name, args.project_id, args.org_id)
        (dirs["environments"] / f"{sanitize_identifier(env_name)}.yaml").write_text(to_yaml(env_yaml), encoding="utf-8")

    # Pipelines per application (or one placeholder)
    apps = applications.keys() or ["PlaceholderApp"]
    for app_name in apps:
        env_name = app_env.get(app_name, "PlaceholderEnv")
        proc_steps = app_process_map.get(app_name, [])
        services = app_services.get(app_name, list(components.keys()))
        pipe_yaml = build_pipeline_yaml(app_name, env_name, services, proc_steps, args.project_id, args.org_id, args.deployment_type)
        (dirs["pipelines"] / f"{sanitize_identifier(app_name)}.yaml").write_text(to_yaml(pipe_yaml), encoding="utf-8")

    # README
    readme = f"""# Harness YAMLs generated from UCD export

This folder was generated by `ucd_to_harness.py`.
Edit the YAMLs as needed before importing into Harness.

## Mapping Summary
- UCD Application → Harness Pipeline (in `.harness/pipelines/`)
- UCD Component → Harness Service (in `.harness/services/`)
- UCD Environment → Harness Environment (in `.harness/environments/`)
- UCD Processes → ShellScript placeholders in pipeline stages

## Project/Org
- projectIdentifier: {args.project_id}
- orgIdentifier: {args.org_id}

## Next steps
- Fill service definitions (manifests/artifacts) according to your target infra.
- Review pipeline steps; replace ShellScript placeholders with native steps/templates.
"""
    (root / "README.md").write_text(readme, encoding="utf-8")

    print(f"Done. Output written to: {root}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        sys.exit(2)
