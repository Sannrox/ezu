#!/usr/bin/env python3
"""Generate and check the Tenkai compatibility profile ezu publishes.

    scripts/tenkai-profile.py --check
    scripts/tenkai-profile.py --check --file PATH
    scripts/tenkai-profile.py --print-profile
    scripts/tenkai-profile.py --print-contract
    scripts/tenkai-profile.py --print-fields
    scripts/tenkai-profile.py --self-test

Contract ids: `ezu.query` (spatial Query RPC) and `ezu.events` (FenceEvent
stream). Versions are derived from those proto surfaces. `--check` refuses a
product file that does not end with the generated profile v2 block.
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import re
import sys

QUERY_ID = "ezu.query"
EVENTS_ID = "ezu.events"
PROFILE_VERSION = 2
MAX_EVIDENCE_AGE_MS = 86_400_000
COMPONENT = "ezu"
ROOT = pathlib.Path(__file__).resolve().parent.parent
PROTO = ROOT / "proto" / "ezu" / "v1" / "ezu.proto"
CARGO = ROOT / "Cargo.toml"
PRODUCT = ROOT / "deploy" / "tenkai.toml"
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")

# Semantic versions of the query surface. Adding an optional field is a
# minor; removing, renaming, or changing meaning is a major.
QUERY_VERSIONS = {
    (
        ("collection", "filters", "limit", "cursor", "label_filter"),
        ("bbox", "radius", "polygon", "nearest"),
        ("entities", "next_cursor"),
        ("collection", "id", "geometry", "ts_ms", "speed", "heading", "ttl_ms", "fields", "labels"),
        ("field",),
        ("equals", "range"),
        ("readable",),
    ): "0.1.0",
}

# Semantic versions of the fence-event stream. Resume ids, kinds, and
# event fields are the surface dependents call.
EVENTS_VERSIONS = {
    (
        (
            "lsn",
            "index",
            "fence",
            "collection",
            "entity",
            "kind",
            "ts_ms",
            "position",
            "entity_labels",
            "fence_labels",
        ),
        (
            "EVENT_KIND_UNSPECIFIED",
            "EVENT_KIND_ENTER",
            "EVENT_KIND_EXIT",
            "EVENT_KIND_INSIDE",
            "EVENT_KIND_DWELL",
        ),
        ("after_lsn", "after_index", "limit", "fences", "label_filter"),
        ("after_lsn", "after_index", "fences", "label_filter"),
    ): "0.1.0",
}


def fail(message: str) -> None:
    raise SystemExit(f"tenkai-profile: {message}")


def proto_block(source: str, kind: str, name: str) -> str:
    marker = f"{kind} {name} {{"
    start = source.find(marker)
    if start < 0:
        fail(f"missing {kind} {name}")
    i = start + len(marker)
    depth = 1
    while i < len(source) and depth:
        char = source[i]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        i += 1
    return source[start + len(marker) : i - 1]


def strip_comments(body: str) -> str:
    lines = []
    for raw in body.splitlines():
        stripped = raw.split("//", 1)[0]
        if stripped.strip():
            lines.append(stripped)
    return "\n".join(lines)


def proto_oneofs(source: str, message: str) -> dict[str, tuple[str, ...]]:
    body = strip_comments(proto_block(source, "message", message))
    oneofs: dict[str, tuple[str, ...]] = {}
    for match in re.finditer(r"oneof\s+(\w+)\s*\{", body):
        name = match.group(1)
        start = match.end()
        depth = 1
        i = start
        while i < len(body) and depth:
            if body[i] == "{":
                depth += 1
            elif body[i] == "}":
                depth -= 1
            i += 1
        variants: list[str] = []
        for raw in body[start : i - 1].splitlines():
            line = raw.strip()
            if not line or "=" not in line:
                continue
            ident = line.partition("=")[0].strip().split()[-1]
            if IDENT.fullmatch(ident):
                variants.append(ident)
        if not variants:
            fail(f"oneof {message}.{name} has no variants")
        oneofs[name] = tuple(variants)
    return oneofs


def proto_message_fields(source: str, name: str) -> tuple[str, ...]:
    body = strip_comments(proto_block(source, "message", name))
    without_oneofs = re.sub(r"oneof\s+\w+\s*\{[^{}]*\}", "", body, flags=re.S)
    fields: list[str] = []
    for raw in without_oneofs.splitlines():
        line = raw.strip()
        if not line or "=" not in line:
            continue
        ident = line.partition("=")[0].strip().split()[-1]
        if IDENT.fullmatch(ident):
            fields.append(ident)
    if not fields:
        fail(f"message {name} has no fields")
    return tuple(fields)


def proto_enum_variants(source: str, name: str) -> tuple[str, ...]:
    body = strip_comments(proto_block(source, "enum", name))
    variants: list[str] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line or "=" not in line:
            continue
        ident = line.partition("=")[0].strip()
        if IDENT.fullmatch(ident):
            variants.append(ident)
    if not variants:
        fail(f"enum {name} has no variants")
    return tuple(variants)


def query_surface(source: str | None = None) -> tuple:
    text = PROTO.read_text() if source is None else source
    query_oneofs = proto_oneofs(text, "QueryRequest")
    filter_oneofs = proto_oneofs(text, "Filter")
    if "query" not in query_oneofs:
        fail("QueryRequest is missing oneof query")
    if "test" not in filter_oneofs:
        fail("Filter is missing oneof test")
    return (
        proto_message_fields(text, "QueryRequest"),
        query_oneofs["query"],
        proto_message_fields(text, "QueryResponse"),
        proto_message_fields(text, "Entity"),
        proto_message_fields(text, "Filter"),
        filter_oneofs["test"],
        proto_message_fields(text, "LabelFilter"),
    )


def events_surface(source: str | None = None) -> tuple:
    text = PROTO.read_text() if source is None else source
    return (
        proto_message_fields(text, "FenceEvent"),
        proto_enum_variants(text, "EventKind"),
        proto_message_fields(text, "ListEventsRequest"),
        proto_message_fields(text, "SubscribeRequest"),
    )


def lookup_version(table: dict, key: tuple, label: str) -> str:
    try:
        version = table[key]
    except KeyError:
        fail(f"unknown {label} surface {key!r}; record a contract version")
    if not SEMVER.fullmatch(version):
        fail(f"{label} contract version must be stable MAJOR.MINOR.PATCH: {version}")
    return version


def query_version(surface: tuple | None = None) -> str:
    return lookup_version(
        QUERY_VERSIONS,
        query_surface() if surface is None else surface,
        "query",
    )


def events_version(surface: tuple | None = None) -> str:
    return lookup_version(
        EVENTS_VERSIONS,
        events_surface() if surface is None else surface,
        "events",
    )


def crate_version() -> str:
    text = CARGO.read_text()
    match = re.search(
        r'(?ms)^\[package\][^\[]*?^version\s*=\s*"([^"]+)"',
        text,
    )
    if not match:
        fail("Cargo.toml has no [package] version")
    return match.group(1)


def product_version(body: str) -> str:
    match = re.search(
        r'(?ms)^\[product\].*?^version = "([^"]+)"',
        body,
    )
    if not match:
        fail("product file is missing [product].version")
    return match.group(1)


def pin_digest(version: str) -> str:
    # Version-bound pin so the committed product can be checked without a
    # built image. The family release manifest carries the image digest.
    return "sha256:" + hashlib.sha256(f"ezu@{version}".encode()).hexdigest()


def compatibility_profile(
    product_ver: str,
    query_ver: str,
    events_ver: str,
) -> str:
    pin = pin_digest(product_ver)
    return (
        "[compatibility]\n"
        f"version = {PROFILE_VERSION}\n"
        "# Evidence binds one release digest; one day is Tenkai's maximum.\n"
        f"max_evidence_age_ms = {MAX_EVIDENCE_AGE_MS}\n"
        "\n"
        f"[compatibility.components.{COMPONENT}.pin]\n"
        'kind = "digest"\n'
        f'value = "{pin}"\n'
        "\n"
        f"[compatibility.components.{COMPONENT}.provides]\n"
        f'"{QUERY_ID}" = "{query_ver}"\n'
        f'"{EVENTS_ID}" = "{events_ver}"\n'
    )


def contracts() -> tuple[str, str, str, str]:
    return QUERY_ID, query_version(), EVENTS_ID, events_version()


def check_tenkai_toml(
    body: str,
    proto_src: str | None = None,
) -> None:
    product_ver = product_version(body)
    crate_ver = crate_version()
    if product_ver != crate_ver:
        fail(f"product.version {product_ver} must match crate version {crate_ver}")
    if ":latest" in body or "/latest" in body:
        fail("product file must not use a latest tag")
    image = f"ghcr.io/sannrox/ezu:{product_ver}"
    if image not in body:
        fail(f"product file must install {image}")
    if "test -f ezu.env" not in body:
        fail("product file must require operator-supplied ezu.env")
    if "--env-file /work/" in body:
        fail("product --env-file is read on the host; do not use a container path")
    if "--env-file ezu.env" not in body and '--env-file "$PWD/ezu.env"' not in body:
        fail("product file must pass host-path --env-file ezu.env")
    if "/healthz" not in body:
        fail("product file must health-check /healthz")
    query_ver = query_version(query_surface(proto_src) if proto_src is not None else None)
    events_ver = events_version(events_surface(proto_src) if proto_src is not None else None)
    profile = compatibility_profile(product_ver, query_ver, events_ver)
    if not body.endswith(profile):
        fail("deploy/tenkai.toml must end with this compatibility profile:\n\n" + profile)


def generated_profile() -> str:
    body = PRODUCT.read_text()
    _, query_ver, _, events_ver = contracts()
    return compatibility_profile(product_version(body), query_ver, events_ver)


def self_test() -> None:
    proto_src = PROTO.read_text()
    query = query_surface(proto_src)
    events = events_surface(proto_src)
    query_ver = query_version(query)
    events_ver = events_version(events)
    body = PRODUCT.read_text()
    check_tenkai_toml(body, proto_src)
    profile = compatibility_profile(product_version(body), query_ver, events_ver)
    if f"version = {PROFILE_VERSION}\n" not in profile:
        fail("self-test: generated profile is not v2")
    if f'"{QUERY_ID}" = "{query_ver}"' not in profile:
        fail("self-test: generated profile missing the query contract")
    if f'"{EVENTS_ID}" = "{events_ver}"' not in profile:
        fail("self-test: generated profile missing the events contract")

    extra_query = (query[0] + ("extra",), *query[1:])
    try:
        query_version(extra_query)
    except SystemExit:
        pass
    else:
        fail("self-test: extra query field must be unknown")

    extra_events = (events[0] + ("extra",), *events[1:])
    try:
        events_version(extra_events)
    except SystemExit:
        pass
    else:
        fail("self-test: extra event field must be unknown")

    mutated = [
        body.replace(f'"{QUERY_ID}" = "{query_ver}"', f'"{QUERY_ID}" = "0.0.1"'),
        body.replace(f'"{EVENTS_ID}" = "{events_ver}"', f'"{EVENTS_ID}" = "0.0.1"'),
        body.replace(f'"{QUERY_ID}"', '"spatial.query"'),
        body.replace("version = 2", "version = 1", 1),
        body.replace("--env-file ezu.env", "--env-file /work/ezu.env"),
    ]
    for stale in mutated:
        if stale == body:
            fail("self-test: mutation did not change the product")
        try:
            check_tenkai_toml(stale, proto_src)
        except SystemExit:
            continue
        fail("self-test: stale product must be refused")


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--print-profile", action="store_true")
    mode.add_argument("--print-contract", action="store_true")
    mode.add_argument("--print-fields", action="store_true")
    mode.add_argument("--self-test", action="store_true")
    parser.add_argument("--file", type=pathlib.Path)
    args = parser.parse_args(argv)
    if args.self_test:
        if args.file:
            parser.error("--self-test does not take --file")
        self_test()
        _, query_ver, _, events_ver = contracts()
        print(f"tenkai-profile: {QUERY_ID} {query_ver} {EVENTS_ID} {events_ver} ok")
        return
    if args.print_fields:
        query = query_surface()
        events = events_surface()
        print("query")
        print("request " + " ".join(query[0]))
        print("variants " + " ".join(query[1]))
        print("response " + " ".join(query[2]))
        print("entity " + " ".join(query[3]))
        print("filter " + " ".join(query[4]))
        print("filter_test " + " ".join(query[5]))
        print("label_filter " + " ".join(query[6]))
        print("---")
        print("event " + " ".join(events[0]))
        print("kind " + " ".join(events[1]))
        print("list " + " ".join(events[2]))
        print("subscribe " + " ".join(events[3]))
        return
    if args.print_contract:
        _, query_ver, _, events_ver = contracts()
        print(f"{QUERY_ID} {query_ver}")
        print(f"{EVENTS_ID} {events_ver}")
        return
    if args.print_profile:
        sys.stdout.write(generated_profile())
        return
    path = args.file if args.file is not None else PRODUCT
    check_tenkai_toml(path.read_text())
    _, query_ver, _, events_ver = contracts()
    print(f"tenkai-profile: {QUERY_ID} {query_ver} {EVENTS_ID} {events_ver} ok")


if __name__ == "__main__":
    main(sys.argv[1:])
