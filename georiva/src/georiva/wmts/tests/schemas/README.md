# Vendored OGC schemas

The WMTS 1.0.0 capabilities schema and its full import closure (OWS 1.1.0,
GML 3.1.1 base, xlink/xml), mirrored verbatim from `schemas.opengis.net` and
`www.w3.org` on 2026-08-11, laid out as `<host>/<path>`. Vendored so the
schema-validation test in `test_schema_validity.py` runs offline; the resolver
there maps each absolute `schemaLocation` URL onto this directory.
