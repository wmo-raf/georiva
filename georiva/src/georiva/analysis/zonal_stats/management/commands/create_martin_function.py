"""
Management command: create_martin_function

Creates or replaces the PostgreSQL function source used by Martin tileserver
to serve BoundaryZonalStats as vector tiles.

The function joins BoundaryZonalStats with AdminBoundary.geom and returns
Mapbox Vector Tiles. Martin calls it via:

    GET /martin/boundary_stats/{z}/{x}/{y}
        ?variable=precipitation
        &time=2026-03-01T00:00:00Z
        &reference_time=2026-03-01T00:00:00Z  (optional, forecasts only)

Usage
-----
    python manage.py create_martin_function
    python manage.py create_martin_function --drop   # drop only
"""

from django.core.management.base import BaseCommand
from django.db import connection


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# Table names derived from Django app labels:
#   georiva_analysis_zonal_stats  → georiva_analysis_zonal_stats_boundaryzonalstats
#   adminboundarymanager          → adminboundarymanager_adminboundary
#   georivacore                   → georivacore_variable, georivacore_item

_CREATE_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION georiva_boundary_stats(
    z           integer,
    x           integer,
    y           integer,
    query_params json DEFAULT '{}'
)
RETURNS bytea
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $func$
DECLARE
    tile        bytea;
    var_slug    text        := query_params->>'variable';
    valid_time  timestamptz := NULLIF(query_params->>'time', '')::timestamptz;
    ref_time    timestamptz := NULLIF(query_params->>'reference_time', '')::timestamptz;
    admin_level integer     := (query_params->>'admin_level')::integer;
    tile_env    geometry    := ST_TileEnvelope(z, x, y);
    tile_bbox   geometry    := ST_Transform(tile_env, 4326);
BEGIN
    IF var_slug IS NULL OR var_slug = '' THEN
        RAISE EXCEPTION 'variable query parameter is required';
    END IF;

    IF valid_time IS NULL THEN
        RAISE EXCEPTION 'time query parameter is required';
    END IF;

    IF admin_level IS NULL THEN
        RAISE EXCEPTION 'admin_level query parameter is required';
    END IF;

    SELECT ST_AsMVT(tile_data.*, 'boundary_stats', 4096, 'geom')
    INTO tile
    FROM (
        SELECT
            b.id                AS boundary_id,
            b.level,
            b.gid_0,
            b.gid_1,
            b.gid_2,
            b.name_0,
            b.name_1,
            b.name_2,
            b.name_3,
            s.mean,
            s.min,
            s.max,
            s.sum,
            s.std,
            s.count,
            s.time              AS valid_time,
            i.reference_time,
            ST_AsMVTGeom(
                ST_Transform(b.geom, 3857),
                tile_env,
                4096,
                256,
                true
            )                   AS geom
        FROM adminboundarymanager_adminboundary b
        INNER JOIN georiva_analysis_zonal_stats_boundaryzonalstats s
            ON s.boundary_id = b.id
        INNER JOIN georivacore_item i
            ON i.id = s.item_id
        INNER JOIN georivacore_variable v
            ON v.id = s.variable_id
        WHERE b.geom && tile_bbox
          AND b.level = admin_level
          AND v.slug = var_slug
          AND s.time = valid_time
          AND (
                i.reference_time = COALESCE(
                    ref_time,
                    (
                        SELECT MAX(i2.reference_time)
                        FROM georiva_analysis_zonal_stats_boundaryzonalstats s2
                        INNER JOIN georivacore_item i2
                            ON i2.id = s2.item_id
                        INNER JOIN adminboundarymanager_adminboundary b2
                            ON b2.id = s2.boundary_id
                        WHERE s2.variable_id = s.variable_id
                          AND s2.time = valid_time
                          AND b2.level = admin_level
                          AND i2.reference_time IS NOT NULL
                    )
                )
                OR (
                    ref_time IS NULL
                    AND i.reference_time IS NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM georiva_analysis_zonal_stats_boundaryzonalstats s3
                        INNER JOIN georivacore_item i3
                            ON i3.id = s3.item_id
                        INNER JOIN adminboundarymanager_adminboundary b3
                            ON b3.id = s3.boundary_id
                        WHERE s3.variable_id = s.variable_id
                          AND s3.time = valid_time
                          AND b3.level = admin_level
                          AND i3.reference_time IS NOT NULL
                    )
                )
          )
    ) tile_data
    WHERE tile_data.geom IS NOT NULL;

    RETURN COALESCE(tile, ''::bytea);
END;
$func$;
"""

_DROP_FUNCTION_SQL = """
DROP FUNCTION IF EXISTS georiva_boundary_stats(integer, integer, integer, json);
"""

_COMMENT_SQL = """
DO $do$
BEGIN
    EXECUTE 'COMMENT ON FUNCTION georiva_boundary_stats(integer, integer, integer, json) IS $tj$' || $$
    {
        "description": "Boundary zonal statistics as vector tiles. Required query parameters: variable (string), time (ISO 8601 datetime), admin_level (integer). Optional query parameter: reference_time (ISO 8601 datetime). If reference_time is omitted and matching rows have reference_time values, the latest available reference_time is used.",
        "attribution": "GeoRiva",
        "version": "1.0.0",
        "parameters": {
            "variable": {
                "type": "string",
                "required": true,
                "description": "Variable slug, for example precipitation."
            },
            "time": {
                "type": "string",
                "format": "date-time",
                "required": true,
                "description": "Valid time in ISO 8601 format."
            },
            "admin_level": {
                "type": "integer",
                "required": true,
                "description": "Administrative boundary level to query."
            },
            "reference_time": {
                "type": "string",
                "format": "date-time",
                "required": false,
                "description": "Reference time in ISO 8601 format. If omitted, the latest available matching reference_time is used when present."
            }
        },
        "vector_layers": [
            {
                "id": "boundary_stats",
                "description": "Zonal statistics per admin boundary",
                "fields": {
                    "boundary_id": "Number",
                    "level": "Number",
                    "gid_0": "String",
                    "gid_1": "String",
                    "gid_2": "String",
                    "name_0": "String",
                    "name_1": "String",
                    "name_2": "String",
                    "name_3": "String",
                    "mean": "Number",
                    "min": "Number",
                    "max": "Number",
                    "sum": "Number",
                    "std": "Number",
                    "count": "Number",
                    "valid_time": "String",
                    "reference_time": "String"
                }
            }
        ]
    }
    $$::json || '$tj$';
END
$do$;
"""


class Command(BaseCommand):
    help = (
        "Create or replace the georiva_boundary_stats PostgreSQL function "
        "used by Martin tileserver as a vector tile function source."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--drop",
            action="store_true",
            help="Drop the function without recreating it.",
        )

    def handle(self, *args, **options):
        with connection.cursor() as cursor:

            self.stdout.write("Dropping existing function (if any)…")
            cursor.execute(_DROP_FUNCTION_SQL)

            if options["drop"]:
                self.stdout.write(self.style.SUCCESS("Function dropped."))
                return

            self.stdout.write("Creating georiva_boundary_stats…")
            cursor.execute(_CREATE_FUNCTION_SQL)

            self.stdout.write("Adding function comment…")
            cursor.execute(_COMMENT_SQL)

        self.stdout.write(self.style.SUCCESS(
            "\nFunction created. Martin tile URL:\n"
            "  /martin/boundary_stats/{z}/{x}/{y}"
            "?variable=<slug>&time=<iso>&reference_time=<iso>\n\n"
            "Reload Martin to pick up the new function source:\n"
            "  docker compose restart martin\n"
            "  # or POST http://martin:3000/reload"
        ))