"""
GeoRiva STAC API OpenAPI Schema

Generates OpenAPI 3.0 specification for the STAC API.
Access via: GET /stac/api
"""

from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response


def get_openapi_schema(request: Request) -> dict:
    """Generate OpenAPI 3.0 schema for STAC API."""
    
    base_url = request.build_absolute_uri('/api/stac/')
    
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "GeoRiva STAC API",
            "description": "SpatioTemporal Asset Catalog API for Earth observation and meteorological data",
            "version": "1.0.0",
            "contact": {
                "name": "GeoRiva Support",
            },
            "license": {
                "name": "MIT",
            },
        },
        "servers": [
            {"url": base_url, "description": "GeoRiva STAC API"}
        ],
        "tags": [
            {"name": "Core", "description": "STAC API core endpoints"},
            {"name": "Collections", "description": "Collection management"},
            {"name": "Items", "description": "Item access (OGC API Features)"},
            {"name": "Search", "description": "Cross-collection search"},
        ],
        "paths": {
            "/": {
                "get": {
                    "tags": ["Core"],
                    "summary": "Landing Page",
                    "description": "Returns the root STAC Catalog",
                    "operationId": "getLandingPage",
                    "responses": {
                        "200": {
                            "description": "STAC Catalog",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Catalog"}
                                }
                            }
                        }
                    }
                }
            },
            "/conformance": {
                "get": {
                    "tags": ["Core"],
                    "summary": "Conformance Classes",
                    "operationId": "getConformance",
                    "responses": {
                        "200": {
                            "description": "Conformance declaration",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Conformance"}
                                }
                            }
                        }
                    }
                }
            },
            "/collections": {
                "get": {
                    "tags": ["Collections"],
                    "summary": "List Collections",
                    "operationId": "getCollections",
                    "responses": {
                        "200": {
                            "description": "List of STAC Collections",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Collections"}
                                }
                            }
                        }
                    }
                }
            },
            "/collections/{catalogId}/{collectionId}": {
                "get": {
                    "tags": ["Collections"],
                    "summary": "Get Collection",
                    "operationId": "getCollection",
                    "parameters": [
                        {"$ref": "#/components/parameters/catalogId"},
                        {"$ref": "#/components/parameters/collectionId"},
                    ],
                    "responses": {
                        "200": {
                            "description": "STAC Collection",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Collection"}
                                }
                            }
                        },
                        "404": {"description": "Collection not found"}
                    }
                }
            },
            "/collections/{catalogId}/{collectionId}/items": {
                "get": {
                    "tags": ["Items"],
                    "summary": "List Items",
                    "operationId": "getItems",
                    "parameters": [
                        {"$ref": "#/components/parameters/catalogId"},
                        {"$ref": "#/components/parameters/collectionId"},
                        {"$ref": "#/components/parameters/limit"},
                        {"$ref": "#/components/parameters/datetime"},
                        {"$ref": "#/components/parameters/bbox"},
                        {"$ref": "#/components/parameters/token"},
                    ],
                    "responses": {
                        "200": {
                            "description": "STAC ItemCollection (GeoJSON FeatureCollection)",
                            "content": {
                                "application/geo+json": {
                                    "schema": {"$ref": "#/components/schemas/ItemCollection"}
                                }
                            }
                        }
                    }
                }
            },
            "/collections/{catalogId}/{collectionId}/items/{itemId}": {
                "get": {
                    "tags": ["Items"],
                    "summary": "Get Item",
                    "operationId": "getItem",
                    "parameters": [
                        {"$ref": "#/components/parameters/catalogId"},
                        {"$ref": "#/components/parameters/collectionId"},
                        {"$ref": "#/components/parameters/itemId"},
                    ],
                    "responses": {
                        "200": {
                            "description": "STAC Item (GeoJSON Feature)",
                            "content": {
                                "application/geo+json": {
                                    "schema": {"$ref": "#/components/schemas/Item"}
                                }
                            }
                        },
                        "404": {"description": "Item not found"}
                    }
                }
            },
            "/search": {
                "get": {
                    "tags": ["Search"],
                    "summary": "Search Items (GET)",
                    "operationId": "searchItemsGet",
                    "parameters": [
                        {"$ref": "#/components/parameters/collections"},
                        {"$ref": "#/components/parameters/limit"},
                        {"$ref": "#/components/parameters/datetime"},
                        {"$ref": "#/components/parameters/bbox"},
                        {"$ref": "#/components/parameters/token"},
                    ],
                    "responses": {
                        "200": {
                            "description": "Search results",
                            "content": {
                                "application/geo+json": {
                                    "schema": {"$ref": "#/components/schemas/ItemCollection"}
                                }
                            }
                        }
                    }
                },
                "post": {
                    "tags": ["Search"],
                    "summary": "Search Items (POST)",
                    "operationId": "searchItemsPost",
                    "requestBody": {
                        "description": "Search parameters",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/SearchBody"}
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Search results",
                            "content": {
                                "application/geo+json": {
                                    "schema": {"$ref": "#/components/schemas/ItemCollection"}
                                }
                            }
                        }
                    }
                }
            },
        },
        "components": {
            "parameters": {
                "catalogId": {
                    "name": "catalogId",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string"},
                    "description": "Catalog identifier (slug)",
                },
                "collectionId": {
                    "name": "collectionId",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string"},
                    "description": "Collection identifier (slug)",
                },
                "itemId": {
                    "name": "itemId",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string"},
                    "description": "Item identifier",
                },
                "limit": {
                    "name": "limit",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "integer", "default": 100, "maximum": 1000},
                    "description": "Maximum number of items to return",
                },
                "datetime": {
                    "name": "datetime",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string"},
                    "description": "Datetime filter (RFC 3339). Single datetime or range: start/end",
                    "example": "2024-01-01T00:00:00Z/2024-01-31T23:59:59Z",
                },
                "bbox": {
                    "name": "bbox",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string"},
                    "description": "Bounding box filter: west,south,east,north",
                    "example": "-10,35,5,45",
                },
                "token": {
                    "name": "token",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string"},
                    "description": "Pagination token (datetime of last item)",
                },
                "collections": {
                    "name": "collections",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "array", "items": {"type": "string"}},
                    "description": "Collection IDs to search",
                    "style": "form",
                    "explode": True,
                },
            },
            "schemas": {
                "Catalog": {
                    "type": "object",
                    "required": ["type", "stac_version", "id", "description", "links"],
                    "properties": {
                        "type": {"type": "string", "enum": ["Catalog"]},
                        "stac_version": {"type": "string"},
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "conformsTo": {"type": "array", "items": {"type": "string"}},
                        "links": {"type": "array", "items": {"$ref": "#/components/schemas/Link"}},
                    },
                },
                "Conformance": {
                    "type": "object",
                    "properties": {
                        "conformsTo": {"type": "array", "items": {"type": "string"}},
                    },
                },
                "Collections": {
                    "type": "object",
                    "properties": {
                        "collections": {"type": "array", "items": {"$ref": "#/components/schemas/Collection"}},
                        "links": {"type": "array", "items": {"$ref": "#/components/schemas/Link"}},
                    },
                },
                "Collection": {
                    "type": "object",
                    "required": ["type", "stac_version", "id", "description", "license", "extent", "links"],
                    "properties": {
                        "type": {"type": "string", "enum": ["Collection"]},
                        "stac_version": {"type": "string"},
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "license": {"type": "string"},
                        "extent": {"$ref": "#/components/schemas/Extent"},
                        "summaries": {"type": "object"},
                        "links": {"type": "array", "items": {"$ref": "#/components/schemas/Link"}},
                        "providers": {"type": "array", "items": {"$ref": "#/components/schemas/Provider"}},
                    },
                },
                "Extent": {
                    "type": "object",
                    "properties": {
                        "spatial": {
                            "type": "object",
                            "properties": {
                                "bbox": {"type": "array", "items": {"type": "array", "items": {"type": "number"}}},
                            },
                        },
                        "temporal": {
                            "type": "object",
                            "properties": {
                                "interval": {"type": "array",
                                             "items": {"type": "array", "items": {"type": "string", "nullable": True}}},
                            },
                        },
                    },
                },
                "ItemCollection": {
                    "type": "object",
                    "required": ["type", "features"],
                    "properties": {
                        "type": {"type": "string", "enum": ["FeatureCollection"]},
                        "features": {"type": "array", "items": {"$ref": "#/components/schemas/Item"}},
                        "links": {"type": "array", "items": {"$ref": "#/components/schemas/Link"}},
                        "numberMatched": {"type": "integer"},
                        "numberReturned": {"type": "integer"},
                        "context": {
                            "type": "object",
                            "properties": {
                                "returned": {"type": "integer"},
                                "matched": {"type": "integer"},
                                "limit": {"type": "integer"},
                            },
                        },
                    },
                },
                "Item": {
                    "type": "object",
                    "required": ["type", "stac_version", "id", "geometry", "bbox", "properties", "links", "assets"],
                    "properties": {
                        "type": {"type": "string", "enum": ["Feature"]},
                        "stac_version": {"type": "string"},
                        "stac_extensions": {"type": "array", "items": {"type": "string"}},
                        "id": {"type": "string"},
                        "geometry": {"type": "object"},
                        "bbox": {"type": "array", "items": {"type": "number"}},
                        "properties": {"type": "object"},
                        "links": {"type": "array", "items": {"$ref": "#/components/schemas/Link"}},
                        "assets": {"type": "object", "additionalProperties": {"$ref": "#/components/schemas/Asset"}},
                        "collection": {"type": "string"},
                    },
                },
                "Asset": {
                    "type": "object",
                    "required": ["href"],
                    "properties": {
                        "href": {"type": "string", "format": "uri"},
                        "type": {"type": "string"},
                        "title": {"type": "string"},
                        "roles": {"type": "array", "items": {"type": "string"}},
                        "file:size": {"type": "integer"},
                        "raster:bands": {"type": "array", "items": {"type": "object"}},
                    },
                },
                "Link": {
                    "type": "object",
                    "required": ["rel", "href"],
                    "properties": {
                        "rel": {"type": "string"},
                        "href": {"type": "string", "format": "uri"},
                        "type": {"type": "string"},
                        "title": {"type": "string"},
                        "method": {"type": "string", "enum": ["GET", "POST"]},
                    },
                },
                "Provider": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "url": {"type": "string", "format": "uri"},
                        "roles": {"type": "array", "items": {"type": "string"}},
                    },
                },
                "SearchBody": {
                    "type": "object",
                    "properties": {
                        "collections": {"type": "array", "items": {"type": "string"}},
                        "ids": {"type": "array", "items": {"type": "string"}},
                        "bbox": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 6},
                        "datetime": {"type": "string"},
                        "limit": {"type": "integer", "default": 100, "maximum": 1000},
                        "intersects": {"type": "object", "description": "GeoJSON Geometry"},
                    },
                },
            },
        },
    }


@api_view(['GET'])
def openapi_view(request: Request) -> Response:
    """Serve OpenAPI schema."""
    schema = get_openapi_schema(request)
    return Response(schema, content_type='application/vnd.oai.openapi+json;version=3.0')
