"""Install source plugins onto an image-only server (ADR 0035).

Pure Python — no Django — so it runs as a one-shot before any app container
starts, and is tested without an app registry.
"""
