"""Hardcoded OneData credentials for Domino dev (UC3.2 Cost Optimizer).

Domino workflow secrets can stay empty — pieces read these defaults when OneData
paths are used. Remove or externalize before a public release.
"""

DEFAULT_ONEZONE_HOST = "data.spice-platform.eu"
DEFAULT_INPUT_DIR = "onedata:///FilipsSpace/cost_optimizer/inputs"
DEFAULT_OUTPUT_DIR = "onedata:///FilipsSpace/cost_optimizer/outputs"
DEFAULT_ONEDATA_TOKEN = (
    "MDAyNGxvY2F00aW9uIGRhdGEuc3BpY2UtcGxhdGZvcm00uZXUKMDA2YmlkZW500aWZpZXIgMi9ubWQvdXNyLWMzZjNkNDc3NjE3MzBiMjk4OWZkNzEyZTJmODc4YWJiY2hlMTdiL2FjdC8yODNjMzExMmU1NmVhNjgxMWUxOGUyYWRkZTdiYjJjNGNoOGNjZgowMDJmc2lnbmF00dXJlIL1Iu96JPccUim5K102wEE01WY00yzaw3E5uAt88MXsxl02pCg"
)
