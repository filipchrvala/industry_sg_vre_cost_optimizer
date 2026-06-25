from pathlib import Path

p = Path(__file__).resolve().parents[1] / "config.toml"
text = (
    "[repository]\n"
    'REGISTRY_NAME = "filipchrvala"\n'
    'REPOSITORY_NAME = "industry_sg_vre_cost_optimizer"\n'
    'REPOSITORY_LABEL = "Industry SG VRE Cost Optimizer"\n'
    'VERSION = "0.1.36"\n'
)
p.write_bytes(text.encode("utf-8"))
b = p.read_bytes()
assert not b.startswith(b"\xef\xbb\xbf"), "BOM still present"
print("config.toml written without BOM")
