#!/usr/bin/env python3
import matplotlib.pyplot as plt
from pathlib import Path
from PIL import Image

# Path to your existing charts
src_dir = Path("reports/metrics")
charts = [
    "p95_latency_by_op.png",
    "mbps_by_provider.png",
    "error_rate_by_provider_op.png",
    "error_rate_heatmap.png"
]

# Load images
images = [Image.open(src_dir / c) for c in charts if (src_dir / c).exists()]

# Compute grid (2x2)
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
axes = axes.flatten()

for ax, img, title in zip(axes, images, charts):
    ax.imshow(img)
    ax.axis("off")
    ax.set_title(title.replace("_", " ").replace(".png", ""), fontsize=10)

plt.tight_layout()
out_path = src_dir / "combined_dashboard.png"
plt.savefig(out_path, dpi=150)
print(f"✅ Combined chart saved to {out_path}")
