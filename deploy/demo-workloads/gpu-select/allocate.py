"""Allocate ~N GB of GPU VRAM and hold it for a while.

Reads its target from ORCH_PARAM_TARGET_GB (projected into the container's
env by the agent per internal/agent/agent.go's buildEnv). Exits 0 on success
(VRAM held for HOLD_SECONDS), non-zero if the allocation itself fails -- a
placement bug shows up as a failed task, not silent wrong behavior.
"""
import os
import sys
import time

import torch

TARGET_GB = float(os.environ.get("ORCH_PARAM_TARGET_GB", "1"))
HOLD_SECONDS = int(os.environ.get("ORCH_PARAM_HOLD_SECONDS", "25"))

def main():
    if not torch.cuda.is_available():
        print("no CUDA device visible in container", file=sys.stderr)
        sys.exit(1)

    device = torch.device("cuda:0")
    name = torch.cuda.get_device_name(device)
    total = torch.cuda.get_device_properties(device).total_memory / (1024**3)
    print(f"device={name} total_vram_gb={total:.1f} target_gb={TARGET_GB}")

    # float32 = 4 bytes/element; leave ~10% headroom for CUDA context overhead.
    elements = int(TARGET_GB * (1024**3) / 4 * 0.9)
    try:
        block = torch.empty(elements, dtype=torch.float32, device=device)
        block.fill_(1.0)
        torch.cuda.synchronize()
    except RuntimeError as e:
        print(f"allocation of {TARGET_GB}GB failed: {e}", file=sys.stderr)
        sys.exit(1)

    allocated = torch.cuda.memory_allocated(device) / (1024**3)
    print(f"holding {allocated:.2f}GB for {HOLD_SECONDS}s")
    time.sleep(HOLD_SECONDS)
    print("done")

if __name__ == "__main__":
    main()
