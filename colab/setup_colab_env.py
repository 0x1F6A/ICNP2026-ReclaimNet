import os
import pathlib
import subprocess
import sys


REPO_URL = os.environ.get("UNIGPU_REPO_URL", "https://github.com/0x1F6A/ICNP2026-ReclaimNet.git")
BRANCH = os.environ.get("UNIGPU_BRANCH", "main")
WORKDIR = pathlib.Path(os.environ.get("UNIGPU_WORKDIR", "/content/ICNP2026-ReclaimNet"))


def run(cmd, cwd=None, check=True):
    print(f"$ {cmd}")
    return subprocess.run(cmd, shell=True, cwd=cwd, check=check)


def check_gpu():
    result = subprocess.run(
        "nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader",
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    print(result.stdout.strip())
    if result.returncode != 0:
        raise RuntimeError("No NVIDIA GPU is visible. In Colab, switch Runtime -> Change runtime type -> GPU.")
    if "A100" not in result.stdout:
        print("WARNING: The assigned GPU is not A100. Reconnect or select A100 if your Colab plan supports it.")


def clone_or_pull():
    if WORKDIR.exists() and (WORKDIR / ".git").exists():
        run(f"git fetch origin {BRANCH}", cwd=WORKDIR)
        run(f"git checkout {BRANCH}", cwd=WORKDIR)
        run(f"git pull --ff-only origin {BRANCH}", cwd=WORKDIR)
    else:
        WORKDIR.parent.mkdir(parents=True, exist_ok=True)
        run(f"git clone --branch {BRANCH} {REPO_URL} {WORKDIR}")


def install_python_tools():
    run(f"{sys.executable} -m pip install -U pip setuptools wheel")
    run(f"{sys.executable} -m pip install -U pytest psutil numpy pandas matplotlib")
    if (WORKDIR / "pyproject.toml").exists():
        run(f"{sys.executable} -m pip install -e .[agent]", cwd=WORKDIR)


def smoke_check():
    run(f"{sys.executable} - <<'PY'\nimport sys\nprint(sys.version)\ntry:\n    import torch\n    print('torch:', torch.__version__)\n    print('cuda:', torch.cuda.is_available())\n    if torch.cuda.is_available():\n        print('gpu:', torch.cuda.get_device_name(0))\nexcept Exception as exc:\n    print('torch check skipped:', exc)\nPY")
    if (WORKDIR / "tests").exists():
        run(f"{sys.executable} -m pytest -q tests", cwd=WORKDIR, check=False)


def main():
    check_gpu()
    clone_or_pull()
    install_python_tools()
    smoke_check()
    print(f"Ready: {WORKDIR}")


if __name__ == "__main__":
    main()

