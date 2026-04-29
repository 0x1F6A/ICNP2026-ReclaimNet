# VS Code + Colab A100 Workflow

## Recommended Setup

Use VS Code locally as the editor and Colab as the A100 execution runtime:

1. Edit code in local VS Code.
2. Commit and push to GitHub.
3. Open `colab/UniGPU_A100_Setup.ipynb` in Colab.
4. Select GPU runtime and A100 if available.
5. Run the setup cells.
6. Pull latest code in Colab before each experiment run.

This avoids using SSH or remote-desktop control into a managed Colab runtime.

## Local VS Code Commands

For the `Code` repository:

```bash
cd /Users/wenyang/Desktop/ICNP2026/UniGPU/Code
git status
git add .
git commit -m "Update experiment"
git push origin main
```

## Colab Commands

Inside the Colab notebook:

```bash
cd /content/ICNP2026-ReclaimNet
git pull --ff-only origin main
```

Then run the experiment command.

## GPU Check

```bash
nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader
```

Expected A100 output contains `A100`. If Colab assigns a different GPU, reconnect or select A100 if your plan supports it.

## Why Not VS Code Remote-SSH Directly

Colab's FAQ states that managed runtimes prioritize interactive notebook programming and lists remote-control workflows such as SSH shells and remote desktops as restricted in common managed-runtime cases. Because of that, this repository uses a notebook-driven execution workflow instead of SSH tunneling into Colab.

If you need a true VS Code Remote-SSH machine with stable A100 access, use a VM you control, Colab Enterprise / GCP Marketplace, or another managed GPU host that explicitly supports SSH. VS Code Remote-SSH is a good fit for those machines.

