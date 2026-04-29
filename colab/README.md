# Colab A100 Experiment Workflow

This project uses a policy-safe Colab workflow:

1. Edit code locally in VS Code.
2. Push changes to GitHub.
3. Run the Colab notebook on an A100 runtime.
4. Pull the latest code inside Colab for each experiment iteration.

Direct VS Code Remote-SSH into a managed Colab runtime is intentionally not used here. Colab's FAQ lists remote-control workflows such as SSH shells and remote desktops as restricted for managed runtimes in common cases, and Colab resources are not guaranteed. Use Colab as the interactive notebook runtime and keep VS Code as the local editor.

## Files

- `UniGPU_A100_Setup.ipynb`: Open this in Colab, select an A100 GPU runtime, clone/pull the experiment repo, install dependencies, and run smoke checks.
- `setup_colab_env.py`: Script version of the setup logic if you prefer to paste a single cell into Colab.

## VS Code to Colab Iteration

From local VS Code:

```bash
cd /Users/wenyang/Desktop/ICNP2026/UniGPU/Code
git add README.md
git commit -m "Update experiment code"
git push origin main
```

In Colab:

```bash
cd /content/ICNP2026-ReclaimNet
git pull --ff-only origin main
```

Then rerun the experiment cell.

## A100 Selection

In Colab, use:

```text
Runtime -> Change runtime type -> Hardware accelerator: GPU
```

If your Colab plan exposes GPU selection, choose A100. The notebook checks `nvidia-smi` and reports the actual GPU assigned by Colab.

