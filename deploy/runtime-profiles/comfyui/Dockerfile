# syntax=docker/dockerfile:1.19

# Model-agnostic Cloud Offload ComfyUI runner.
#
# Pinned ComfyUI, including native 3D paint nodes, the CloudPartition bridge,
# and a baked capability manifest. Model weights are staged for the submitted
# graph rather than embedded in the image.
ARG PYTORCH_RUNTIME_IMAGE=pytorch/pytorch@sha256:77f17f843507062875ce8be2a6f76aa6aa3df7f9ef1e31d9d7432f4b0f563dee
FROM ${PYTORCH_RUNTIME_IMAGE} AS pytorch-runtime

# Build-time installation only: no package installation on a rented worker.
# A previously verified runtime can be supplied to reuse these immutable bytes.
RUN python -c "import torch, torchvision, torchaudio; assert (torch.__version__, torchvision.__version__, torchaudio.__version__) == ('2.11.0+cu128', '0.26.0+cu128', '2.11.0+cu128')" \
    || python -m pip install --no-cache-dir \
        torch==2.11.0+cu128 torchvision==0.26.0+cu128 torchaudio==2.11.0+cu128 \
        --index-url https://download.pytorch.org/whl/cu128

# A monolithic Conda tree requires one long serial download before a rented
# worker can start. Repack its largest libraries into independent layers.
# Container runtimes can pull these layers in parallel.
FROM ubuntu:22.04@sha256:0e0a0fc6d18feda9db1590da249ac93e8d5abfea8f4c3c0c849ce512b5ef8982

COPY --from=pytorch-runtime \
    --exclude=lib/python3.11/site-packages/torch \
    --exclude=lib/python3.11/site-packages/nvidia \
    /opt/conda/ /opt/conda/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/torch/ /opt/conda/lib/python3.11/site-packages/torch/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/__init__.py /opt/conda/lib/python3.11/site-packages/nvidia/__init__.py
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cublas/ /opt/conda/lib/python3.11/site-packages/nvidia/cublas/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cuda_cupti/ /opt/conda/lib/python3.11/site-packages/nvidia/cuda_cupti/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cuda_nvrtc/ /opt/conda/lib/python3.11/site-packages/nvidia/cuda_nvrtc/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cuda_runtime/ /opt/conda/lib/python3.11/site-packages/nvidia/cuda_runtime/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cudnn/ /opt/conda/lib/python3.11/site-packages/nvidia/cudnn/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cufft/ /opt/conda/lib/python3.11/site-packages/nvidia/cufft/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/curand/ /opt/conda/lib/python3.11/site-packages/nvidia/curand/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cusolver/ /opt/conda/lib/python3.11/site-packages/nvidia/cusolver/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cusparse/ /opt/conda/lib/python3.11/site-packages/nvidia/cusparse/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/nccl/ /opt/conda/lib/python3.11/site-packages/nvidia/nccl/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/nvjitlink/ /opt/conda/lib/python3.11/site-packages/nvidia/nvjitlink/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/nvtx/ /opt/conda/lib/python3.11/site-packages/nvidia/nvtx/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cufile/ /opt/conda/lib/python3.11/site-packages/nvidia/cufile/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cusparselt/ /opt/conda/lib/python3.11/site-packages/nvidia/cusparselt/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/nvshmem/ /opt/conda/lib/python3.11/site-packages/nvidia/nvshmem/

ARG COMFYUI_REPOSITORY=https://github.com/jethac/ComfyUI.git
ARG COMFYUI_REVISION=84ba85773925f071c516f0208184773802b4d44a

LABEL org.opencontainers.image.title="Cloud Offload ComfyUI runner"
LABEL ai.cloud-offload.comfyui-source-revision="${COMFYUI_REVISION}"

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PATH=/opt/conda/bin:/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    LD_LIBRARY_PATH=/usr/local/nvidia/lib:/usr/local/nvidia/lib64 \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility \
    NVIDIA_VISIBLE_DEVICES=all \
    PYTORCH_VERSION=2.11.0 \
    CLOUD_OFFLOAD_WORKER_MANIFEST=/opt/cloud-offload/runtime-profile.json \
    CLOUD_OFFLOAD_COMFYUI_URL=http://127.0.0.1:8188 \
    CLOUD_OFFLOAD_COMFYUI_ROOT=/opt/ComfyUI \
    COMFY_PARTITION_ROOT=/opt/cloud-offload/partitions \
    HF_HUB_ENABLE_HF_TRANSFER=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        git \
        libgl1 \
        libglib2.0-0 \
        libjpeg-dev \
        libopengl0 \
        libpng-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --filter=blob:none "${COMFYUI_REPOSITORY}" /opt/ComfyUI \
    && git -C /opt/ComfyUI checkout --detach "${COMFYUI_REVISION}"

COPY deploy/runtime-profiles/comfyui/runtime-constraints.txt /opt/cloud-offload/runtime-constraints.txt
RUN python -m pip install --no-cache-dir -r /opt/ComfyUI/requirements.txt \
    -c /opt/cloud-offload/runtime-constraints.txt

COPY . /opt/cloud-offload/source
COPY deploy/runtime-profiles/comfyui/runtime-profile.json /opt/cloud-offload/runtime-profile.json
COPY deploy/runtime-profiles/comfyui/entrypoint.sh /opt/cloud-offload/entrypoint.sh
COPY deploy/runtime-profiles/comfyui/ComfyUI-Cloud-Offload-Runtime /opt/ComfyUI/custom_nodes/ComfyUI-Cloud-Offload-Runtime

RUN python -m pip install --no-cache-dir "/opt/cloud-offload/source[cloud]" \
    -c /opt/cloud-offload/runtime-constraints.txt \
    && mkdir -p /opt/cloud-offload/partitions \
    && sed -i 's/\r$//' /opt/cloud-offload/entrypoint.sh \
    && chmod +x /opt/cloud-offload/entrypoint.sh \
    && python -c "import cloud_offload; print('Cloud Offload runner import OK')"

# Revision metadata belongs after the expensive immutable runtime layers. A new
# source revision must not force apt, ComfyUI, and its Python dependencies to be
# rebuilt from scratch.
ARG CLOUD_OFFLOAD_SOURCE_REVISION=working-tree
LABEL org.opencontainers.image.revision="${CLOUD_OFFLOAD_SOURCE_REVISION}"

WORKDIR /opt/ComfyUI
ENTRYPOINT ["/opt/cloud-offload/entrypoint.sh"]
