# vines-worker-milvus

## 安装依赖

### macos

```text
brew install cmake
brew install mupdf swig freetype
```

### Linux 环境

#### 安装 GPU 版本的 pytorch

```
pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

#### 下载模型（可选）

如果在无法连接 huggingface

```
mkdir models
cd models
git lfs install
git clone https://huggingface.co/BAAI/bge-base-zh-v1.5
```