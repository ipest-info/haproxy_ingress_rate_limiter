#!/usr/bin/env bash
# 构建发布产物：单文件二进制 rl-limiter / rl-fleet（PyInstaller），连同
# 配置模板、安装脚本打成 tar.gz，并生成 .sha256 校验文件。
#
# 用法：
#   deploy/release/build.sh              # 版本取 pyproject.toml
#   VERSION=0.5.0 deploy/release/build.sh
#
# 产物（dist/ 下）：
#   rl-limiter-v<版本>-linux-<架构>.tar.gz          发布包
#   rl-limiter-v<版本>-linux-<架构>.tar.gz.sha256   校验
#
# 包内布局（解开即装，见 install.sh）：
#   bin/rl-limiter  bin/rl-fleet        单文件二进制（无需目标机装 Python）
#   config/*.yaml / *.env.example       配置模板（节点 + 总控）
#   install.sh                          安装脚本
#
# **glibc 兼容性**：PyInstaller 产物要求目标机 glibc ≥ 构建机 glibc。
# 正式发布请在 CI（manylinux2014 容器，glibc 2.17，覆盖 CentOS 7+）里
# 构建——见 .github/workflows/release.yml；本脚本在哪台机器上跑，产物
# 就只保证兼容那台机器同代及更新的系统。
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PY=${PYTHON:-python3}
VERSION=${VERSION:-$(sed -n 's/^version *= *"\(.*\)"/\1/p' "$ROOT/pyproject.toml")}
ARCH=$(uname -m)
NAME="rl-limiter-v${VERSION}-linux-${ARCH}"
DIST="$ROOT/dist"
WORK="$DIST/$NAME"

echo "==> 构建 $NAME（python=$($PY --version 2>&1)）"
rm -rf "$WORK" "$DIST/$NAME.tar.gz" "$DIST/$NAME.tar.gz.sha256" \
       "$ROOT/build" "$ROOT"/*.spec
mkdir -p "$WORK/bin" "$WORK/config"

# 本体 + PyInstaller 装进当前 Python 环境（幂等）。
"$PY" -m pip install --quiet "$ROOT" pyinstaller

# 两个二进制。要点：
#   --add-data   控制台静态页随包（webconsole/fleetweb 按 __file__ 相对
#                路径找 rl_limiter/static，PyInstaller 解包后路径一致）；
#   --copy-metadata  rl-limiter --version 取的是包元数据，不带上就是 dev。
common_flags=(--onefile --clean --noconfirm
              --distpath "$WORK/bin" --workpath "$ROOT/build"
              --specpath "$ROOT/build"
              --add-data "$ROOT/rl_limiter/static:rl_limiter/static"
              --copy-metadata rl-limiter)
"$PY" -m PyInstaller "${common_flags[@]}" --name rl-limiter \
      "$ROOT/deploy/release/entry-rl-limiter.py"
"$PY" -m PyInstaller "${common_flags[@]}" --name rl-fleet \
      "$ROOT/deploy/release/entry-rl-fleet.py"

# 冒烟：二进制必须能报出与构建版本一致的版本号。
got=$("$WORK/bin/rl-limiter" --version)
echo "==> 冒烟 rl-limiter --version → $got"
case "$got" in *"$VERSION"*) ;; *)
    echo "版本不一致：二进制报 '$got'，期望含 '$VERSION'" >&2; exit 1;;
esac

# 配置模板 + 安装脚本随包。
cp "$ROOT/deploy/config/limiter.example.yaml" \
   "$ROOT/deploy/config/fleet.example.yaml" \
   "$ROOT/deploy/config/rl-fleet.env.example" \
   "$ROOT/deploy/bare/rl-limiter.env.example" "$WORK/config/"
cp "$ROOT/deploy/release/install.sh" "$WORK/"
chmod +x "$WORK/install.sh"

tar -C "$DIST" -czf "$DIST/$NAME.tar.gz" "$NAME"
( cd "$DIST" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256" )
rm -rf "$WORK" "$ROOT/build"
echo "==> 完成："
ls -lh "$DIST/$NAME.tar.gz" "$DIST/$NAME.tar.gz.sha256"
