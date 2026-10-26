# PyInstaller 的入口桩（build.sh 用）。等价于 console_scripts 的
# rl-limiter 命令——PyInstaller 需要一个真实的脚本文件做入口。
from rl_limiter.__main__ import main

if __name__ == "__main__":
    main()
