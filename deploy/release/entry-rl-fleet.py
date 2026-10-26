# PyInstaller 的入口桩（build.sh 用）。等价于 console_scripts 的
# rl-fleet 命令。
from rl_limiter.fleetmain import main

if __name__ == "__main__":
    main()
