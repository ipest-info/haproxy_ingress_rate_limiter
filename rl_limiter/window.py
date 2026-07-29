# rl_limiter.window —— collector 的两个平滑原语（设计文档 §3.1）：
#   - SlidingWindow：10 秒滑动窗口均值的载体，对应"10 秒均值 ≤ 约定带宽"
#     的承诺口径，是快环限速决策的输入；
#   - Ewma：约 60 秒指数加权移动平均，是执行路径按挂载点加权分配的输入
#     （alpha = 2/(N+1)、N=60 由调用方传入，本模块不内置具体参数）。
# 两者都由 tick 协程独占使用；asyncio 单线程模型下天然无并发问题，因此
# 刻意不做任何锁保护。

from __future__ import annotations


class SlidingWindow:
    """固定容量的环形缓冲，保存最近 capacity 个样本。

    窗口填满之前，均值只在已推入的样本上计算——冷启动阶段不用零值凑满
    窗口，否则 mean10 会被人为压低，延迟快环的收紧动作。
    """

    __slots__ = ("_buf", "_next", "_count")

    def __init__(self, capacity: int):
        # capacity 必须为正；<= 0 的值被钳制为 1，保证 push/mean 永不因
        # 空缓冲崩溃——窗口是纯内部工具类，防御性钳制好过在热路径抛错。
        if capacity <= 0:
            capacity = 1
        self._buf = [0.0] * capacity
        self._next = 0   # 下一次写入的位置（环形推进）
        self._count = 0  # 已持有的有效样本数，最多为 len(_buf)

    def push(self, v: float) -> None:
        """追加一个样本；窗口已满时按环形位置覆盖最旧的样本。"""
        self._buf[self._next] = v
        self._next = (self._next + 1) % len(self._buf)
        if self._count < len(self._buf):
            self._count += 1

    @property
    def count(self) -> int:
        """当前持有的有效样本数。调用方据此判断窗口是否还是"空的"
        （空窗口的 mean() 返回 0，与"样本恰好都是 0"无法区分）。"""
        return self._count

    def mean(self) -> float:
        """返回当前持有样本的算术平均（空窗口返回 0）。

        为什么每次全量重算而不是维护滚动和（push 时加、evict 时减）：滚动
        和的浮点误差会随运行时间无界累积，rl-limiter 是常驻进程、每秒推一
        个样本，跑上数周后滚动和可能明显偏离真值；而窗口只有 10 格，O(n)
        重算的成本可以忽略，换来的是长期运行下的数值稳定。
        """
        if self._count == 0:
            return 0.0
        # 首次绕回之前有效样本是 _buf[0:count]；绕回之后整个缓冲都有效且
        # count == len(_buf)，因此统一按 _buf[0:count] 求和即可，无需区分
        # 两种形态。
        return sum(self._buf[: self._count]) / self._count


class Ewma:
    """指数加权移动平均。

    首个样本直接作为初值（seed），而不是从 0 开始衰减——否则服务启动后
    EWMA 要花约一个时间常数（alpha=2/61 时约 60 秒）才能爬到真实水平，
    加权分配在这段时间会看到系统性偏低的用量。
    """

    __slots__ = ("_alpha", "_value", "_seeded")

    def __init__(self, alpha: float):
        # alpha 应满足 0 < alpha <= 1，越大对新样本越敏感。collector 以
        # alpha = 2/(N+1)、N=60 构造，在 1 秒采样节奏下近似 60 秒均线。
        self._alpha = alpha
        self._value = 0.0
        self._seeded = False  # 是否已用首个样本完成初始化

    def update(self, v: float) -> None:
        """把一个新样本折入均值：首个样本直接落位（理由见类注释），之后
        按标准递推 value = α·v + (1-α)·value。"""
        if not self._seeded:
            self._value = v
            self._seeded = True
            return
        self._value = self._alpha * v + (1 - self._alpha) * self._value

    @property
    def value(self) -> float:
        """当前均值（首次 update 之前为 0）。"""
        return self._value
