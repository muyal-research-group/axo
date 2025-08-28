import pytest
from axo import Axo, axo_method
from axo.contextmanager import AxoContextManager

class Calc(Axo):
    @axo_method
    def sum(self, x, y, **kwargs):
        return x + y

# --- Baseline: sin Axo ---------------------------------------------------------
@pytest.mark.benchmark(group="baseline_sum")
def test_baseline_sum(benchmark):
    result = benchmark.pedantic(lambda: 1 + 2, iterations=10, rounds=100)
    assert result == 3

# --- Axo Local -----------------------------
@pytest.mark.benchmark(group="axo_sum")
def test_axo_sum_local(benchmark):
    with AxoContextManager.local():
        c = Calc()
        result = benchmark.pedantic(lambda: c.sum(1, 2).unwrap_or(0) , iterations=10, rounds=100)
        # result
    assert result == 3
