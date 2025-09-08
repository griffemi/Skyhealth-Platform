def hdd(base, temp):
    return max(base - temp, 0)

def cdd(base, temp):
    return max(temp - base, 0)

def gdd(low, high, temp):
    clamped = max(min(temp, high), low)
    return max(clamped - low, 0)


def test_hdd():
    assert hdd(18, 10) == 8
    assert hdd(18, 20) == 0


def test_cdd():
    assert cdd(18, 25) == 7
    assert cdd(18, 10) == 0


def test_gdd():
    assert gdd(10, 30, 5) == 0
    assert gdd(10, 30, 35) == 20
    assert gdd(10, 30, 20) == 10
