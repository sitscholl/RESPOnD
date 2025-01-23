import numpy as np

def transform_suitability(x, upper1, upper2, vmin = None, vmax = None, max_suitability = 1):

    def line1(x, vmin, upper1):
        slp = max_suitability/(upper1-vmin)
        return vmin*slp + slp*(x-vmin) - vmin*slp
    def line2(x, vmax, upper2):
        slp2 = -max_suitability/(vmax-upper2)
        return max_suitability + slp2*(x - upper2)

    if vmin is None:
        vmin = np.min(x)
    if vmax is None:
        vmax = np.max(x)

    if upper1 > upper2:
        raise ValueError(f'upper1 must be smaller than upper2!')
    if vmin > vmax:
        raise ValueError(f'vmin must be smaller than vmax!')
    if vmax <= upper2:
        raise ValueError(f'vmax must be greater than upper2!')
    if vmin >= upper1:
        raise ValueError(f'vmin must be smaller than upper1!')

    return(
        np.piecewise(x,
        [(x > vmin) & (x < upper1), (x >= upper1) & (x <= upper2), (x > upper2) & (x < vmax)],
        [lambda x: line1(x, vmin, upper1), max_suitability, lambda x: line2(x, vmax, upper2)]
        )
    )
