from numpy.core.multiarray import ndarray
import os

crs_wkt = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
crs_wkt_a = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'


def add_crs_var_to_netcdf(dataset):
    ll_var = dataset.createVariable("crs", 'i1')
    ll_var.setncatts({
        "grid_mapping_name": "latitude_longitude",
        "long_name": "crs",
        "longitude_of_prime_meridian": 0.0,
        "semi_major_axis": 6378137.0,
        "inverse_flattening": 298.257223563,
        "spatial_ref": crs_wkt_a,
        "crs_wkt": crs_wkt_a
    })
    return ll_var

def first(i):
    assert is_iterable(i)
    return next(iter(i))

def is_iterable(i):
    return isinstance(i, (list, set, tuple, frozenset, ndarray))

def grouper(iterable, n):
    assert is_iterable(iterable)
    if isinstance(iterable, (list, tuple, ndarray)):
        return list_grouper(iterable, n)
    elif isinstance(iterable, (set, frozenset)):
        return set_grouper(iterable, n)

def distributor(iterable, n):
    assert is_iterable(iterable)
    if isinstance(iterable, (list, tuple, ndarray)):
        return list_distributor(iterable, n)
    elif isinstance(iterable, (set, frozenset)):
        return list_distributor(list(iterable), n)

def list_distributor(l, n):
    assert isinstance(l, (list, tuple, ndarray))
    assert n > 0 # n = 4
    l_len = len(l)  # l = 8
    d = l_len // n  # l = 2
    remainder = l_len % n  #r = 0
    # res = [[0,1],[2,3],[4, 5],[6,7]]
    if remainder == 0:
        # split evenly
        yield from (l[i*d:i*d+d] for i in range(n))
    else:
        parts = [l[i*d:i*d+d] for i in range(n)]
        if isinstance(l, (tuple, ndarray)):
            parts = [list(p) for p in parts]
        _ = [parts[j].append(l[(d*n)+j]) if j < remainder else None for j in range(n)]
        yield from parts

def list_grouper(iterable, n):
    assert isinstance(iterable, (list, tuple, ndarray))
    assert n > 0
    iterable = iter(iterable)
    count = 0
    group = list()
    while True:
        try:
            group.append(next(iterable))
            count += 1
            if count % n == 0:
                yield tuple(group)
                group = list()
        except StopIteration:
            if len(group) < 1:
                raise StopIteration()
            else:
                yield tuple(group)
            break

def set_grouper(iterable, n):
    assert isinstance(iterable, (set, frozenset))
    assert n > 0
    iterable = iter(iterable)
    count = 0
    group = set()
    while True:
        try:
            group.add(next(iterable))
            count += 1
            if count % n == 0:
                yield frozenset(group)
                group = set()
        except StopIteration:
            if len(group) < 1:
                raise StopIteration()
            else:
                yield frozenset(group)
            break

def in_kubernetes():
    h = os.getenv("KUBERNETES_SERVICE_HOST", None)
    return h is not None or os.path.exists("/var/run/secrets/kubernetes.io")

def in_docker():
    return (
        os.path.exists('/.dockerenv') or
        os.path.isfile('/proc/self/cgroup') and any('docker' in line for line in open('/proc/self/cgroup'))
    )
