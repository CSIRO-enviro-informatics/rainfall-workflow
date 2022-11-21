"""\
Move ACCESS-G forecast data from NCI to our network. Limit the data to Australia only to save space.
"""

import datetime
import ssl
import tempfile

import xarray as xr
from dateutil.relativedelta import relativedelta

from . import settings
from .dates import get_dates, check_latest_local_files
from lxml import etree as et
from urllib.request import urlopen, Request
from os import path, getenv
from multiprocessing.pool import ThreadPool
import pathlib

def limit_coordinates(netcdf_file_path: str):
    """\ Limit data of a global netCDF file to Australian coordinates."""
    data = xr.open_dataset(netcdf_file_path)
    # aus_data = data.sel(lat=slice(-9.005, -43.735), lon=slice(112.905, 153.995))  # coordinates matching SMIPS
    # aus_data = data.sel(lat=slice(-9.140625, -45.0), lon=slice(110.03906, 157.85156)) # coordinates from APS2 bounded access-g data
    aus_data = data.sel(lat=slice(-9.0, -45.0), lon=slice(110.0, 158.0))  # coordinates from standard australia slice (used by Andrew for historical access-g)
    return data, aus_data

def restrict_vars(ds, varnames, always_keep=["time", "lat", "lon", "latitude_longitude", "forecast_reference_time", "base_date", "base_time", "seg_type"]):
    for varname in varnames:
        if varname not in ds.variables:
            raise RuntimeError("Cannot find variable {} in dataset.".format(varname))
    to_remove = []
    for var in ds.variables:
        if var in varnames or var in always_keep:
            continue
        to_remove.append(var)
        #print("will remove {}".format(var))
    slimmed = ds.drop_vars(to_remove)
    del ds
    return slimmed

def merge_files(file_names, varnames):
    # merges all files into first file
    base_ds = xr.open_dataset(file_names[0])
    for varname in varnames:
        if varname not in base_ds.variables:
            raise RuntimeError("Cannot find variable {} in dataset.".format(varname))
    try:
        # try to add it a little bit of basic expected metadata to our merged file
        base_date_time = base_ds['forecast_reference_time'].dt.strftime("%Y%m%d %H%M").data[0]
        base_date, base_time = base_date_time.split(" ")
        base_ds.attrs['base_date'] = base_date
        base_ds.attrs['base_time'] = base_time
    except:
        pass
    for fn in file_names[1:]:
        print("merging {}".format(fn))
        next_ds = xr.open_dataset(fn)
        base_ds = base_ds.merge(next_ds)
    return base_ds

_url_tempfiles = []
def url_retrieve_badssl(url, filename=None, reporthook=None, data=None):
    """
    Retrieve a URL into a temporary location on disk.

    Requires a URL argument. If a filename is passed, it is used as
    the temporary file location. The reporthook argument should be
    a callable that accepts a block number, a read size, and the
    total file size of the URL target. The data argument should be
    valid URL encoded data.

    If a filename is passed and the URL points to a local resource,
    the result is a copy from local file to new file.

    Returns a tuple containing the path to the newly created
    data file as well as the resulting HTTPMessage object.
    """
    url = str(url)
    if url.startswith("file:"):
        raise ValueError("Don't use this function for file:// links")
    elif url.startswith("http:"):
        raise ValueError("This function is only for https endpoints.")
    if reporthook is not None:
        raise ValueError("this function only works with reporthook=None")
    url_type = "https"
    path = url
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urlopen(url, data, context=ctx) as fp:
        headers = fp.info()

        # Handle temporary file setup.
        if filename:
            tfp = open(filename, 'wb')
        else:
            tfp = tempfile.NamedTemporaryFile(delete=False)
            filename = tfp.name
            _url_tempfiles.append(filename)

        with tfp:
            result = filename, headers
            bs = 1024 * 8
            size = -1
            read = 0
            blocknum = 0
            if "content-length" in headers:
                size = int(headers["Content-Length"])
            while True:
                block = fp.read(bs)
                if not block:
                    break
                read += len(block)
                tfp.write(block)
                blocknum += 1

    if size >= 0 and read < size:
        raise RuntimeError(
            "retrieval incomplete: got only %i out of %i bytes"
            % (read, size), result)

    return result

def read_xml_catalog(url):
    parser = et.XMLParser(encoding='utf-8', recover=True, huge_tree=True)

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # Get server directory
    r = Request(url,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Accept-Encoding": "gzip,deflate",
            "Accept-Language": "en-US,en"
        })
    all_catalogrefs = []
    with urlopen(r, context=ctx) as cat:
        tree = et.parse(cat, parser=parser)  # type: et._ElementTree
        root = tree.getroot()
        #print(et.tostring(root, pretty_print=True).decode())
        nsmap = {
            "i": root.nsmap[None],
            'xlink': root.nsmap['xlink']
        }
        # root is /catalog/, so don't include that in the xpath from root.
        ds = root.xpath("/i:catalog/i:dataset", namespaces=nsmap)
        if len(ds) < 1:
            raise RuntimeError("Cannot get a Dataset entry in the XML Catalog: {}".format(url))
        ds = ds[0]
        all_catalogrefs = ds.xpath("./i:catalogRef", namespaces=nsmap)
        all_dss = ds.xpath("./i:dataset", namespaces=nsmap)
    refs = {r.get('{{{xlink}}}title'.format(**nsmap)): r.get('{{{xlink}}}href'.format(**nsmap)) for r in all_catalogrefs}
    refs.update({d.get('name'): d.get('urlPath') for d in all_dss})
    return refs

compressed = {'dtype': 'float64', 'complevel': 1,'zlib': True}

def _download_part(part_i, dl_url, temp_download_path, file_name_template):
    if not temp_download_path.exists():
        print("Attempting to download: {}".format(dl_url))
        temp_download_path.parent.mkdir(parents=True, exist_ok=True)
        finished_dl, http_status = url_retrieve_badssl(dl_url, temp_download_path)
    else:
        print("Using existing downloaded: {}".format(dl_url))
        finished_dl = temp_download_path
    whole_ds, australia_ds = limit_coordinates(finished_dl)
    prcp_file = restrict_vars(australia_ds, ["accum_prcp"])  # this deletes its own reference to australia_ds
    new_file_name1 = file_name_template.replace(".nc", ".{:03d}.nc".format(part_i + 1))
    local_file_path2 = pathlib.Path(settings.TEMP_PATH).absolute() / new_file_name1
    local_file_path2.parent.mkdir(parents=True, exist_ok=True)
    prcp_file.to_netcdf(str(local_file_path2),
                        encoding={'accum_prcp': compressed, 'lat': compressed, 'lon': compressed,
                                  'time': compressed})
    del prcp_file
    whole_ds.close()
    finished_dl.unlink()
    return local_file_path2

def get_thread_pool():
    p = getattr(get_thread_pool, '_cached', None)
    if p is None:
        p = ThreadPool(8)
        setattr(get_thread_pool, '_cached', p)
    return p

def download_all_parts(dl_list, file_name_template, thredds_fs_base):
    p = get_thread_pool()
    tasks = []
    for _index, link in enumerate(dl_list):
        full_dl = thredds_fs_base + link
        temp_download_path = pathlib.Path(settings.TEMP_PATH).absolute() / link
        task = p.apply_async(_download_part, args=(_index, full_dl, temp_download_path, file_name_template))
        tasks.append(task)
    done_files = [t.get() for t in tasks]
    return done_files

def get_latest_accessg_files(start_date=None, end_date=None):
    """\
    Transfer daily ACCESS-G files from Digiscapes Thredds server
    Run without arguments to update - only transfer files newer than the newest file.

    @param start_date: starting date for files to download (in UTC)
    @param end_date: end date for files to download (not inclusive)
    """

    # geoserver base
    thredds_base = "https://data-cbr.it.csiro.au/thredds/"
    thredds_fs_base = thredds_base + "fileServer/"
    thredds_cat_base = thredds_base + "catalog/"
    thredds_accessg_catalog = thredds_cat_base + "catch_all/Digiscape_Climate_Data_Portal/access-g_forecast/"
    d1 = relativedelta(days=1)
    if not start_date:
        (recent_date, recent_file) = check_latest_local_files(settings.ACCESS_G_APS3_PATH)
        if recent_date is None:
            raise RuntimeError("Cannot determine which date to start looking for in ACCESS_G files.")
        #recent_date is date is the 12z date from the filename, so consider it UTC
        start_date = recent_date.date() + d1  # Add one day. We don't want a duplicate

    catalog = read_xml_catalog(thredds_accessg_catalog+"catalog.xml")
    latest = None
    nowz = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    todayz = nowz
    compare_date = datetime.datetime(start_date.year, start_date.month, start_date.day, hour=int(settings.ACCESS_HR), tzinfo=datetime.timezone.utc)\
                   +datetime.timedelta(hours=settings.ACCESS_G_UPDATE_HOUR)
    if compare_date >= nowz:
        # The previous day's 1200 file is finished updating to Digiscapes thredds server at 10.45am each day
        print('ACCESS-G downloaded files are already up to date')
        return []
    if end_date is None:
        end_date = todayz.date()
        compare_date = datetime.datetime(todayz.year, todayz.month, todayz.day, int(settings.ACCESS_HR), tzinfo=datetime.timezone.utc) \
                       +datetime.timedelta(hours=settings.ACCESS_G_UPDATE_HOUR)
        if nowz >= compare_date:
            end_date = end_date + d1
    dates = get_dates(start_date, end_date)  # not inclusive of end_date
    GET_ACCESSG_TEST = getenv("GET_ACCESSG_TEST", "")
    download_links = {}
    done_dates = []
    # collect download links
    for date in dates:
        try:
            find_names = [settings.make_access_g_product_filename(date, fchours=h) for h in range(1, 241)]
            find_file_name1 = find_names[0]
            find_file_name2 = find_names[1]
            find_file_name3 = find_names[-1]
            print("Looking for file: {}".format(find_file_name1))
            datez = str(date)+settings.ACCESS_HR
            # it might be in the saved folders in the catalog
            if datez in catalog:
                href = catalog[datez]
                cat2 = read_xml_catalog(thredds_accessg_catalog+href)
                if find_file_name1 in cat2:
                    if find_file_name2 not in cat2:
                        raise RuntimeError("Found {} but not {}".format(find_file_name1, find_file_name2))
                    if find_file_name3 not in cat2:
                        raise RuntimeError("Found {} and {} but not {}"
                                           .format(find_file_name1, find_file_name2, find_file_name3))
                    found_file = cat2[find_file_name1]
                    print("Found it in archive: {}".format(found_file))
                    download_links[date] = [cat2[f] for f in find_names]
                    if GET_ACCESSG_TEST:
                        break
                    continue
            if latest is None:
                latest = read_xml_catalog(thredds_accessg_catalog+"latest/catalog.xml")
            if find_file_name1 in latest:
                if find_file_name2 not in latest:
                    raise RuntimeError("Found {} but not {}".format(find_file_name1, find_file_name2))
                if find_file_name3 not in latest:
                    raise RuntimeError("Found {} and {} but not {}"
                                       .format(find_file_name1, find_file_name2, find_file_name3))
                found_file = latest[find_file_name1]
                print("Found it in latest: {}".format(found_file))
                download_links[date] = [latest[f] for f in find_names]
                if GET_ACCESSG_TEST:
                    break
            else:
                print(RuntimeError("Cannot find a location to download {}".format(find_file_name1)))
                continue
        except Exception as e:
            print(e)
            continue

    for date_str, dl in download_links.items():
        new_file_name = settings.access_g_filename(date_str)
        date = datetime.date(int(date_str[0:4]),int(date_str[4:6]),int(date_str[6:8]))
        new_files = download_all_parts(dl, new_file_name, thredds_fs_base)
        merged_file = merge_files(new_files, ["accum_prcp"])
        merged_file.to_netcdf(settings.ACCESS_G_APS3_PATH + new_file_name,
                              encoding={'accum_prcp': compressed, 'lat': compressed, 'lon': compressed, 'time': compressed})
        merged_file.close()
        done_dates.append(date)

    #Save to settings.ACCESS_G_APS3_PATH

    # with pysftp.Connection(host=my_hostname, username=my_username, private_key=private_key) as sftp:
    #     print("Connection succesfully established ... ")
    #
    #     # Switch to a remote directory
    #     sftp.cwd('/g/data3/lb4/ops_aps2/access-g/0001/')
    #
    #     nc_filename = 'accum_prcp.nc'
    #     hour = settings.ACCESS_HOUR
    #
    #     localPath = 'temp/'
    #
    #     for date in dates:
    #         new_file_name = settings.access_g_filename(date)
    #         remoteFilePath = date + '/' + hour + '/fc/sfc/' + nc_filename
    #         localFilePath = localPath + new_file_name
    #         sftp.get(remoteFilePath, localFilePath)
    #
    #         australiaFile = limit_coordinates(localFilePath)
    #         australiaFile.to_netcdf(networkPath + new_file_name)
    #
    #         print('File: ' + new_file_name + ' written')
    # connection closed automatically at the end of the with-block
    return done_dates

if __name__ == '__main__':
    get_latest_accessg_files()
