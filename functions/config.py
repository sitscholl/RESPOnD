import pooch

dem_chelsa = r"https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_orog_30arcsec_global.nc#mode=bytes"
gisco_lau = r"https://gisco-services.ec.europa.eu/distribution/v2/lau/gpkg/LAU_RG_01M_2021_4326.gpkg"
url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc#mode=bytes"

aois = {
    'europe': [-19.4, 27, 34.5, 57],
    'south_tyrol': [10,46,12,47],
    'alpine_space': [3.69,42.98,17.16,50.56]
}

downloader = pooch.create(
    path=pooch.os_cache("sdm"),
    base_url="",
    registry={
        "EU_PDO.gpkg": None,
        'LUISA_50m.tif': None
    },
    urls={
        "EU_PDO.gpkg": "https://springernature.figshare.com/ndownloader/files/35955185",
        'LUISA_50m.tif': "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/LUISA/EUROPE/Basemaps/LandUse/2018/LATEST/LUISA_basemap_020321_50m.tif"
    },
)