import pooch

aois = {
    'europe': [-19.4, 27, 34.5, 57],
    'south_tyrol': [10,46,12,47],
    'alpine_space': [3.69,42.98,17.16,50.56]
}

downloader = pooch.create(
    path=pooch.os_cache("sdm"),
    base_url="",
    urls={
        "EU_PDO.gpkg": "https://springernature.figshare.com/ndownloader/files/35955185",
        'LUISA_50m.tif': "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/LUISA/EUROPE/Basemaps/LandUse/2018/LATEST/LUISA_basemap_020321_50m.tif"
    },
)