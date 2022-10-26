import subprocess
from pathlib import Path

from datasets import concatenate_datasets
from toolz import concat
from tqdm.auto import tqdm

from alto2dataset.europena import *

europena_ids = {9200300, 9200338, 9200339, 9200355, 9200356, 9200357, 9200301, 9200396}


all_datasets = []
Path("altodata").mkdir(exist_ok=True)
Path("altodata/metadata").mkdir(exist_ok=True)
for id_ in tqdm(europena_ids):
    subprocess.call(
        [
            "aria2c",
            "-x",
            "4",
            "-d",
            "altodata/",
            f"ftp://download.europeana.eu/newspapers/fulltext/alto/{id_}.zip",
        ]
    )
    subprocess.call(["unzip", "altodata/*.zip", "-d", "altodata"])
    [p.unlink() for p in Path("altodata").rglob("*.zip")]
    subprocess.call(
        [
            "aria2c",
            "-x",
            "4",
            "-d",
            "altodata/metadata/",
            f"ftp://download.europeana.eu/newspapers/metadata/{id_}.zip",
        ]
    )
    subprocess.call(["unzip", "altodata/metadata/*.zip", "-d", "altodata/metadata/"])
    [p.unlink() for p in Path("altodata/metadata").rglob("*.zip")]
    alto_xmls = [f for f in Path("altodata").rglob("*.xml") if "edm" not in f.name]
    datasets = process(
        alto_xmls, batch_size=8, metadata_directory="altodata/metadata", max_workers=4
    )
    not_none_datasets = []
    for dataset in datasets:
        if dataset is not None:
            not_none_datasets.append(dataset)
        else:
            print("None found in batch")
    dataset = concatenate_datasets(not_none_datasets)

    all_datasets.append(dataset)
    [p.unlink() for p in Path("altodata").rglob("*.xml")]
ds = concatenate_datasets(all_datasets)
ds.save_to_disk("all_data")
languages = set(concat(ds["language"]))
decades = {f"{d[:3]}0" for d in ds["date"]}
multi_language_ds = ds.filter(lambda x: x["multi_language"] == True)
single_language_ds = ds.filter(lambda x: x["multi_language"] == False)
for language in tqdm(languages):
    lang_ds = single_language_ds.filter(lambda x: language in x["language"])
    for decade in tqdm(decades, leave=False):
        decade_ds = lang_ds.filter(
            lambda x: f"{x['date'].split('-')[0][:3]}0" == decade
        )
        if len(decade_ds) > 0:
            decade_ds.to_parquet(f"{language}-{decade}.parquet")
for decade in tqdm(decades, leave=False):
    decade_ds = multi_language_ds.filter(
        lambda x: f"{x['date'].split('-')[0][:3]}0" == decade
    )
    if len(decade_ds) > 0:
        decade_ds.to_parquet(f"multi_language-{decade}.parquet")
