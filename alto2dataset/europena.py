# AUTOGENERATED! DO NOT EDIT! File to edit: ../01_europena.ipynb.

# %% auto 0
__all__ = ['features', 'alto_parse', 'get_alto_text', 'alto_illustrations', 'NewspaperPageAlto', 'parse_newspaper_page',
           'NewspaperPageMetadata', 'get_metadata_from_xml', 'get_metadata_for_page', 'NewspaperPage',
           'process_newspaper_page', 'process_batch', 'process']

# %% ../01_europena.ipynb 4
import io
import os
import xml
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor, as_completed
# from dataclaises import asdict, dataclass, field
from functools import lru_cache
from pathlib import Path
from statistics import mean, stdev
from attrs import asdict
import toolz
import itertools
import  multiprocessing
from typing import Any, Dict, Iterable, List, Optional, Union
from attrs import define, field

import xmltodict
from toolz import partition_all
from tqdm.auto import tqdm

# %% ../01_europena.ipynb 11
from loguru import logger

# %% ../01_europena.ipynb 12
def alto_parse(alto: Union[str, Path], **kwargs):
    """Convert ALTO xml file to element tree"""
    try:
        xml = ET.parse(alto, **kwargs)
    except ET.ParseError as e:
        logger.error(f"Parser Error in file '{alto}': {e}")
        return None
    # Register ALTO namespaces
    # https://www.loc.gov/standards/alto/ | https://github.com/altoxml
    # alto-bnf (unoffical) BnF ALTO dialect - for further info see
    # http://bibnum.bnf.fr/alto_prod/documentation/alto_prod.html
    namespace = {
        "alto-1": "http://schema.ccs-gmbh.com/ALTO",
        "alto-2": "http://www.loc.gov/standards/alto/ns-v2#",
        "alto-3": "http://www.loc.gov/standards/alto/ns-v3#",
        "alto-4": "http://www.loc.gov/standards/alto/ns-v4#",
        "alto-5": "http://schema.ccs-gmbh.com/docworks/version20/alto-1-4.xsd",
        "alto-bnf": "http://bibnum.bnf.fr/ns/alto_prod",
    }
    # Extract namespace from document root
    if "http://" in str(xml.getroot().tag.split("}")[0].strip("{")):
        xmlns = xml.getroot().tag.split("}")[0].strip("{")
    else:
        try:
            ns = xml.getroot().attrib
            xmlns = str(ns).split(" ")[1].strip("}").strip("'")
        except IndexError:
            logger.warning(f"File {alto.name}: no namespace declaration found.")
            xmlns = "no_namespace_found"
    if xmlns in namespace.values():
        return alto, xml, xmlns
    else:
        logger.warning(f"File {alto.name}: namespace {xmlns} is not registered.")

# %% ../01_europena.ipynb 16
def get_alto_text(xml, xmlns, join_lines=True):
    """Extract text content from ALTO xml file"""
    all_text = []
    all_wc = []
    # Find all <TextLine> elements
    for lines in xml.iterfind(".//{%s}TextLine" % xmlns):
        # Find all <String> elements
        for line in lines.findall("{%s}String" % xmlns):
            wc = line.attrib["WC"]
            if wc is not None:
                all_wc.append(float(wc))
            # Check if there are no hyphenated words
            if "SUBS_CONTENT" not in line.attrib and "SUBS_TYPE" not in line.attrib:
                # Get value of attribute @CONTENT from all <String> elements
                text = line.attrib.get("CONTENT")  # + ' '
            elif "HypPart1" in line.attrib.get("SUBS_TYPE"):
                text = line.attrib.get("SUBS_CONTENT")  # + ' '
                if "HypPart2" in line.attrib.get("SUBS_TYPE"):
                    pass
            all_text.append(text)
    if all_wc:
        mean_ocr = mean(all_wc)
    if len(all_wc) > 2:
        std_ocr = stdev(all_wc)
    else:
        mean_ocr = None
        std_ocr = None
    return " ".join(all_text), mean_ocr, std_ocr

# %% ../01_europena.ipynb 18
def alto_illustrations(xml, xmlns):
    """Extract bounding boxes of illustration from ALTO xml file"""
    # Find all <Illustration> elements
    bounding_boxes = []
    for illustration in xml.iterfind(".//{%s}Illustration" % xmlns):
        # Get @ID of <Illustration> element
        illustration_id = illustration.attrib.get("ID")
        # Get coordinates of <Illustration> element
        illustration_coords = list(
            map(
                float,
                (
                    illustration.attrib.get("HEIGHT"),
                    illustration.attrib.get("WIDTH"),
                    illustration.attrib.get("VPOS"),
                    illustration.attrib.get("HPOS"),
                ),
            )
        )
        bounding_boxes.append(illustration_coords)
    return bounding_boxes

# %% ../01_europena.ipynb 26
@define(slots=True)
class NewspaperPageAlto:
    fname: Union[str, Path]
    text: Optional[str]
    mean_ocr: Optional[float]
    std_ocr: Optional[float]
    bounding_boxes: List[Union[float, None]]
    item_id: str = field(init=False)
    def _get_id(self):
        return "/".join(Path(self.fname).parts[-3:-1])

    def __attrs_post_init__(self):
        self.item_id = self._get_id()


# %% ../01_europena.ipynb 27
def parse_newspaper_page(xml_fname: Union[str, Path]):
    fname, xml, ns = alto_parse(xml_fname)
    text, wc, std_ocr = get_alto_text(xml, ns)
    bounding_boxes = alto_illustrations(xml, ns)
    return NewspaperPageAlto(xml_fname, text, wc, std_ocr, bounding_boxes)

# %% ../01_europena.ipynb 33
@define(slots=True)
class NewspaperPageMetadata:
    metadata_xml_fname: Union[str, Path]
    title: Optional[str]
    date: Optional[str]
    languages: Union[List[str], str, None]
    item_iiif_url: Optional[str]
    all_metadata_dict: Dict[Any, Any]

    def __attrs_post_init__(self):
        self.languages = (
            self.languages.split(",")
            if isinstance(self.languages, str)
            else self.languages
        )
        self.title = self.title.split("-")[0].strip(" ")
        self.metadata_xml_fname = str(self.metadata_xml_fname)

# %% ../01_europena.ipynb 34
def get_metadata_from_xml(xml_file: Union[Path, str]):
    with open(xml_file, "r") as f:
        xml: Dict = xmltodict.parse(f.read())
    metadata = xml.get("rdf:RDF")
    ProvidedCHO = metadata.get("edm:ProvidedCHO")
    if ProvidedCHO is not None:
        title = ProvidedCHO.get("dc:title")
        data = ProvidedCHO.get("dcterms:issued")
        languages = ProvidedCHO.get("dc:language")
        try:
            iiif_url = metadata["ore:Aggregation"]["edm:isShownBy"]["@rdf:resource"]
        except KeyError:
            iiif_url = None
    else:
        title, data, languages, iiif_url = None, None, None, None
    return NewspaperPageMetadata(xml_file, title, data, languages, iiif_url, metadata)

# %% ../01_europena.ipynb 38
def get_metadata_for_page(
    page: NewspaperPageAlto, metadata_directory: Optional[str] = None
):
    short_id = page.item_id.split("_")[-1]
    metadata_xml = f"{metadata_directory}/http%3A%2F%2Fdata.theeuropeanlibrary.org%2FBibliographicResource%2F{short_id}.edm.xml"
    return get_metadata_from_xml(metadata_xml)

# %% ../01_europena.ipynb 42
@define(slots=True)
class NewspaperPage:
    fname: Union[str, Path]
    text: Optional[str]
    mean_ocr: Optional[float]
    std_ocr: Optional[float]
    bounding_boxes: List[Union[float, None]]
    item_id: str
    metadata_xml_fname: Union[str, Path]
    title: Optional[str]
    date: Optional[str]
    languages: Union[List[str], None]
    item_iiif_url: Optional[str]
   # all_metadata_dict: Dict[Any, Any]
    multi_language: bool = field(init=False)
    issue_uri: str = field(init=False)
    id: str = field(init=False)
    def __attrs_post_init__(self):
        self.issue_uri = f"https://www.europeana.eu/item/{self.item_id}"
        self.metadata_xml_fname = str(self.metadata_xml_fname)
        self.languages = (
            [lang for lang in self.languages if lang != "=="]
            if isinstance(self.languages, list)
            else self.languages
        )
        self.multi_language = (
            isinstance(self.languages, list) and len(self.languages) > 1
        )
        self.id = f"{self.issue_uri}/${self.fname.name.strip('.xml')}"

# %% ../01_europena.ipynb 43
def process_newspaper_page(
    xml_file: Union[str, Path], metadata_directory: Optional[str] = None
) -> NewspaperPage:
    page = parse_newspaper_page(xml_file)
    metadata = get_metadata_for_page(page, metadata_directory=metadata_directory)
    metadata = asdict(metadata)
    metadata.pop("all_metadata_dict")
    page = asdict(page)
    return NewspaperPage(**page, **metadata)

# %% ../01_europena.ipynb 48
from datasets import Dataset
from datasets import Value, Sequence, Features

# %% ../01_europena.ipynb 49
features=Features({
    'fname': Value(dtype='string', id=None),
    'text': Value(dtype='string', id=None),
    'mean_ocr': Value(dtype='float64', id=None),
    'std_ocr': Value(dtype='float64', id=None),
    'bounding_boxes': Sequence(
        feature=Sequence(feature=Value(dtype='float64', id=None), length=-1, id=None),
        length=-1,
        id=None
    ),
    'item_id': Value(dtype='string', id=None),
    "id": Value(dtype="string",id=None),
    "issue_uri": Value(dtype="string", id=None),
    'metadata_xml_fname': Value(dtype='string', id=None),
    'title': Value(dtype='string', id=None),
    'date': Value(dtype='string', id=None),
    'languages': Sequence(feature=Value(dtype='string', id=None), length=-1, id=None),
    'item_iiif_url': Value(dtype='string', id=None),
    'multi_language': Value(dtype='bool', id=None),
   
})


# %% ../01_europena.ipynb 52
@logger.catch()
def process_batch(xml_batch: Iterable[Union[str, Path]], metadata_directory: Optional[Union[str,Path]]=None)-> Dataset:
    """Returns a dataset containing parsed newspaper pages."""
    batch = [
        asdict(process_newspaper_page(xml, metadata_directory=metadata_directory))
        for xml in xml_batch
    ]
    batch = {key: [i[key] for i in batch] for key in batch[0]}
    dataset = Dataset.from_dict(batch,features=features)
    dataset = dataset.remove_columns(["item_id","metadata_xml_fname","fname"])
    dataset = dataset.rename_columns({"languages":"language"})
    return dataset


# %% ../01_europena.ipynb 56
def process(
    xml_files: Iterable[Union[str, Path]],
    batch_size: int = 32,
    metadata_directory: Optional[Union[str,Path]] = None,
    max_workers: Optional[int] = None
):
    xml_files_for_count, xml_files = itertools.tee(xml_files)
    total = toolz.count(xml_files_for_count)
    with tqdm(total=total // batch_size) as pbar:
        if not max_workers:
            max_workers = multiprocessing.cpu_count()
        futures = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for batch in partition_all(batch_size, xml_files):
                batch = list(batch)
                future = executor.submit(
                    process_batch, batch, metadata_directory=metadata_directory
                )
                future.add_done_callback(lambda p: pbar.update(1))
                futures.append(future)
    return [future.result() for future in as_completed(futures)]


