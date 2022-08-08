# AUTOGENERATED! DO NOT EDIT! File to edit: ../01_europena.ipynb.

# %% auto 0
__all__ = ['alto_parse', 'get_alto_text', 'alto_illustrations', 'NewspaperPageAlto', 'parse_newspaper_page',
           'NewspaperPageMetadata', 'get_metadata_from_xml', 'get_metadata_for_page', 'NewspaperPage',
           'process_newspaper_page', 'process_batch', 'process']

# %% ../01_europena.ipynb 6
from typing import Any
from typing import Optional
from functools import lru_cache
import io
from typing import Union
from typing import Any
from statistics import mean
from statistics import stdev
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List
from dataclasses import asdict
from dataclasses import field
import os
from toolz import partition_all
import xml
from types import NoneType
from pathlib import Path
from rich import print
from typing import Union
import xmltodict
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Iterable

from tqdm.auto import tqdm

from typing import Dict

# %% ../01_europena.ipynb 14
from loguru import logger

# %% ../01_europena.ipynb 15
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

# %% ../01_europena.ipynb 18
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

# %% ../01_europena.ipynb 20
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

# %% ../01_europena.ipynb 28
@dataclass
class NewspaperPageAlto:
    fname: Union[str, Path]
    text: Optional[str]
    mean_ocr: Optional[float]
    std_ocr: Optional[float]
    bounding_boxes: List[Union[float, None]]
    item_id: str = field(init=False)

    def _get_id(self):
        return "/".join(Path(self.fname).parts[-3:-1])

    def __post_init__(self):
        self.item_id = self._get_id()

# %% ../01_europena.ipynb 29
def parse_newspaper_page(xml_fname: Union[str, Path]):
    fname, xml, ns = alto_parse(xml_fname)
    text, wc, std_ocr = get_alto_text(xml, ns)
    bounding_boxes = alto_illustrations(xml, ns)
    return NewspaperPageAlto(xml_fname, text, wc, std_ocr, bounding_boxes)

# %% ../01_europena.ipynb 37
@dataclass
class NewspaperPageMetadata:
    metadata_xml_fname: Union[str, Path]
    title: Optional[str]
    date: Optional[str]
    languages: Union[List[str], str, None]
    item_iiif_url: Optional[str]
    all_metadata_dict: Dict[Any, Any]

    def __post_init__(self):
        self.languages = (
            self.languages.split(",")
            if isinstance(self.languages, str)
            else self.languages
        )
        self.title = self.title.split("-")[0].strip(" ")
        self.metadata_xml_fname = str(self.metadata_xml_fname)

# %% ../01_europena.ipynb 38
def get_metadata_from_xml(xml_file: Union[Path, str]):
    with open(xml_file, "r") as f:
        xml = xmltodict.parse(f.read())
    metadata = xml["rdf:RDF"]
    ProvidedCHO = metadata["edm:ProvidedCHO"]
    title = ProvidedCHO["dc:title"]
    data = ProvidedCHO["dcterms:issued"]
    languages = ProvidedCHO["dc:language"]
    iiif_url = metadata["ore:Aggregation"]["edm:isShownBy"]["@rdf:resource"]
    return NewspaperPageMetadata(xml_file, title, data, languages, iiif_url, metadata)

# %% ../01_europena.ipynb 45
def get_metadata_for_page(
    page: NewspaperPageAlto, metadata_directory: Optional[str] = None
):
    short_id = page.item_id.split("_")[-1]
    metadata_xml = f"{metadata_directory}/http%3A%2F%2Fdata.theeuropeanlibrary.org%2FBibliographicResource%2F{short_id}.edm.xml"
    return get_metadata_from_xml(metadata_xml)

# %% ../01_europena.ipynb 48
@dataclass
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

    def __post_init__(self):
        self.fname = str(self.fname)
        self.metadata_xml_fname = str(self.metadata_xml_fname)
        self.languages = (
            [lang for lang in self.languages if lang != "=="]
            if isinstance(self.languages, list)
            else self.languages
        )
        self.multi_language = (
            isinstance(self.languages, list) and len(self.languages) > 1
        )

# %% ../01_europena.ipynb 49
def process_newspaper_page(
    xml_file: Union[str, Path], metadata_directory: Optional[str] = None
) -> Dict[Any, Any]:
    page = parse_newspaper_page(xml_file)
    metadata = get_metadata_for_page(page, metadata_directory=metadata_directory)
    metadata = asdict(metadata)
    metadata.pop("all_metadata_dict")
    page = asdict(page)
    return NewspaperPage(**page, **metadata)

# %% ../01_europena.ipynb 57
from datasets import Dataset

# %% ../01_europena.ipynb 58
@logger.catch()
def process_batch(xml_batch: Iterable[Union[str, Path]], metadata_directory=None):
    batch = [
        asdict(process_newspaper_page(xml, metadata_directory=metadata_directory))
        for xml in xml_batch
    ]

    batch = {key: [i[key] for i in batch] for key in batch[0]}

    return Dataset.from_dict(batch)

# %% ../01_europena.ipynb 60
def process(
    xml_files: Iterable[Union[str, Path]],
    batch_size: int = 32,
    metadata_directory: Optional[str] = None,
):
    with tqdm(total=len(xml_files) // 64) as pbar:
        futures = []
        with ProcessPoolExecutor(max_workers=8) as executor:
            for batch in partition_all(64, xml_files):
                batch = list(batch)
                future = executor.submit(
                    process_batch, batch, metadata_directory=metadata_directory
                )
                future.add_done_callback(lambda p: pbar.update(1))
                futures.append(future)
    return [future.result() for future in as_completed(futures)]
