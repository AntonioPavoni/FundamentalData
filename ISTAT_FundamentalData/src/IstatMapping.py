import requests
import json
import os
import logging
from xml.etree import ElementTree as ET
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('istat_mapping_builder.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DataStructureInfo:
    agency_id: str
    structure_id: str
    version: str

@dataclass
class CodelistRef:
    agency_id: str
    id: str
    version: str

class IstatMappingBuilder:
    def __init__(self, dataset_id: str, output_dir: str = "./mappings"):
        self.dataset_id = dataset_id
        self.base_url = "https://sdmx.istat.it/SDMXWS/rest"
        self.output_dir = output_dir
        self.namespaces = {
            'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
            'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic'
        }
        os.makedirs(output_dir, exist_ok=True)

    def fetch_xml(self, url: str) -> Optional[ET.Element]:
        """Fetch and parse XML from a URL."""
        try:
            logger.info(f"Fetching URL: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            logger.debug(f"Successfully fetched from {url}")
            return ET.fromstring(response.content)
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            raise

    def get_structure_from_data(self) -> DataStructureInfo:
        """Get structure information from the data endpoint."""
        try:
            url = f"{self.base_url}/data/{self.dataset_id}"
            root = self.fetch_xml(url)
            
            # Find the Structure element with common:Structure
            struct_elem = root.find('.//common:Structure', self.namespaces)
            if struct_elem is None:
                raise ValueError("Structure element not found in data response")
            
            # Get the Ref element
            ref_elem = struct_elem.find('Ref')
            if ref_elem is None:
                raise ValueError("Ref element not found in Structure")
            
            return DataStructureInfo(
                agency_id=ref_elem.attrib.get('agencyID'),
                structure_id=ref_elem.attrib.get('id'),
                version=ref_elem.attrib.get('version')
            )
        except Exception as e:
            logger.error(f"Error getting structure from data: {str(e)}")
            raise

    def get_dimension_codelist_refs(self, structure_info: DataStructureInfo) -> Dict[str, CodelistRef]:
        """Get codelist references for each dimension."""
        try:
            url = f"{self.base_url}/datastructure/{structure_info.agency_id}/{structure_info.structure_id}"
            root = self.fetch_xml(url)
            
            codelist_refs = {}
            
            # Find all dimensions
            dimensions = root.findall('.//structure:Dimension', self.namespaces)
            for dim in dimensions:
                dim_id = dim.attrib.get('id')
                if not dim_id:
                    continue
                
                # Find enumeration reference
                enum_ref = dim.find('.//structure:Enumeration/Ref', self.namespaces)
                if enum_ref is not None:
                    codelist_refs[dim_id] = CodelistRef(
                        agency_id=enum_ref.attrib.get('agencyID', structure_info.agency_id),
                        id=enum_ref.attrib.get('id'),
                        version=enum_ref.attrib.get('version', '1.0')
                    )
                    logger.debug(f"Found codelist reference for dimension {dim_id}: {codelist_refs[dim_id]}")
            
            return codelist_refs
        except Exception as e:
            logger.error(f"Error getting dimension codelist refs: {str(e)}")
            raise

    def get_codelist_values(self, codelist_ref: CodelistRef) -> Dict[str, Dict]:
        """Get all values and descriptions from a codelist."""
        try:
            url = f"{self.base_url}/codelist/{codelist_ref.agency_id}/{codelist_ref.id}"
            root = self.fetch_xml(url)
            
            values = {}
            
            # Get codelist name and description
            codelist_info = {
                "name": {},
                "description": {}
            }
            
            for name in root.findall('.//structure:Name/common:Name', self.namespaces):
                lang = name.attrib.get('{http://www.w3.org/XML/1998/namespace}lang', 'default')
                codelist_info["name"][lang] = name.text
            
            # Get all codes
            for code in root.findall('.//structure:Code', self.namespaces):
                code_id = code.attrib.get('id')
                if not code_id:
                    continue
                
                values[code_id] = {
                    "name": {},
                    "description": {}
                }
                
                # Get names in all languages
                for name in code.findall('.//common:Name', self.namespaces):
                    lang = name.attrib.get('{http://www.w3.org/XML/1998/namespace}lang', 'default')
                    values[code_id]["name"][lang] = name.text
                
                # Get descriptions in all languages
                for desc in code.findall('.//common:Description', self.namespaces):
                    lang = desc.attrib.get('{http://www.w3.org/XML/1998/namespace}lang', 'default')
                    values[code_id]["description"][lang] = desc.text
            
            return {
                "codelist_info": codelist_info,
                "values": values
            }
        except Exception as e:
            logger.error(f"Error getting codelist values for {codelist_ref.id}: {str(e)}")
            raise

    def build_mapping(self):
        """Build complete mapping dictionary and save to JSON file."""
        try:
            logger.info(f"Starting to build mapping for dataset {self.dataset_id}")
            
            # Get structure info from data endpoint
            structure_info = self.get_structure_from_data()
            logger.info(f"Found structure: {structure_info}")
            
            # Initialize mapping dictionary
            mapping = {
                "dataset_id": self.dataset_id,
                "generated_at": datetime.now().isoformat(),
                "structure": {
                    "agency_id": structure_info.agency_id,
                    "id": structure_info.structure_id,
                    "version": structure_info.version
                },
                "dimensions": {}
            }
            
            # Get codelist references for all dimensions
            codelist_refs = self.get_dimension_codelist_refs(structure_info)
            
            # Get values for each codelist
            for dim_id, codelist_ref in codelist_refs.items():
                logger.info(f"Processing dimension {dim_id} with codelist {codelist_ref.id}")
                
                codelist_data = self.get_codelist_values(codelist_ref)
                
                mapping["dimensions"][dim_id] = {
                    "codelist": {
                        "id": codelist_ref.id,
                        "agency_id": codelist_ref.agency_id,
                        "version": codelist_ref.version,
                        "name": codelist_data["codelist_info"]["name"],
                        "description": codelist_data["codelist_info"]["description"]
                    },
                    "values": codelist_data["values"]
                }
            
            # Save to file
            output_file = os.path.join(self.output_dir, f"mapping_{self.dataset_id}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Mapping file created successfully: {output_file}")
            return mapping
            
        except Exception as e:
            logger.error(f"Error building mapping: {str(e)}")
            raise

def main():
    try:
        dataset_id = "111_111"
        builder = IstatMappingBuilder(dataset_id)
        builder.build_mapping()
    except Exception as e:
        logger.critical(f"Script execution failed: {str(e)}")

if __name__ == "__main__":
    main()