import requests
import json
import os
import logging
from xml.etree import ElementTree as ET
from typing import Dict, Optional, List
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('istat_constraints.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IstatConstraintsBuilder:
    def __init__(self, dataset_id: str, mappings_dir: str = "./mappings"):
        self.dataset_id = dataset_id
        self.base_url = "https://sdmx.istat.it/SDMXWS/rest"
        self.mappings_dir = mappings_dir
        self.output_dir = "./constraints"
        self.namespaces = {
            'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
            'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic'
        }
        
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_xml(self, url: str) -> Optional[ET.Element]:
        """Fetch and parse XML from a URL."""
        try:
            logger.info(f"Fetching URL: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return ET.fromstring(response.content)
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            raise

    def get_available_constraints(self) -> Dict[str, List[str]]:
        """Get available constraints from the availableconstraint endpoint."""
        try:
            url = f"{self.base_url}/availableconstraint/{self.dataset_id}"
            root = self.fetch_xml(url)
            
            constraints = {}
            
            # Find the CubeRegion element
            cube_region = root.find('.//structure:CubeRegion', self.namespaces)
            if cube_region is None:
                logger.error("No CubeRegion found in constraints")
                return {}

            # Process each KeyValue element
            for key_value in cube_region.findall('.//common:KeyValue', self.namespaces):
                dim_id = key_value.attrib.get('id')
                if dim_id:
                    constraints[dim_id] = []
                    # Get all Value elements for this dimension
                    for value in key_value.findall('common:Value', self.namespaces):
                        if value.text:
                            constraints[dim_id].append(value.text)
                    logger.debug(f"Found dimension {dim_id} with values: {constraints[dim_id]}")

            logger.info(f"Extracted constraints: {constraints}")
            return constraints
        except Exception as e:
            logger.error(f"Error getting available constraints: {str(e)}")
            raise

    def get_dataflow_info(self) -> Dict:
        """Get dataset metadata from dataflow endpoint."""
        try:
            url = f"{self.base_url}/dataflow/IT1/{self.dataset_id}"
            root = self.fetch_xml(url)
            
            info = {
                "id": self.dataset_id,
                "names": {},
                "structure_reference": {}  # Removed empty descriptions field
            }
            
            dataflow = root.find('.//structure:Dataflow', self.namespaces)
            if dataflow is not None:
                # Get names
                for name in dataflow.findall('.//common:Name', self.namespaces):
                    lang = name.attrib.get('{http://www.w3.org/XML/1998/namespace}lang')
                    info["names"][lang] = name.text
                
                # Get structure reference
                struct_ref = dataflow.find('.//structure:Structure/Ref', self.namespaces)
                if struct_ref is not None:
                    info["structure_reference"] = {
                        "agency_id": struct_ref.attrib.get('agencyID'),
                        "id": struct_ref.attrib.get('id'),
                        "version": struct_ref.attrib.get('version')
                    }
            
            return info
        except Exception as e:
            logger.error(f"Error getting dataflow info: {str(e)}")
            raise

    def load_mapping_file(self) -> Dict:
        """Load the existing mapping file."""
        try:
            mapping_file = os.path.join(self.mappings_dir, f"mapping_{self.dataset_id}.json")
            with open(mapping_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading mapping file: {str(e)}")
            raise

    def build_constraints_overview(self):
        """Build constraints overview using available constraints and mapping descriptions."""
        try:
            logger.info(f"Building constraints overview for dataset {self.dataset_id}")
            
            # Get dataflow info
            dataflow_info = self.get_dataflow_info()
            
            # Get available constraints
            available_constraints = self.get_available_constraints()
            
            # Load mapping file
            mapping = self.load_mapping_file()
            
            # Initialize overview
            overview = {
                "dataset_info": dataflow_info,
                "generated_at": datetime.now().isoformat(),
                "dimensions": {}
            }
            
            # Process each available constraint
            for dim_id, values in available_constraints.items():
                # Get dimension info from mapping if available
                dim_info = mapping.get("dimensions", {}).get(dim_id, {})
                
                overview["dimensions"][dim_id] = {
                    "id": dim_id,
                    "values": {}
                }
                
                # Process each value
                for value in values:
                    value_info = dim_info.get("values", {}).get(value, {})
                    if value_info:
                        # Only include name field without empty description
                        overview["dimensions"][dim_id]["values"][value] = {
                            "name": value_info.get("name", {"default": value})
                        }
                    else:
                        # If no mapping found, use default without empty description
                        overview["dimensions"][dim_id]["values"][value] = {
                            "name": {"default": value}
                        }
            
            # Save to file
            output_file = os.path.join(self.output_dir, f"constraints_{self.dataset_id}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(overview, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully created constraints overview with {len(overview['dimensions'])} dimensions")
            return overview
            
        except Exception as e:
            logger.error(f"Error building constraints overview: {str(e)}")
            raise

def main():
    try:
        dataset_id = "111_111"
        builder = IstatConstraintsBuilder(dataset_id)
        builder.build_constraints_overview()
    except Exception as e:
        logger.critical(f"Script execution failed: {str(e)}")

if __name__ == "__main__":
    main()