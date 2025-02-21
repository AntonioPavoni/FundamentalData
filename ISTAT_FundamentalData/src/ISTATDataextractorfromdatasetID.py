import requests
import json
import os
import logging
from xml.etree import ElementTree as ET
from typing import Dict, List, Optional
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('istat_series_extractor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def find_constraints_file(dataset_id: str) -> str:
    """Find the constraints file for the given dataset ID."""
    constraints_dir = r"C:\Users\Antonio\Streamlit app\constraints"
    expected_filename = f"constraints_{dataset_id}.json"
    constraints_path = os.path.join(constraints_dir, expected_filename)
    
    if not os.path.exists(constraints_path):
        raise FileNotFoundError(
            f"Constraints file not found for dataset {dataset_id}. "
            f"Expected file: {constraints_path}. "
            f"Please ensure you have created the constraints file first."
        )
    
    return constraints_path

class IstatSeriesExtractor:
    def __init__(self, dataset_id: str, output_dir: str = "./series_data"):
        self.dataset_id = dataset_id
        self.base_url = "https://sdmx.istat.it/SDMXWS/rest"
        self.output_dir = output_dir
        
        # Automatically find the constraints file
        self.constraints_path = find_constraints_file(dataset_id)
        logger.info(f"Found constraints file: {self.constraints_path}")
        
        self.namespaces = {
            'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
            'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }
        
        # Load and validate constraints
        self.constraints = self.load_constraints()
        self.validate_constraints()
        
        os.makedirs(output_dir, exist_ok=True)

    def validate_constraints(self):
        """Validate that the constraints file matches the dataset ID."""
        if self.dataset_id != self.constraints['dataset_info']['id']:
            raise ValueError(
                f"Constraints file mismatch. Expected dataset {self.dataset_id} "
                f"but got {self.constraints['dataset_info']['id']}. "
                f"Please check the constraints file: {self.constraints_path}"
            )

    def load_constraints(self) -> Dict:
        """Load the constraints mapping file."""
        try:
            with open(self.constraints_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading constraints file: {str(e)}")
            raise

    def fetch_series_data(self) -> ET.Element:
        """Fetch series data from ISTAT API."""
        try:
            url = f"{self.base_url}/data/{self.dataset_id}"
            logger.info(f"Fetching data from: {url}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return ET.fromstring(response.content)
        except Exception as e:
            logger.error(f"Error fetching series data: {str(e)}")
            raise

    def get_value_description(self, concept: str, code: str) -> str:
        """Get description for a value, with proper fallback handling."""
        try:
            logger.debug(f"Getting description for concept: {concept}, code: {code}")
            
            if concept in self.constraints["dimensions"]:
                dimension_info = self.constraints["dimensions"][concept]
                value_info = dimension_info["values"].get(code, {})
                
                # Try to get name in different languages
                names = value_info.get("name", {})
                description = (
                    names.get("en") or  # Try English first
                    names.get("it") or  # Then Italian
                    names.get("default", code)  # Fall back to code if no translations
                )
                
                logger.debug(f"Found description for {concept}-{code}: {description}")
                return description
            
            logger.warning(f"No dimension info found for concept: {concept}")
            return code
        except Exception:
            logger.error(f"Error getting description for {concept}-{code}")
            return code

    def extract_series_key(self, series_elem: ET.Element) -> Dict:
        """Extract and map values from a series key element."""
        try:
            # Initialize with dataset_info at the top
            series_data = {
                "dataset_info": {
                    "id": self.dataset_id,
                    "names": self.constraints["dataset_info"]["names"],
                    "structure_reference": self.constraints["dataset_info"]["structure_reference"],
                    "generated_at": datetime.now().isoformat()
                },
                "metadata": {},
                "observations": []
            }
            
            # Extract series key values
            series_key = series_elem.find('generic:SeriesKey', self.namespaces)
            if series_key is not None:
                for value in series_key.findall('.//generic:Value', self.namespaces):
                    concept = value.attrib.get('id')
                    code = value.attrib.get('value')
                    
                    # Get description using the helper method
                    description = self.get_value_description(concept, code)
                    
                    series_data["metadata"][concept] = {
                        "code": code,
                        "description": description
                    }

            # Extract observations
            for obs in series_elem.findall('generic:Obs', self.namespaces):
                observation = {}
                
                # Get time period from ObsDimension
                time_dim = obs.find('generic:ObsDimension[@id="TIME_PERIOD"]', self.namespaces)
                if time_dim is not None:
                    observation["time_period"] = time_dim.attrib.get('value')
                
                # Get observation value from ObsValue
                obs_value = obs.find('generic:ObsValue', self.namespaces)
                if obs_value is not None:
                    value = obs_value.attrib.get('value')
                    if value:
                        try:
                            observation["value"] = float(value)
                        except ValueError:
                            observation["value"] = value
                
                # Only add observation if we have both time and value
                if "time_period" in observation and "value" in observation:
                    series_data["observations"].append(observation)
            
            # Sort observations by time period
            if series_data["observations"]:
                series_data["observations"].sort(key=lambda x: x.get("time_period", ""))
            
            return series_data
        except Exception as e:
            logger.error(f"Error extracting series key: {str(e)}")
            raise

    def process_series(self):
        """Process all series in the dataset."""
        try:
            # Fetch data
            root = self.fetch_series_data()
            
            # Find all series elements
            series_elements = root.findall('.//generic:Series', self.namespaces)
            total_series = len(series_elements)
            logger.info(f"Found {total_series} series to process")
            
            # Process each series
            for i, series_elem in enumerate(series_elements, 1):
                try:
                    logger.info(f"Processing series {i}/{total_series}")
                    
                    # Extract series data
                    series_data = self.extract_series_key(series_elem)
                    
                    # Skip if no observations found
                    if not series_data["observations"]:
                        logger.warning(f"No observations found for series {i}")
                        continue
                    
                    # Create unique filename based on metadata INCLUDING DATASET ID
                    filename_parts = [f"dataset_{self.dataset_id}"]
                    for concept, info in series_data["metadata"].items():
                        filename_parts.append(f"{concept}-{info['code']}")
                    
                    filename = f"series_{'_'.join(filename_parts)}.json"
                    output_path = os.path.join(self.output_dir, filename)
                    
                    # Save to file
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(series_data, f, indent=2, ensure_ascii=False)
                    
                    logger.info(f"Saved series data to: {output_path}")
                
                except Exception as e:
                    logger.error(f"Error processing series {i}: {str(e)}")
                    continue
            
            logger.info("Series extraction completed")
            
        except Exception as e:
            logger.error(f"Error processing series: {str(e)}")
            raise

def main():
    try:
        # Only need to specify the dataset ID
        dataset_id = "111_111"  # Change this to process a different dataset
        
        logger.info(f"Processing dataset: {dataset_id}")
        extractor = IstatSeriesExtractor(dataset_id)
        extractor.process_series()
        
    except FileNotFoundError as e:
        logger.critical(f"Constraints file error: {str(e)}")
    except Exception as e:
        logger.critical(f"Script execution failed: {str(e)}")

if __name__ == "__main__":
    main()