import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
from typing import Dict, List, Optional
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IstatSeriesPlotter:
    def __init__(self, series_dir: str = "./series", output_dir: str = "./images"):
        self.series_dir = Path(series_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        sns.set_theme()

    def load_series(self, file_path: Path) -> Dict:
        """Load a series JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            raise

    def get_series_description(self, series_data: Dict) -> str:
        """Generate a comprehensive description from metadata and dataset info."""
        try:
            # Get dataset name in English
            dataset_name = series_data.get('dataset_info', {}).get('names', {}).get('en', '')
            
            # Get metadata descriptions
            metadata = series_data.get('metadata', {})
            descriptions = []
            
            # Add frequency
            if freq := metadata.get('FREQ', {}).get('description'):
                descriptions.append(f"Frequency: {freq}")
            
            # Add adjustment type
            if adj := metadata.get('ADJUSTMENT', {}).get('description'):
                descriptions.append(f"Adjustment: {adj}")
            
            # Add specific data type description if available
            tipo_dato = metadata.get('TIPO_DATO', {}).get('description')
            tipo_aggr = metadata.get('TIPO_AGGR_MILEURO', {}).get('description')
            data_type = tipo_dato or tipo_aggr
            if data_type:
                descriptions.append(f"Type: {data_type}")
            
            # Add geographic coverage
            if territory := metadata.get('ITTER107', {}).get('description'):
                descriptions.append(f"Territory: {territory}")
            
            # Combine all information
            return f"{dataset_name}\n{' | '.join(descriptions)}"
            
        except Exception as e:
            logger.error(f"Error generating description: {e}")
            return "Series Plot"

    def create_series_df(self, series_data: Dict) -> pd.DataFrame:
        """Convert series data to DataFrame with proper datetime index."""
        try:
            df = pd.DataFrame(series_data['observations'])
            
            # Handle different date formats
            try:
                # First try quarterly format (e.g., '1995-Q1')
                df['time_period'] = pd.to_datetime(df['time_period'].str.replace('Q', ''))
            except:
                try:
                    # Try annual format
                    df['time_period'] = pd.to_datetime(df['time_period'], format='%Y')
                except:
                    # Fallback to general parsing
                    df['time_period'] = pd.to_datetime(df['time_period'])
            
            df.set_index('time_period', inplace=True)
            df.sort_index(inplace=True)
            return df
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}")
            raise

    def plot_series(self, series_data: Dict, output_filename: str):
        """Create a detailed plot of the time series."""
        try:
            # Create DataFrame
            df = self.create_series_df(series_data)
            
            # Get frequency from metadata
            freq_code = series_data.get('metadata', {}).get('FREQ', {}).get('code', 'A')
            
            # Create figure
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), height_ratios=[2, 1])
            
            # Main time series plot
            df['value'].plot(ax=ax1, linewidth=2)
            
            # Generate and set title
            title = self.get_series_description(series_data)
            ax1.set_title(title, pad=20, wrap=True)
            
            # Format axes
            ax1.grid(True, alpha=0.3)
            ax1.set_xlabel('')
            
            # Calculate and plot growth rate
            periods = 4 if freq_code == 'Q' else 1
            yoy = df['value'].pct_change(periods=periods) * 100
            yoy.plot(ax=ax2, color='green', linewidth=2)
            
            ax2.axhline(y=0, color='r', linestyle='--', alpha=0.3)
            growth_title = 'Quarter-over-Quarter Growth Rate (%)' if freq_code == 'Q' else 'Year-over-Year Growth Rate (%)'
            ax2.set_title(growth_title)
            ax2.grid(True, alpha=0.3)
            
            # Adjust layout
            plt.tight_layout()
            
            # Save plot
            output_path = self.output_dir / output_filename
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Plot saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Error creating plot: {e}")
            raise

    def generate_output_filename(self, series_data: Dict, file_path: Path) -> str:
        """Generate a descriptive output filename."""
        try:
            # Get series identifier
            series_id = series_data.get('dataset_info', {}).get('id', '')
            
            # Get data type identifier
            metadata = series_data.get('metadata', {})
            tipo_dato = metadata.get('TIPO_DATO', {}).get('code', '')
            tipo_aggr = metadata.get('TIPO_AGGR_MILEURO', {}).get('code', '')
            data_type = tipo_dato or tipo_aggr
            
            # Get frequency
            freq = metadata.get('FREQ', {}).get('code', '')
            
            # Generate filename
            filename_parts = [p for p in [series_id, freq, data_type] if p]
            if filename_parts:
                return f"{'_'.join(filename_parts)}.png"
            else:
                return f"{file_path.stem}.png"
            
        except Exception as e:
            logger.error(f"Error generating filename: {e}")
            return f"{file_path.stem}.png"

    def process_files(self):
        """Process all JSON files in the series directory."""
        try:
            # Get all JSON files
            json_files = list(self.series_dir.glob('**/*.json'))
            
            if not json_files:
                logger.warning(f"No JSON files found in {self.series_dir}")
                return
            
            logger.info(f"Found {len(json_files)} JSON files to process")
            
            for file_path in json_files:
                try:
                    logger.info(f"Processing {file_path}")
                    
                    # Load series data
                    series_data = self.load_series(file_path)
                    
                    # Generate output filename
                    output_filename = self.generate_output_filename(series_data, file_path)
                    
                    # Create plot
                    self.plot_series(series_data, output_filename)
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error processing files: {e}")
            raise

def main():
    try:
        # Get the directory of the current script (src folder)
        script_dir = Path(__file__).resolve().parent
        
        # Get the parent directory (project root)
        project_root = script_dir.parent
        
        # Define paths relative to project root
        series_dir = project_root / "data" / "series_data"
        output_dir = project_root / "img"
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Using series directory: {series_dir}")
        logger.info(f"Using output directory: {output_dir}")
        
        # Initialize plotter
        plotter = IstatSeriesPlotter(
            series_dir=str(series_dir),
            output_dir=str(output_dir)
        )
        
        # Process all files
        plotter.process_files()
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":
    main()