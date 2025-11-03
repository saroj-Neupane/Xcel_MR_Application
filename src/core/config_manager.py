import json
import logging
from pathlib import Path

class ConfigManager:
    """Manages Xcel-specific configuration loading and saving"""
    
    def __init__(self, base_dir=None):
        if base_dir is None:
            base_dir = Path.cwd()
        self.base_dir = Path(base_dir)
        self.config_file = self.base_dir / "xcel_config.json"
    
    def get_default_config(self):
        """Get Xcel configuration - hardcoded Xcel.json values"""
        return {
            "power_company": "Xcel",
            "proposed_company": "",
            "telecom_providers": [
                "Proposed MetroNet",
                "CATV",
                "Telephone Company",
                "Fiber"
            ],
            "power_keywords": [
                "Primary",
                "Secondary",
                "Neutral",
                "Transformer",
                "Secondary Drip Loop",
                "Riser"
            ],
            "power_equipment_keywords": [
                "Transformer",
                "Riser",
                "Capacitor"
            ],
            "comm_keywords": [
                "catv com",
                "telco com",
                "fiber optic com",
                "insulator",
                "power guy",
                "catv",
                "telco",
                "fiber",
                "communication",
                "comm"
            ],
            "ignore_scid_keywords": [
                "AT&T",
                "Foreign Pole",
                "Unknown",
                "Xcel"
            ],
            "telecom_keywords": {
                "Proposed MetroNet": [
                    "Proposed MNT",
                    "MNT",
                    "MetroNet"
                ],
                "Comcast": [
                    "Com Cast",
                    "Commcast",
                    "comcast"
                ],
                "AT&T": [
                    "AT&T",
                    "ATT",
                    "att"
                ],
                "Lightower": [
                    "lightower",
                    "Lightower"
                ],
                "Kellogg": [
                    "kellogg",
                    "Kellogg"
                ],
                "MCI": [
                    "mci",
                    "MCI"
                ],
                "CATV": [
                    "CATV"
                ],
                "Telephone Company": [
                    "telephone company",
                    "telco"
                ],
                "Fiber": [
                    "Fiber",
                    "fiber"
                ]
            },
            "output_settings": {
                "header_row": 1,
                "data_start_row": 2,
                "worksheet_name": "1"
            },
            "processing_options": {
                "open_output": True,
                "debug_mode": False,
                "output_decimal": True
            },
            "data_sources": {
                "attachment_data": {
                    "source": "SCID sheets",
                    "description": "Attachment data comes exclusively from individual SCID sheets (e.g., 'SCID 123') in the attachment data file"
                },
                "midspan_height": {
                    "source": "sections sheet",
                    "description": "Midspan height data comes exclusively from the 'sections' sheet in the main input file"
                },
                "span_length": {
                    "source": "connections sheet", 
                    "description": "Span length data comes exclusively from the 'connections' sheet in the main input file"
                },
                "pole_tag": {
                    "source": "pole_tag_tagtext column in nodes sheet",
                    "description": "Pole Tag data comes exclusively from the 'pole_tag_tagtext' column in the 'nodes' sheet"
                },
                "pole_height_class": {
                    "source": "pole_spec column in nodes sheet",
                    "description": "Pole Height&Class data comes exclusively from the 'pole_spec' column in the 'nodes' sheet (e.g., '35-4 SOUTHERN PINE' -> '35/4')"
                },
                "single_source_only": True,
                "blank_missing_data": True,
                "description": "No fallback mechanisms - each data type uses only its designated source sheet. Missing data is left blank in output."
            },
            "column_mappings": [
                [
                    "Pole",
                    "Number",
                    "Pole No. (on Map)"
                ],
                [
                    "Pole",
                    "Tag",
                    "Xcel Energy GIS Pole ID"
                ],
                [
                    "Pole",
                    "Latitude",
                    "Latitude\n\n"
                ],
                [
                    "Pole",
                    "Longitude",
                    "Longitude\n\n"
                ],
                [
                    "Pole",
                    "Height & Class",
                    "Pole Ht/ Class"
                ],
                [
                    "Power",
                    "Lowest Height",
                    "Lowest Power at Pole"
                ],
                [
                    "Power",
                    "Lowest Height",
                    "Lowest Power Cable at Mid-span"
                ],
                [
                    "Power",
                    "Lowest Type",
                    "Lowest Power Type"
                ],
                [
                    "Street Light",
                    "Lowest Height",
                    "Street Light Bracket / Drip Loop (Lowest)"
                ],
                [
                    "comm1",
                    "Midspan Ht",
                    "Highest Existing Communic. At Pole (1)"
                ],
                [
                    "comm2",
                    "Attachment Ht",
                    "Existing Communic. At Pole (2)"
                ],
                [
                    "comm3",
                    "Attachment Ht",
                    "Existing Communic. At Pole (3)"
                ],
                [
                    "Pole",
                    "Number of Existing Risers",
                    "# of Existing Risers"
                ],
                [
                    "Proposed MetroNet",
                    "Attachment Ht",
                    "Proposed Attachment Height for New Cable"
                ],
                [
                    "Proposed MetroNet",
                    "Midspan Ht",
                    "Proposed Mid-span Cable Height (Must meet Sht J-6 min. and local ord.)"
                ],
                [
                    "Pole",
                    "MR Notes",
                    "Proposed Modifications / Make-Ready Necessary to Allow for Attachment"
                ],
                [
                    "New Guy",
                    "Required",
                    "New Guy Required"
                ]
            ],
            "decimal_measurements": False
        }
    
    def load_config(self):
        """Load Xcel configuration"""
        config = self.get_default_config()
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    loaded = json.load(f)
                    config.update(loaded)
                logging.info(f"Xcel configuration successfully loaded from {self.config_file}")
            except Exception as e:
                logging.warning(f"Failed to load configuration from {self.config_file}: {e}")
        
        return config
    
    def save_config(self, config):
        """Save Xcel configuration"""
        try:
            self.config_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
            
            logging.info(f"Xcel configuration successfully saved to {self.config_file}")
            return True
        except Exception as e:
            logging.error(f"Failed to save configuration to {self.config_file}: {e}")
            return False