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
        """Get Xcel configuration - hardcoded Xcel defaults"""
        return {
            "power_company": "Xcel",
            "proposed_company": "Proposed MetroNet",
            "telecom_providers": [
                "Proposed MetroNet",
                "CATV",
                "Telephone Company",
                "Fiber",
                "CenturyLink"
            ],
            "power_keywords": [
                "Primary",
                "Secondary",
                "Neutral",
                "Transformer",
                "Secondary Drip Loop",
                "Riser",
                "CAP"
            ],
            "power_equipment_keywords": [
                "Riser",
                "Capacitor",
                "transformer bottom_of_equipment",
                "CAP"
            ],
            "comm_keywords": [
                "Guy",
                "Power Guy",
                "insulator*",
                "fiber",
                "telco",
                "catv"
            ],
            "street_light_keywords": [
                "street"
            ],
            "ignore_scid_keywords": [
                "AT&T",
                "Foreign Pole",
                "Unknown",
                "Xcel",
                "PCO",
                "LUMEN",
                "US WEST",
                "OTHER",
                "NWBT",
                "CENTURY LINK",
                "CENTURYLINK",
                "Transmission"
            ],
            "output_settings": {
                "header_row": 1,
                "data_start_row": 2,
                "worksheet_name": "1"
            },
            "processing_options": {
                "open_output": True,
                "output_decimal": True
            },
            "column_mappings": [
                ["Pole", "Number", "Pole"],
                ["Pole", "To Pole", "To Pole"],
                ["Pole", "Tag", "Pole Tag"],
                ["Pole", "Latitude", "Latitude"],
                ["Pole", "Longitude", "Longitude"],
                ["Pole", "Height & Class", "Pole Ht/ Class"],
                ["Power", "Lowest Height", "Lowest Power at Pole"],
                ["Power", "Lowest Midspan", "Lowest Power at Mid"],
                ["Power", "Lowest Type", "Lowest Power Type"],
                ["Street Light", "Lowest Height", "Street Light"],
                ["comm1", "Attachment Ht", "comm1"],
                ["comm2", "Attachment Ht", "comm2"],
                ["comm3", "Attachment Ht", "comm3"],
                ["Pole", "Number of Existing Risers", "# of Existing Risers"],
                ["Proposed MetroNet", "Attachment Ht", "Metro Attachment"],
                ["Proposed MetroNet", "Midspan Ht", "Metro Mid"],
                ["Span", "Length", "Span Length"],
                ["Pole", "MR Notes", "MR Notes"],
                ["Power Equipment", "Equipment List", "Power Equipments"],
                ["Pole", "Existing Structure Type", "Structure Type"],
                ["Pole", "Existing Loading", "Existing Load"],
                ["Pole", "Proposed Loading", "Proposed Load"],
                ["New Guy", "Required", "Guy Needed"],
                ["comm1", "Midspan Ht", "comm1 mid"],
                ["comm2", "Midspan Ht", "comm2 mid"],
                ["comm3", "Midspan Ht", "comm3 mid"]
            ]
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
            logging.error(f"Failed to save configuration to {self.config_file}: {e}", exc_info=True)
            return False