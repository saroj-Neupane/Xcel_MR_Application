import pandas as pd
import logging
from pathlib import Path


class AldenQCReader:
    """Reads and processes Alden QC Excel files for pole comparison"""
    
    def __init__(self, alden_qc_file_path=None):
        """
        Initialize Alden QC reader with QC file path
        
        Args:
            alden_qc_file_path (str, optional): Path to Alden QC Excel file
        """
        self.alden_qc_file_path = alden_qc_file_path
        self.qc_data = {}  # Dict mapping normalized pole_number to mr_notes
        self.metronet_heights = {}  # Dict mapping normalized pole_number to dict with 'attachment_height' and 'midspan_height'
        self.power_heights = {}  # Dict mapping normalized pole_number to dict with 'attachment_height' and 'midspan_height'
        self.comm_heights = {}  # Dict mapping normalized pole_number to list of dicts with 'comm_number', 'attachment_height' and 'midspan_height'
        self._active = False
        self._raw_dataframe = None  # Store the raw DataFrame from Alden file
        
        if alden_qc_file_path:
            self.load_alden_qc_file(alden_qc_file_path)
    
    def _normalize_pole_number(self, pole_number):
        """
        Normalize pole number by removing leading zeros
        
        Args:
            pole_number (str or int): Pole number to normalize
            
        Returns:
            str: Normalized pole number
        """
        if not pole_number:
            return ""
        # Convert to string and remove leading zeros
        pole_str = str(pole_number).strip()
        # Remove leading zeros but keep at least one digit
        if pole_str.startswith('0') and len(pole_str) > 1:
            pole_str = pole_str.lstrip('0')
        return pole_str if pole_str else "0"
    
    def _extract_notes_after_colon(self, notes):
        """
        Extract MR notes text after the colon if colon exists
        
        Args:
            notes (str): Full MR notes string
            
        Returns:
            str: MR notes text after colon, or original string if no colon
        """
        if not notes:
            return ""
        notes_str = str(notes).strip()
        # Check if colon exists
        if ':' in notes_str:
            # Split by colon and take everything after the first colon
            parts = notes_str.split(':', 1)
            if len(parts) > 1:
                return parts[1].strip()
        return notes_str
    
    def _parse_height_to_decimal(self, height_str):
        """
        Parse height string to decimal feet for sorting
        Examples: "22ft 1in" -> 22.08, "23ft 7in" -> 23.58
        
        Args:
            height_str (str): Height string in format like "22ft 1in"
            
        Returns:
            float: Decimal feet, or 0 if parsing fails
        """
        try:
            if not height_str:
                return 0.0
            
            height_str = str(height_str).strip()
            
            # Parse format like "22ft 1in"
            import re
            match = re.match(r"(\d+)ft\s*(\d+)in", height_str)
            if match:
                feet = int(match.group(1))
                inches = int(match.group(2))
                return feet + (inches / 12.0)
            
            return 0.0
            
        except Exception as e:
            logging.debug(f"Error parsing height '{height_str}': {e}")
            return 0.0
    
    def load_alden_qc_file(self, alden_qc_file_path):
        """
        Load Alden QC file and extract pole/MR notes data
        
        Args:
            alden_qc_file_path (str): Path to Alden QC Excel file
        """
        try:
            qc_path = Path(alden_qc_file_path)
            if not qc_path.exists():
                logging.error(f"Alden QC file not found: {alden_qc_file_path}")
                self._active = False
                return
            
            # Read the specific sheet with header in row 3
            df = pd.read_excel(alden_qc_file_path, sheet_name='Poles_Joint Use Attachment', header=2)
            logging.info(f"Loaded {len(df)} Alden QC rows")
            
            # Store the raw DataFrame for later use
            self._raw_dataframe = df
            
            # Clear existing data
            self.qc_data.clear()
            self.metronet_heights.clear()
            self.power_heights.clear()
            self.comm_heights.clear()
            
            # Extract Pole Number and MR Notes columns
            pole_col = 'DesignSketchReferenceNumber'
            mr_notes_col = 'MakeReadyNotes'
            company_col = 'CompanyName'
            height_col = '_Height'
            midspan_col = 'MidSpan'
            status_col = 'Status'
            attachment_type_col = 'AttachmentType'
            
            if pole_col not in df.columns:
                logging.error(f"Pole column '{pole_col}' not found in Alden QC file")
                self._active = False
                return
            
            if mr_notes_col not in df.columns:
                logging.error(f"MR Notes column '{mr_notes_col}' not found in Alden QC file")
                self._active = False
                return
            
            # First pass: Create mapping of pole number to MR notes and extract specific heights
            for _, row in df.iterrows():
                pole_number = str(row[pole_col]).strip() if pd.notna(row[pole_col]) else ""
                mr_notes = str(row[mr_notes_col]).strip() if pd.notna(row[mr_notes_col]) else ""
                
                if pole_number and pole_number != "nan":
                    # Normalize pole number and extract notes after colon
                    normalized_pole = self._normalize_pole_number(pole_number)
                    extracted_notes = self._extract_notes_after_colon(mr_notes)
                    self.qc_data[normalized_pole] = extracted_notes
                    
                    # Extract company name
                    company_name = str(row[company_col]).strip() if pd.notna(row.get(company_col, '')) else ""
                    
                    # Extract MetroNet heights
                    if "Metronet Fiber LLC" in company_name:
                        attachment_height = str(row[height_col]).strip() if pd.notna(row.get(height_col, '')) else ""
                        midspan_height = str(row[midspan_col]).strip() if pd.notna(row.get(midspan_col, '')) else ""
                        
                        # Initialize pole entry if not exists
                        if normalized_pole not in self.metronet_heights:
                            self.metronet_heights[normalized_pole] = {
                                'attachment_height': attachment_height,
                                'midspan_height': midspan_height
                            }
                    
                    # Extract Power heights
                    if "XCEL ENERGY" in company_name:
                        attachment_height = str(row[height_col]).strip() if pd.notna(row.get(height_col, '')) else ""
                        midspan_height = str(row[midspan_col]).strip() if pd.notna(row.get(midspan_col, '')) else ""
                        attachment_type = str(row.get(attachment_type_col, '')).strip() if pd.notna(row.get(attachment_type_col, '')) else ""
                        
                        # Initialize pole entry if not exists
                        if normalized_pole not in self.power_heights:
                            self.power_heights[normalized_pole] = {
                                'attachment_height': attachment_height,
                                'midspan_height': midspan_height,
                                'attachment_type': attachment_type
                            }
                        # Update attachment_type if this row has the lowest height
                        elif attachment_height:
                            # Compare heights and keep the lowest one's attachment_type
                            current_height = self.power_heights[normalized_pole].get('attachment_height', '')
                            if current_height:
                                # Parse both heights to compare
                                current_decimal = self._parse_height_to_decimal(current_height)
                                new_decimal = self._parse_height_to_decimal(attachment_height)
                                if new_decimal > 0 and (current_decimal == 0 or new_decimal < current_decimal):
                                    # This is lower, update everything
                                    self.power_heights[normalized_pole]['attachment_height'] = attachment_height
                                    self.power_heights[normalized_pole]['midspan_height'] = midspan_height
                                    self.power_heights[normalized_pole]['attachment_type'] = attachment_type
                            else:
                                # No current height, set this one
                                self.power_heights[normalized_pole]['attachment_height'] = attachment_height
                                self.power_heights[normalized_pole]['midspan_height'] = midspan_height
                                self.power_heights[normalized_pole]['attachment_type'] = attachment_type
            
            # Second pass: Extract and assign comm heights based on attachment heights
            # First collect all comm attachments per pole
            temp_comm_by_pole = {}  # pole_number -> list of (attachment_height, midspan_height)
            for _, row in df.iterrows():
                pole_number = str(row[pole_col]).strip() if pd.notna(row[pole_col]) else ""
                status = str(row.get(status_col, '')).strip() if pd.notna(row.get(status_col, '')) else ""
                attachment_type = str(row.get(attachment_type_col, '')).strip() if pd.notna(row.get(attachment_type_col, '')) else ""
                
                if pole_number and pole_number != "nan":
                    normalized_pole = self._normalize_pole_number(pole_number)
                    
                    # Check if this is a communication attachment (EXISTING with Coax or Communication Fiber-Optic)
                    # Case-insensitive comparison
                    if status.upper() == "EXISTING" and attachment_type.upper() in ["COAX", "COMMUNICATION FIBER-OPTIC"]:
                        attachment_height = str(row[height_col]).strip() if pd.notna(row.get(height_col, '')) else ""
                        midspan_height = str(row[midspan_col]).strip() if pd.notna(row.get(midspan_col, '')) else ""
                        
                        if attachment_height:
                            if normalized_pole not in temp_comm_by_pole:
                                temp_comm_by_pole[normalized_pole] = []
                            temp_comm_by_pole[normalized_pole].append({
                                'attachment_height': attachment_height,
                                'midspan_height': midspan_height,
                                'height_decimal': self._parse_height_to_decimal(attachment_height)
                            })
            
            # Now assign comm numbers based on height (descending order)
            for normalized_pole, comm_list in temp_comm_by_pole.items():
                # Sort by height in descending order
                comm_list.sort(key=lambda x: x['height_decimal'], reverse=True)
                
                # Assign comm1, comm2, comm3
                self.comm_heights[normalized_pole] = []
                for i, comm in enumerate(comm_list[:3], start=1):  # Limit to 3 comm fields
                    self.comm_heights[normalized_pole].append({
                        'comm_number': i,
                        'attachment_height': comm['attachment_height'],
                        'midspan_height': comm['midspan_height']
                    })
            
            self._active = len(self.qc_data) > 0
            self.alden_qc_file_path = alden_qc_file_path
            
            logging.info(f"Loaded {len(self.qc_data)} poles, {len(self.metronet_heights)} MetroNet, {len(self.comm_heights)} comm")
            
        except Exception as e:
            logging.error(f"Error loading Alden QC file {alden_qc_file_path}: {e}", exc_info=True)
            self._active = False
    
    def is_active(self):
        """Check if Alden QC reader has valid data"""
        return self._active
    
    def get_mr_notes(self, pole_number):
        """
        Get MR notes for a specific pole number
        
        Args:
            pole_number (str): Pole number to look up
            
        Returns:
            str: MR notes for the pole, or empty string if not found
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        return self.qc_data.get(normalized_pole, "")
    
    def has_pole(self, pole_number):
        """
        Check if a pole number exists in the QC data
        
        Args:
            pole_number (str): Pole number to check
            
        Returns:
            bool: True if pole exists in QC data
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        return normalized_pole in self.qc_data
    
    def get_all_poles(self):
        """
        Get all pole numbers from the QC data
        
        Returns:
            list: List of all pole numbers
        """
        return list(self.qc_data.keys())
    
    def get_metronet_attachment_height(self, pole_number):
        """
        Get MetroNet attachment height for a specific pole number
        
        Args:
            pole_number (str): Pole number to look up
            
        Returns:
            str: Attachment height for the pole, or empty string if not found
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        pole_data = self.metronet_heights.get(normalized_pole, {})
        return pole_data.get('attachment_height', '')
    
    def get_metronet_midspan_height(self, pole_number):
        """
        Get MetroNet midspan height for a specific pole number
        
        Args:
            pole_number (str): Pole number to look up
            
        Returns:
            str: Midspan height for the pole, or empty string if not found
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        pole_data = self.metronet_heights.get(normalized_pole, {})
        return pole_data.get('midspan_height', '')
    
    def has_metronet_data(self, pole_number):
        """
        Check if MetroNet height data exists for a pole number
        
        Args:
            pole_number (str): Pole number to check
            
        Returns:
            bool: True if MetroNet data exists for the pole
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        return normalized_pole in self.metronet_heights
    
    def get_power_attachment_height(self, pole_number):
        """
        Get Power attachment height for a specific pole number
        
        Args:
            pole_number (str): Pole number to look up
            
        Returns:
            str: Attachment height for the pole, or empty string if not found
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        pole_data = self.power_heights.get(normalized_pole, {})
        return pole_data.get('attachment_height', '')
    
    def get_power_midspan_height(self, pole_number):
        """
        Get Power midspan height for a specific pole number
        
        Args:
            pole_number (str): Pole number to look up
            
        Returns:
            str: Midspan height for the pole, or empty string if not found
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        pole_data = self.power_heights.get(normalized_pole, {})
        return pole_data.get('midspan_height', '')
    
    def has_power_data(self, pole_number):
        """
        Check if Power height data exists for a pole number
        
        Args:
            pole_number (str): Pole number to check
            
        Returns:
            bool: True if Power data exists for the pole
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        return normalized_pole in self.power_heights
    
    def get_power_attachment_type(self, pole_number):
        """
        Get Power attachment type for a specific pole number
        
        Args:
            pole_number (str): Pole number to look up
            
        Returns:
            str: Attachment type for the pole, or empty string if not found
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        pole_data = self.power_heights.get(normalized_pole, {})
        return pole_data.get('attachment_type', '')
    
    def get_comm_attachment_height(self, pole_number, comm_number):
        """
        Get communication attachment height for a specific pole number and comm number
        
        Args:
            pole_number (str): Pole number to look up
            comm_number (int): Comm number (1, 2, or 3)
            
        Returns:
            str: Attachment height for the specified comm, or empty string if not found
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        comm_list = self.comm_heights.get(normalized_pole, [])
        for comm in comm_list:
            if comm['comm_number'] == comm_number:
                return comm.get('attachment_height', '')
        return ''
    
    def get_comm_midspan_height(self, pole_number, comm_number):
        """
        Get communication midspan height for a specific pole number and comm number
        
        Args:
            pole_number (str): Pole number to look up
            comm_number (int): Comm number (1, 2, or 3)
            
        Returns:
            str: Midspan height for the specified comm, or empty string if not found
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        comm_list = self.comm_heights.get(normalized_pole, [])
        for comm in comm_list:
            if comm['comm_number'] == comm_number:
                return comm.get('midspan_height', '')
        return ''
    
    def has_comm_data(self, pole_number):
        """
        Check if Communication height data exists for a pole number
        
        Args:
            pole_number (str): Pole number to check
            
        Returns:
            bool: True if Communication data exists for the pole
        """
        normalized_pole = self._normalize_pole_number(pole_number)
        return normalized_pole in self.comm_heights and len(self.comm_heights[normalized_pole]) > 0
    
    def get_raw_dataframe(self):
        """
        Get the raw DataFrame from the Alden file
        
        Returns:
            pd.DataFrame: Raw DataFrame from Alden file, or None if not loaded
        """
        return self._raw_dataframe

