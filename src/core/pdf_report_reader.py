import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple
import PyPDF2
try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None  # PyMuPDF not available


class PDFReportReader:
    """Reads pole analysis PDF reports to extract structure type and loading data"""
    
    def __init__(self, existing_reports_folder: str = "", proposed_reports_folder: str = "", ignore_keywords: list = None):
        self.existing_reports_folder = Path(existing_reports_folder) if existing_reports_folder else None
        self.proposed_reports_folder = Path(proposed_reports_folder) if proposed_reports_folder else None
        self.ignore_keywords = ignore_keywords or []
        
    def extract_pole_data(self, pole_number: int) -> Dict[str, Optional[str]]:
        """
        Extract data for a specific pole from both existing and proposed reports
        
        Args:
            pole_number: The pole number (e.g., 1 for pole 001)
            
        Returns:
            Dictionary with keys: 'structure_type', 'existing_load', 'proposed_load'
        """
        result = {
            'structure_type': None,
            'existing_load': None,
            'proposed_load': None
        }
        
        # Format pole number as 3-digit string (e.g., 1 -> "001")
        pole_str = f"{pole_number:03d}"
        
        logging.debug(f"PDFReportReader: Extracting data for pole {pole_number} (formatted as {pole_str})")
        logging.debug(f"PDFReportReader: Existing folder: {self.existing_reports_folder}")
        logging.debug(f"PDFReportReader: Proposed folder: {self.proposed_reports_folder}")
        
        # Extract from existing reports
        if self.existing_reports_folder and self.existing_reports_folder.exists():
            logging.debug(f"PDFReportReader: Searching existing reports folder: {self.existing_reports_folder}")
            existing_data = self._extract_from_folder(self.existing_reports_folder, pole_str)
            if existing_data:
                result['structure_type'] = existing_data.get('structure_type')
                result['existing_load'] = existing_data.get('loading')
                logging.debug(f"PDFReportReader: Found existing data: {existing_data}")
            else:
                logging.debug(f"PDFReportReader: No existing data found for pole {pole_str}")
        else:
            logging.debug(f"PDFReportReader: Existing reports folder not available or doesn't exist")
        
        # Extract from proposed reports
        if self.proposed_reports_folder and self.proposed_reports_folder.exists():
            logging.debug(f"PDFReportReader: Searching proposed reports folder: {self.proposed_reports_folder}")
            proposed_data = self._extract_from_folder(self.proposed_reports_folder, pole_str)
            if proposed_data:
                # Use structure type from proposed if not found in existing
                if not result['structure_type']:
                    result['structure_type'] = proposed_data.get('structure_type')
                result['proposed_load'] = proposed_data.get('loading')
                logging.debug(f"PDFReportReader: Found proposed data: {proposed_data}")
            else:
                logging.debug(f"PDFReportReader: No proposed data found for pole {pole_str}")
        else:
            logging.debug(f"PDFReportReader: Proposed reports folder not available or doesn't exist")
        
        logging.debug(f"PDFReportReader: Final result for pole {pole_number}: {result}")
        return result
    
    def _extract_from_folder(self, folder_path: Path, pole_str: str) -> Optional[Dict[str, str]]:
        """
        Extract data from a specific folder for a given pole
        
        Args:
            folder_path: Path to the reports folder
            pole_str: Pole number as 3-digit string (e.g., "001")
            
        Returns:
            Dictionary with extracted data or None if no matching file found
        """
        logging.debug(f"PDFReportReader: Searching folder {folder_path} for pole {pole_str}")
        
        # List all PDF files in the folder for debugging
        all_pdf_files = list(folder_path.glob("*.pdf"))
        logging.debug(f"PDFReportReader: Found {len(all_pdf_files)} PDF files in folder")
        if all_pdf_files:
            logging.debug(f"PDFReportReader: Sample files: {[f.name for f in all_pdf_files[:5]]}")
        
        matching_files = []
        
        # Try the newest pattern first: 300_590833786_EXISTING_Analysis Report.pdf
        newest_pattern = f"{pole_str}_*.pdf"
        matching_files = list(folder_path.glob(newest_pattern))
        logging.debug(f"PDFReportReader: Newest pattern '{newest_pattern}' found {len(matching_files)} files")
        
        # If no files found with newest pattern, try the new pattern: Pole_307 _590833849_PROPOSED_Analysis Report.pdf
        if not matching_files:
            new_pattern = f"Pole_{pole_str}_*.pdf"
            matching_files = list(folder_path.glob(new_pattern))
            logging.debug(f"PDFReportReader: New pattern '{new_pattern}' found {len(matching_files)} files")
        
        # If no files found with new pattern, try the old pattern: Reports_Pole_001_*.pdf
        if not matching_files:
            old_pattern = f"Reports_Pole_{pole_str}_*.pdf"
            matching_files = list(folder_path.glob(old_pattern))
            logging.debug(f"PDFReportReader: Old pattern '{old_pattern}' found {len(matching_files)} files")
        
        # If no files found with exact match, try to find files with normalized pole numbers
        # This handles cases like "056 PCO" -> "056" after removing "PCO" keyword
        if not matching_files:
            logging.debug(f"PDFReportReader: Trying normalized pole search for {pole_str}")
            matching_files = self._find_files_with_normalized_pole(folder_path, pole_str)
            logging.debug(f"PDFReportReader: Normalized search found {len(matching_files)} files")
        
        if not matching_files:
            logging.debug(f"PDFReportReader: No PDF files found for pole {pole_str} in {folder_path}")
            return None
        
        # Use the first matching file
        pdf_file = matching_files[0]
        logging.debug(f"PDFReportReader: Processing PDF file: {pdf_file}")
        
        try:
            result = self._extract_from_pdf(pdf_file)
            logging.debug(f"PDFReportReader: Extracted data from PDF: {result}")
            return result
        except Exception as e:
            logging.error(f"Error processing PDF {pdf_file}: {e}")
            return None
    
    def _extract_from_pdf(self, pdf_path: Path) -> Dict[str, str]:
        """
        Extract structure type and loading data from a PDF file
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Dictionary with extracted data
        """
        result = {
            'structure_type': None,
            'loading': None
        }
        
        try:
            # Try PyMuPDF first (better text extraction)
            text = self._extract_text_pymupdf(pdf_path)
            if not text:
                # Fallback to PyPDF2
                text = self._extract_text_pypdf2(pdf_path)
            
            if text:
                result['structure_type'] = self._extract_structure_type(text)
                result['loading'] = self._extract_loading(text)
            
        except Exception as e:
            logging.error(f"Error extracting text from PDF {pdf_path}: {e}")
        
        return result
    
    def _extract_text_pymupdf(self, pdf_path: Path) -> str:
        """Extract text using PyMuPDF (fitz)"""
        if fitz is None:
            logging.debug("PyMuPDF not available, skipping PyMuPDF extraction")
            return ""
        
        try:
            doc = fitz.open(pdf_path)
            text = ""
            # Extract text from first page only
            if len(doc) > 0:
                page = doc[0]
                text = page.get_text()
            doc.close()
            return text
        except Exception as e:
            logging.debug(f"PyMuPDF extraction failed for {pdf_path}: {e}")
            return ""
    
    def _extract_text_pypdf2(self, pdf_path: Path) -> str:
        """Extract text using PyPDF2 (fallback)"""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                if len(pdf_reader.pages) > 0:
                    page = pdf_reader.pages[0]
                    return page.extract_text()
        except Exception as e:
            logging.debug(f"PyPDF2 extraction failed for {pdf_path}: {e}")
        return ""
    
    def _extract_structure_type(self, text: str) -> Optional[str]:
        """
        Extract structure type from PDF text by getting all text between 'Structure Type:' and 'Pole'
        and then cleaning it to remove 'Guyed' and 'Unguyed' words.
        
        This approach is more robust against PDF extraction artifacts that split text across lines.
        """
        # Pattern to extract text between "Structure Type:" and "Pole"
        # This handles cases where the structure type might be split across multiple lines
        pattern = r'Structure\s+Type:\s*(.*?)\s*Pole'
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        
        if match:
            # Get the text between "Structure Type:" and "Pole"
            structure_type_raw = match.group(1).strip()
            
            # Clean up the text by removing extra whitespace and newlines
            # Replace multiple whitespace/newlines with single spaces
            structure_type_cleaned = re.sub(r'\s+', ' ', structure_type_raw).strip()
            
            logging.debug(f"Found structure type (raw): '{structure_type_raw}'")
            logging.debug(f"Found structure type (cleaned): '{structure_type_cleaned}'")
            
            return structure_type_cleaned
        
        # Fallback: try to find just "Type:" followed by text until "Pole"
        fallback_pattern = r'Type:\s*(.*?)\s*Pole'
        match = re.search(fallback_pattern, text, re.IGNORECASE | re.DOTALL)
        
        if match:
            structure_type_raw = match.group(1).strip()
            structure_type_cleaned = re.sub(r'\s+', ' ', structure_type_raw).strip()
            logging.debug(f"Found structure type (fallback): '{structure_type_cleaned}'")
            return structure_type_cleaned
        
        logging.debug("No structure type found in PDF text")
        return None
    
    def _extract_loading(self, text: str) -> Optional[str]:
        """
        Extract loading data from PDF text
        
        Looks for capacity utilization percentages in the analysis results
        """
        # Pattern to match capacity utilization percentages
        # Look for "Maximum XX.X" or "Groundline XX.X" patterns (without colon)
        patterns = [
            r'Maximum\s+(\d+\.?\d*)',
            r'Groundline\s+(\d+\.?\d*)',
            r'Pole Capacity Utilization.*?Maximum\s+(\d+\.?\d*)',
            r'Pole Capacity Utilization.*?Groundline\s+(\d+\.?\d*)',
            # Also try with colon for backward compatibility
            r'Maximum:\s*(\d+\.?\d*)',
            r'Groundline:\s*(\d+\.?\d*)',
            r'Pole Capacity Utilization.*?Maximum:\s*(\d+\.?\d*)',
            r'Pole Capacity Utilization.*?Groundline:\s*(\d+\.?\d*)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                loading = match.group(1)
                logging.debug(f"Found loading: {loading}%")
                return f"{loading}%"
        
        # Alternative: look for any percentage values that might be loading
        percentage_pattern = r'(\d+\.?\d*)\s*%'
        matches = re.findall(percentage_pattern, text)
        
        if matches:
            # Return the highest percentage found (likely the maximum loading)
            max_loading = max([float(m) for m in matches])
            logging.debug(f"Found loading (alt method): {max_loading}%")
            return f"{max_loading}%"
        
        logging.debug("No loading data found in PDF text")
        return None
    
    def _find_files_with_normalized_pole(self, folder_path: Path, pole_str: str) -> list:
        """
        Find PDF files that contain the pole number after normalizing filenames
        by removing ignore keywords. This handles cases like "056 PCO" -> "056"
        
        Args:
            folder_path: Path to the reports folder
            pole_str: Pole number as 3-digit string (e.g., "001")
            
        Returns:
            List of matching PDF files
        """
        from .utils import Utils
        
        # Use ignore keywords from instance variable
        ignore_keywords = self.ignore_keywords
        if not ignore_keywords:
            # Default ignore keywords if not set
            ignore_keywords = ["PCO", "AT&T", "Foreign Pole", "Unknown", "Xcel"]
        
        matching_files = []
        
        # Get all PDF files in the folder - check all patterns
        newest_pattern_files = list(folder_path.glob("*_*_*.pdf"))  # 300_590833786_EXISTING_Analysis Report.pdf
        old_pattern_files = list(folder_path.glob("Reports_Pole_*_*.pdf"))
        new_pattern_files = list(folder_path.glob("Pole_*_*.pdf"))
        all_pdf_files = newest_pattern_files + old_pattern_files + new_pattern_files
        
        for pdf_file in all_pdf_files:
            pole_part = None
            
            # Try to extract pole part from newest pattern: 300_590833786_EXISTING_Analysis Report.pdf
            newest_match = re.search(r'^(\d{3})_', pdf_file.name)
            if newest_match:
                pole_part = newest_match.group(1)
            else:
                # Try to extract pole part from pattern with space: 118 PCO_346094539_EXISTING.PDF
                space_pattern_match = re.search(r'^(\d{3})\s+\w+_', pdf_file.name)
                if space_pattern_match:
                    pole_part = space_pattern_match.group(1)
                else:
                    # Try to extract pole part from old pattern: Reports_Pole_056 PCO_...
                    old_match = re.search(r'Reports_Pole_([^_]+)_', pdf_file.name)
                    if old_match:
                        pole_part = old_match.group(1)
                    else:
                        # Try to extract pole part from new pattern: Pole_307 _590833849_PROPOSED_...
                        new_match = re.search(r'Pole_([^_\s]+)', pdf_file.name)
                        if new_match:
                            pole_part = new_match.group(1)
            
            if pole_part:
                # Normalize the pole part by removing ignore keywords
                normalized_pole = Utils.normalize_scid(pole_part, ignore_keywords)
                
                # Check if normalized pole matches our target pole
                # Convert both to 3-digit format for comparison
                normalized_pole_3digit = f"{int(normalized_pole):03d}"
                if normalized_pole_3digit == pole_str:
                    matching_files.append(pdf_file)
                    logging.debug(f"Found normalized match: {pdf_file.name} -> {normalized_pole} -> {normalized_pole_3digit}")
        
        return matching_files
    
    def get_available_poles(self) -> set:
        """
        Get set of pole numbers that have PDF reports available
        
        Returns:
            Set of pole numbers (integers)
        """
        poles = set()
        
        for folder_path in [self.existing_reports_folder, self.proposed_reports_folder]:
            if folder_path and folder_path.exists():
                # Look for all PDF files matching all patterns
                newest_pattern_files = list(folder_path.glob("*_*_*.pdf"))  # 300_590833786_EXISTING_Analysis Report.pdf
                old_pattern_files = list(folder_path.glob("Reports_Pole_*_*.pdf"))
                new_pattern_files = list(folder_path.glob("Pole_*_*.pdf"))
                pdf_files = newest_pattern_files + old_pattern_files + new_pattern_files
                
                for pdf_file in pdf_files:
                    # Try to extract pole number from newest pattern first: 300_590833786_EXISTING_Analysis Report.pdf
                    newest_match = re.search(r'^(\d{3})_', pdf_file.name)
                    if newest_match:
                        pole_num = int(newest_match.group(1))
                        poles.add(pole_num)
                    else:
                        # Try to extract pole number from pattern with space: 118 PCO_346094539_EXISTING.PDF
                        space_pattern_match = re.search(r'^(\d{3})\s+\w+_', pdf_file.name)
                        if space_pattern_match:
                            pole_num = int(space_pattern_match.group(1))
                            poles.add(pole_num)
                        else:
                            # Try to extract pole number from old pattern
                            old_match = re.search(r'Reports_Pole_(\d{3})_', pdf_file.name)
                            if old_match:
                                pole_num = int(old_match.group(1))
                                poles.add(pole_num)
                            else:
                                # Try to extract pole number from new pattern
                                new_match = re.search(r'Pole_(\d{3})', pdf_file.name)
                                if new_match:
                                    pole_num = int(new_match.group(1))
                                    poles.add(pole_num)
        
        return poles
