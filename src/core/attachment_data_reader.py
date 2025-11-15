from pathlib import Path
import pandas as pd
import json
import logging
import re
try:
    from .utils import Utils
except ImportError:
    from utils import Utils

class AttachmentDataReader:
    """Handles reading attachment data from the new Excel format"""
    
    def __init__(self, file_path, config=None, valid_scids=None):
        self.file_path = file_path
        self.attachment_data = {}
        self.config = config or {}
        self.valid_scids = set(valid_scids) if valid_scids else None
        self.load_attachment_data()
    
    def load_attachment_data(self):
        """Load attachment data from Excel file with SCID sheets.
           Sheet names are expected to be 'SCID <scid>' where <scid> is already filtered.
        """
        try:
            xls = pd.ExcelFile(self.file_path)
            scid_sheets = [sheet for sheet in xls.sheet_names if sheet.startswith("SCID ")]
            
            logging.info(f"AttachmentDataReader: Discovered {len(scid_sheets)} SCID sheet(s) in {self.file_path}")
            
            for sheet_name in scid_sheets:
                scid = sheet_name[5:].strip()
                ignore_keywords = self.config.get('ignore_scid_keywords', [])
                scid = Utils.normalize_scid(scid, ignore_keywords)
                
                if self.valid_scids is not None and scid not in self.valid_scids:
                    logging.debug(f"AttachmentDataReader: Skipping sheet '{sheet_name}' because SCID '{scid}' is not in the valid set")
                    continue
                
                try:
                    df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=1)
                    df = df.fillna("")
                    
                    logging.info(f"AttachmentDataReader: Loaded {len(df)} record(s) for SCID '{scid}' from sheet '{sheet_name}'")
                    
                    df.columns = df.columns.str.strip().str.lower()
                    required_cols = ['company', 'measured', 'height_in_inches']
                    missing_cols = [col for col in required_cols if col not in df.columns]
                    if missing_cols:
                        logging.warning(f"AttachmentDataReader: Sheet '{sheet_name}' missing columns: {missing_cols}. Available: {list(df.columns)}")
                        continue
                    
                    self.attachment_data[scid] = df
                except Exception as e:
                    logging.error(f"AttachmentDataReader: Error reading sheet '{sheet_name}': {e}")
            
            logging.info(f"AttachmentDataReader: Total valid SCIDs loaded: {len(self.attachment_data)}")
            if not self.attachment_data:
                logging.error("AttachmentDataReader: No valid SCID data loaded from the attachment file!")
        except Exception as e:
            logging.error(f"AttachmentDataReader: Failed to load attachment data from {self.file_path}: {e}")
    
    def get_scid_data(self, scid):
        """Get attachment data for a specific SCID"""
        ignore_keywords = self.config.get('ignore_scid_keywords', [])
        normalized_scid = Utils.normalize_scid(scid, ignore_keywords)
        data = self.attachment_data.get(normalized_scid, pd.DataFrame())
        if data.empty:
            logging.debug(f"No attachment data found for SCID {scid} (normalized: {normalized_scid})")
        return data

    def find_power_attachment(self, scid, power_keywords):
        """Find the lowest power attachment for a SCID"""
        df = self.get_scid_data(scid)
        if df.empty:
            return None
        try:
            power_company = self.config.get("power_company", "").strip().lower()
            keyword_map = [
                (kw.strip().lower(), kw)
                for kw in power_keywords
                if isinstance(kw, str) and kw.strip()
            ]
            if not keyword_map:
                return None
            
            df['company_stripped'] = df['company'].astype(str).str.strip().str.lower()
            df['measured_stripped'] = df['measured'].astype(str).str.strip().str.lower()
            
            if power_company:
                power_company_pattern = r'\b' + re.escape(power_company) + r'\b'
                company_mask = df['company_stripped'].str.contains(power_company_pattern, na=False, regex=True)
                blank_mask = df['company_stripped'].eq('')
                candidate_rows = df[company_mask | blank_mask].copy()
            else:
                power_company_pattern = None
                company_mask = pd.Series(False, index=df.index)
                candidate_rows = df.copy()
            
            if candidate_rows.empty:
                return None
            
            keyword_pattern = '|'.join([re.escape(k) for k, _ in keyword_map])
            candidate_rows = candidate_rows[
                candidate_rows['measured_stripped'].str.contains(keyword_pattern, na=False, regex=True)
            ]
            if candidate_rows.empty:
                return None
            
            # Determine which keyword matched each row
            def find_keyword(measured_text):
                for keyword_lower, keyword_original in sorted(keyword_map, key=lambda x: len(x[0]), reverse=True):
                    if keyword_lower in measured_text:
                        return keyword_original
                return None
            
            candidate_rows['matched_keyword'] = candidate_rows['measured_stripped'].apply(find_keyword)
            candidate_rows = candidate_rows[candidate_rows['matched_keyword'].notna()]
            if candidate_rows.empty:
                return None
            
            if power_company_pattern:
                candidate_rows['company_matches_power'] = candidate_rows['company_stripped'].str.contains(
                    power_company_pattern, na=False, regex=True
                )
            else:
                candidate_rows['company_matches_power'] = False
            
            def requires_power(keyword):
                return 'riser' in keyword.strip().lower()
            
            candidate_rows = candidate_rows[
                ~(candidate_rows['matched_keyword'].apply(requires_power) & ~candidate_rows['company_matches_power'])
            ]
            if candidate_rows.empty:
                return None
            
            candidate_rows['height_numeric'] = pd.to_numeric(
                candidate_rows['height_in_inches'].astype(str).str.replace('"', '').str.replace('″', ''),
                errors='coerce'
            )
            candidate_rows = candidate_rows.dropna(subset=['height_numeric'])
            candidate_rows = candidate_rows[candidate_rows['height_numeric'] > 0]
            if candidate_rows.empty:
                return None
            
            min_row = candidate_rows.loc[candidate_rows['height_numeric'].idxmin()]
            height_formatted = Utils.inches_to_feet_format(str(int(min_row['height_numeric'])))
            height_formatted = self._format_height_for_output(height_formatted)
            
            result = {
                'height': height_formatted,
                'height_decimal': float(min_row['height_numeric']) / 12,
                'company': min_row['company'],
                'measured': min_row['measured'],
                'keyword': min_row['matched_keyword']
            }
            return result
        except Exception as e:
            logging.error(f"Error processing power attachment for SCID {scid}: {e}")
        return None
    
    def find_power_equipment(self, scid, power_equipment_keywords):
        """Find all power equipment for a SCID and return formatted list"""
        df = self.get_scid_data(scid)
        if df.empty:
            return None
        
        try:
            power_company = self.config.get("power_company", "").strip().lower()
            normalized_keywords = [kw.strip().lower() for kw in power_equipment_keywords if isinstance(kw, str) and kw.strip()]
            if not normalized_keywords:
                return None
            
            df['company_stripped'] = df['company'].astype(str).str.strip().str.lower()
            
            if power_company:
                power_company_pattern = r'\b' + re.escape(power_company) + r'\b'
                company_mask = df['company_stripped'].str.contains(power_company_pattern, na=False, regex=True)
                blank_mask = df['company_stripped'].eq('')
                power_company_rows = df[company_mask | blank_mask]
            else:
                power_company_pattern = None
                power_company_rows = df.copy()
            
            if power_company_rows.empty:
                return None
            
            power_company_rows = power_company_rows.copy()
            power_company_rows['measured_stripped'] = power_company_rows['measured'].astype(str).str.strip().str.lower()
            
            # Find rows that match power equipment keywords (case-insensitive)
            all_equipment = []  # Collect all matching equipment
            # Normalize keywords to lowercase for consistent matching
            normalized_keywords = [kw.strip().lower() for kw in power_equipment_keywords if isinstance(kw, str) and kw.strip()]
            
            for _, row in power_company_rows.iterrows():
                measured = str(row.get('measured', '')).lower().strip()
                company_stripped = str(row.get('company', '')).strip().lower()
                company_matches_power = False
                if power_company_pattern:
                    company_matches_power = bool(re.search(power_company_pattern, company_stripped))
                for normalized_keyword in normalized_keywords:
                    if normalized_keyword in measured:
                        if self._keyword_requires_power_company(normalized_keyword) and not company_matches_power:
                            continue
                        # Get height for this equipment - try different column names (flexible detection)
                        height_value = None
                        # First try standard column names
                        standard_height_cols = ['height_in_inches', 'height_in', 'height']
                        # Then find any column containing 'height' in the name
                        dynamic_height_cols = [col for col in row.index if 'height' in str(col).lower()]
                        # Combine and deduplicate while preserving order
                        all_height_cols = standard_height_cols + [col for col in dynamic_height_cols if col not in standard_height_cols]
                        
                        for height_col in all_height_cols:
                            if height_col in row and pd.notna(row[height_col]) and str(row[height_col]).strip():
                                height_value = row[height_col]
                                logging.debug(f"Found height data in column '{height_col}' for equipment {normalized_keyword} in SCID {scid}")
                                break
                        
                        if height_value is not None:
                            try:
                                height_str = str(height_value).replace('"', '').replace('″', '').strip()
                                height_inches = float(pd.to_numeric(height_str, errors='coerce'))
                                if not pd.isna(height_inches) and height_inches > 0:
                                    height_formatted = Utils.inches_to_feet_format(str(int(height_inches)))
                                    if height_formatted:
                                        # Apply output formatting based on configuration
                                        height_formatted = self._format_height_for_output(height_formatted)
                                        
                                        # Find the original keyword (preserving case) that matched
                                        original_keyword = next((kw for kw in power_equipment_keywords if kw.strip().lower() == normalized_keyword), normalized_keyword)
                                        
                                        # Map specific keywords to their display names
                                        display_name = self._get_equipment_display_name(original_keyword)
                                        
                                        all_equipment.append({
                                            'equipment': display_name,
                                            'height': height_formatted,
                                            'height_decimal': height_inches / 12,
                                            'measured': row.get('measured', '')
                                        })
                                        logging.debug(f"Added power equipment {display_name} at height {height_formatted} for SCID {scid}")
                                    else:
                                        logging.warning(f"Failed to format height for equipment {display_name} in SCID {scid}: height_inches={height_inches}")
                                else:
                                    logging.debug(f"Invalid height value for equipment {display_name} in SCID {scid}: {height_value}")
                            except Exception as e:
                                logging.warning(f"Error processing height for equipment {display_name} in SCID {scid}: {e}")
                        else:
                            logging.debug(f"No height data found for equipment {display_name} in SCID {scid}")
                        # Remove break to allow multiple keywords to match in the same row
            
            if all_equipment:
                # Sort by height (lowest first) but capture ALL equipment items
                all_equipment.sort(key=lambda x: x['height_decimal'])
                
                # Format all equipment items with line breaks
                equipment_lines = []
                for equipment in all_equipment:
                    equipment_lines.append(f"{equipment['equipment']}={equipment['height']}")
                
                return {
                    'equipment_list': '\n'.join(equipment_lines),
                    'equipment_count': len(all_equipment)
                }
                
        except Exception as e:
            logging.error(f"Error processing power equipment for SCID {scid}: {e}")
        return None
    
    def _get_equipment_display_name(self, keyword):
        """Map specific keywords to their display names for output"""
        keyword_lower = keyword.strip().lower()
        
        # Mapping of keywords to their display names
        display_mappings = {
            'transformer bottom_of_equipment': 'Transformer',
            'transformer': 'Transformer',
            'riser': 'Riser',
            'capacitor': 'Capacitor'
        }
        
        # Return mapped display name or original keyword if no mapping exists
        return display_mappings.get(keyword_lower, keyword)

    @staticmethod
    def _keyword_requires_power_company(keyword):
        """Determine if a keyword should only count when the power company matches."""
        try:
            return 'riser' in keyword.strip().lower()
        except AttributeError:
            return False
    
    def find_telecom_attachments(self, scid, telecom_keywords):
        """Find telecom attachments for a SCID and combine multiple heights in the same cell."""
        df = self.get_scid_data(scid)
        if df.empty:
            logging.warning(f"No data found for SCID {scid}")
            return {}
        
        attachments = {}
        try:
            logging.debug(f"Available columns for SCID {scid}: {list(df.columns)}")
            
            if 'height_in_inches' not in df.columns:
                logging.error(f"'height_in_inches' column missing for SCID {scid}")
                return {}
            
            for provider, keywords in telecom_keywords.items():
                clean_keywords = [kw.strip() for kw in keywords if kw.strip()]
                main_name = provider.strip()
                if main_name and main_name not in clean_keywords:
                    clean_keywords.append(main_name)
                
                company_regex = r'\b(?:' + '|'.join(re.escape(k.lower()) for k in clean_keywords) + r')\b'
                
                # Updated keywords for communication attachment selection using configurable keywords
                # Include: 'CATV Com', 'Telco Com', 'Fiber Optic Com', 'insulator', 'Power Guy'
                comm_keywords = self.config.get("comm_keywords", ['catv com', 'telco com', 'fiber optic com', 'insulator', 'power guy'])
                
                # Support wildcard (*) for substring matching or exact match
                def matches_comm_keyword(measured_text):
                    """Check if measured text matches any communication keyword (case-insensitive)
                    - If keyword ends with *, performs substring matching
                    - Otherwise performs exact match
                    - Special case: 'Guy' always uses exact match
                    """
                    measured_clean = str(measured_text).strip().lower()
                    
                    # Debug: Show detailed matching for each keyword
                    if measured_clean:  # Only debug if there's actual measured data
                        logging.debug(f"AttachmentDataReader - Checking measured text: '{measured_text}' -> cleaned: '{measured_clean}'")
                        for kw in comm_keywords:
                            kw_clean = kw.strip().lower()
                            # Check for wildcard or exact match
                            if kw_clean == 'guy':
                                # Special case: Guy always exact match
                                match_result = kw_clean == measured_clean
                            elif kw_clean.endswith('*'):
                                # Wildcard match: substring
                                match_result = kw_clean[:-1] in measured_clean
                            else:
                                # Exact match
                                match_result = kw_clean == measured_clean
                            logging.debug(f"  Keyword '{kw}' -> '{kw_clean}' == '{measured_clean}' ? {match_result}")
                    
                    # Check if any keyword matches
                    matched = False
                    for kw in comm_keywords:
                        kw_clean = kw.strip().lower()
                        if kw_clean == 'guy':
                            # Special case: Guy always exact match
                            if kw_clean == measured_clean:
                                matched = True
                                break
                        elif kw_clean.endswith('*'):
                            # Wildcard match: substring
                            if kw_clean[:-1] in measured_clean:
                                matched = True
                                break
                        else:
                            # Exact match
                            if kw_clean == measured_clean:
                                matched = True
                                break
                    
                    if matched:
                        logging.debug(f"Communication keyword match: '{measured_text}' -> '{measured_clean}' matches keywords: {comm_keywords}")
                    return matched
                
                # For "Power Guy" keyword, company name must be in company column, not measured column
                if 'power guy' in [kw.lower() for kw in comm_keywords]:
                    # Check if measured column contains "power guy" exactly
                    power_guy_rows = df[df['measured'].astype(str).apply(matches_comm_keyword)]
                    if not power_guy_rows.empty:
                        # For power guy entries, company name should be in company column
                        provider_rows = power_guy_rows[
                            power_guy_rows['company'].astype(str).str.lower().str.contains(company_regex, na=False, regex=True)
                        ]
                    else:
                        # Regular telecom attachment matching with exact measured matching
                        provider_rows = df[
                            (df['company'].astype(str).str.lower().str.contains(company_regex, na=False, regex=True)) &
                            (df['measured'].astype(str).apply(matches_comm_keyword))
                        ]
                else:
                    # Regular telecom attachment matching with exact measured matching
                    provider_rows = df[
                        (df['company'].astype(str).str.lower().str.contains(company_regex, na=False, regex=True)) &
                        (df['measured'].astype(str).apply(matches_comm_keyword))
                    ]
                
                logging.debug(f"Processing provider '{provider}' for SCID {scid}. Found {len(provider_rows)} matching rows")
                if not provider_rows.empty:
                    logging.debug(f"Provider rows data for '{provider}': {provider_rows[['company', 'measured', 'height_in_inches']].to_dict('records')}")
                    # Clean and convert height data with better error handling
                    def clean_height_value(height_val):
                        """Clean height value and convert to numeric"""
                        if pd.isna(height_val):
                            return None
                        height_str = str(height_val).replace('"', '').replace('″', '').strip()
                        if not height_str:
                            return None
                        try:
                            return pd.to_numeric(height_str)
                        except (ValueError, TypeError):
                            logging.warning(f"Could not convert height value '{height_val}' to numeric for SCID {scid}")
                            return None
                    
                    provider_rows['height_numeric'] = provider_rows['height_in_inches'].apply(clean_height_value)
                    
                    # Filter out rows with invalid height data
                    valid_rows = provider_rows.dropna(subset=['height_numeric'])
                    invalid_count = len(provider_rows) - len(valid_rows)
                    if invalid_count > 0:
                        logging.warning(f"Dropped {invalid_count} rows with invalid height data for provider {provider}, SCID {scid}")
                    
                    if not valid_rows.empty:
                        valid_rows = valid_rows.sort_values(by='height_numeric', ascending=False)
                        
                        heights = []
                        decimal_values = []
                        
                        for _, row in valid_rows.iterrows():
                            height_inches = row['height_numeric']
                            height_formatted = Utils.inches_to_feet_format(str(int(height_inches)))
                            if height_formatted:  # Only add if conversion was successful
                                # Apply output formatting based on configuration
                                height_formatted = self._format_height_for_output(height_formatted)
                                heights.append(height_formatted)
                                decimal_values.append(float(height_inches) / 12)
                        
                        if heights:  # Only create attachment if we have valid heights
                            # Deduplicate identical heights
                            unique_heights = []
                            seen_heights = set()
                            for height in heights:
                                if height not in seen_heights:
                                    unique_heights.append(height)
                                    seen_heights.add(height)
                            combined_heights = ', '.join(unique_heights)
                            min_decimal = min(decimal_values) if decimal_values else None
                            
                            attachments[provider] = {
                                'heights': combined_heights,
                                'height_decimal': min_decimal,
                                'company': valid_rows.iloc[0]['company'],
                                'measured': valid_rows.iloc[0]['measured']
                            }
                            logging.debug(f"Created attachment for {provider}, SCID {scid}: {combined_heights}")
                        else:
                            logging.warning(f"No valid height data found for provider {provider}, SCID {scid}")
                    else:
                        logging.warning(f"No valid rows found for provider {provider}, SCID {scid} after height validation")
                else:
                    logging.debug(f"No matching rows found for provider {provider}, SCID {scid}")
                    # Debug: Show what data is available
                    if not df.empty:
                        logging.debug(f"Available data in sheet for SCID {scid}: {df[['company', 'measured', 'height_in_inches']].to_dict('records')}")
                    else:
                        logging.debug(f"No data found in sheet for SCID {scid}")
            return attachments
        except Exception as e:
            logging.error(f"Error processing telecom attachments for SCID {scid}: {e}")
            return {}
    
    def find_streetlight_attachment(self, scid):
        """Find the lowest street light attachment for a SCID (measured contains 'street light')"""
        df = self.get_scid_data(scid)
        if df.empty:
            return None
        try:
            df['measured_stripped'] = df['measured'].astype(str).str.strip().str.lower()
            keywords = self._get_street_light_keywords()
            keyword_pattern = self._build_keyword_regex(keywords)
            if keyword_pattern:
                streetlight_rows = df[df['measured_stripped'].str.contains(keyword_pattern, na=False, regex=True)]
            else:
                streetlight_rows = df[df['measured_stripped'].str.contains('street light', na=False)]
            
            if streetlight_rows.empty:
                return None
            
            streetlight_rows['height_numeric'] = pd.to_numeric(
                streetlight_rows['height_in_inches'].astype(str).str.replace('"', '').str.replace('″', ''),
                errors='coerce'
            )
            streetlight_rows = streetlight_rows.dropna(subset=['height_numeric'])
            
            if not streetlight_rows.empty:
                min_row = streetlight_rows.loc[streetlight_rows['height_numeric'].idxmin()]
                height_formatted = Utils.inches_to_feet_format(str(int(min_row['height_numeric'])))
                # Apply output formatting based on configuration
                height_formatted = self._format_height_for_output(height_formatted)
                return {
                    'height': height_formatted,
                    'height_decimal': float(min_row['height_numeric']) / 12,
                    'measured': min_row['measured']
                }
        except Exception as e:
            logging.error(f"Error processing streetlight attachment for SCID {scid}: {e}")
        return None
    
    def _get_street_light_keywords(self):
        """Return configured keywords for street light detection."""
        configured = self.config.get("street_light_keywords", [])
        keywords = [kw.strip().lower() for kw in configured if isinstance(kw, str) and kw.strip()]
        if not keywords:
            keywords = ["street light"]
        return keywords
    
    @staticmethod
    def _build_keyword_regex(keywords):
        """Build regex pattern with '*' wildcard support for keyword list."""
        patterns = []
        for kw in keywords:
            escaped = re.escape(kw).replace(r'\*', '.*')
            patterns.append(escaped)
        if not patterns:
            return None
        return r'(?:' + '|'.join(patterns) + r')'
    
    def _format_height_for_output(self, height_str):
        """Format height string based on output_decimal configuration"""
        try:
            if not height_str or str(height_str).strip() == '':
                return ''
            
            # Check if output_decimal is enabled
            output_decimal = self.config.get("processing_options", {}).get("output_decimal", False)
            
            if output_decimal:
                # Convert to decimal format
                from .utils import Utils
                return Utils.feet_inches_to_decimal_format(height_str)
            else:
                # Return original format
                return str(height_str)
                
        except Exception as e:
            logging.warning(f"Error formatting height '{height_str}': {e}")
            return str(height_str)
    
    def count_existing_risers_from_attachments(self, scid):
        """Count existing risers from attachment data, excluding MetroNet"""
        df = self.get_scid_data(scid)
        if df.empty:
            return 0
        
        try:
            # Filter for riser entries
            riser_rows = df[df['measured'].astype(str).str.lower().str.contains('riser', na=False)]
            
            if riser_rows.empty:
                return 0
            
            # Apply MetroNet filtering
            count = 0
            for _, row in riser_rows.iterrows():
                company = str(row.get('company', '')).lower()
                measured = str(row.get('measured', '')).lower()
                
                # Check if this is a MetroNet riser
                is_metronet = self._is_metronet_riser(company, measured)
                
                if not is_metronet:
                    count += 1
                    logging.debug(f"Counted riser: {company} - {measured}")
                else:
                    logging.debug(f"Excluded MetroNet riser: {company} - {measured}")
            
            return count
            
        except Exception as e:
            logging.error(f"Error counting risers for SCID {scid}: {e}")
            return 0
    
    def _is_metronet_riser(self, company, measured):
        """Check if a riser entry is from MetroNet (should be excluded)"""
        # Get MetroNet keywords from config
        metronet_keywords = self.config.get("telecom_keywords", {}).get("Proposed MetroNet", [])
        
        # Check if company matches MetroNet keywords
        for keyword in metronet_keywords:
            if keyword.lower() in company:
                return True
        
        # Also check measured field for MetroNet keywords
        for keyword in metronet_keywords:
            if keyword.lower() in measured:
                return True
        
        return False