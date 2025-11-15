import sys
import logging
import threading
import json
import shutil
import os
import subprocess
import zipfile
import tempfile
from pathlib import Path
from tkinter import *
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
import pandas as pd

# Add missing imports
from core.config_manager import ConfigManager
from core.utils import Utils
from core.attachment_data_reader import AttachmentDataReader
from core.pole_data_processor import PoleDataProcessor
from core.pdf_report_reader import PDFReportReader
from core.alden_qc_reader import AldenQCReader


class PoleMapperApp:
    """Main application class"""
    
    def __init__(self, root):
        try:
            self.root = root
            self.root.title("Xcel MakeReady Sheet QC App")
            self.root.geometry("1400x900")
            
            # Add protocol handler for window close button
            self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
            
            # Initialize flags FIRST to prevent recursion
            self._is_saving_config = False
            self._is_initializing = True
            self._temp_extract_dirs = []
            
            # Initialize managers and paths
            self.base_dir = Utils.get_base_directory()
            logging.debug(f"Base directory: {self.base_dir}")
            
            self.config_manager = ConfigManager(self.base_dir)
            self.cache_file = self.base_dir / "geocode_cache.csv"

            # Store recent-paths file in the same directory as main.py / executable
            self.paths_file = self.base_dir / "last_paths.json"
            self.last_paths = self.load_last_paths()
            
            # Configuration management - Xcel only
            self.config = self.config_manager.load_config()
            self.mapping_data = self.config.get("column_mappings", [])
            
            # Initialize processing control variables
            self.processing_thread = None
            self.stop_processing = False
            self.process_button = None
            
            # Create GUI
            self.create_widgets()
            self.geocoder = None
            
            # Initialization complete - allow auto-saving
            self._is_initializing = False
            
            # Set initial UI state and values
            self.update_ui_values()
            self.update_ui_state()
            
            # Setup auto-save
            self.auto_save_config()
            
            # Setup exception handling
            sys.excepthook = self.global_exception_handler
            
            logging.info("Pole Mapper application initialized successfully")
            
        except Exception as e:
            logging.error(f"Error in PoleMapperApp initialization: {str(e)}", exc_info=True)
            raise

    def load_last_paths(self):
        """Load last used file paths and configuration from JSON"""
        default_paths = {
            "input_file": "",
            "attachment_file": "",
            "output_file": "",
            "existing_reports_folder": "",
            "proposed_reports_folder": "",
            "alden_qc_file": "",
            "last_directory": str(Path.home())
        }
        
        try:
            if self.paths_file.exists():
                with open(self.paths_file, 'r') as f:
                    loaded_paths = json.load(f)

                    # Validate that each stored file/folder still exists; otherwise clear it
                    for key in [
                        "input_file",
                        "attachment_file",
                        "output_file",
                        "existing_reports_folder",
                        "proposed_reports_folder",
                        "alden_qc_file",
                    ]:
                        p = loaded_paths.get(key, "")
                        if p and not Path(p).exists():
                            logging.info(f"Saved path for '{key}' no longer exists – clearing it")
                            loaded_paths[key] = ""

                    # Validate last_directory
                    last_dir = loaded_paths.get("last_directory", str(Path.home()))
                    if not Path(last_dir).exists():
                        loaded_paths["last_directory"] = str(Path.home())


                    default_paths.update(loaded_paths)
        except Exception as e:
            logging.error(f"Error loading last paths: {e}")
        
        return default_paths

    def save_last_paths(self):
        """Save current file paths and configuration to JSON"""
        try:
            def abs_path(p):
                return self._clean_path(p)

            paths = {
                "input_file": abs_path(self.input_var.get() if hasattr(self, 'input_var') else ""),
                "attachment_file": abs_path(self.attachment_var.get() if hasattr(self, 'attachment_var') else ""),
                "output_file": abs_path(self.output_var.get() if hasattr(self, 'output_var') else ""),
                "existing_reports_folder": abs_path(self.existing_reports_var.get() if hasattr(self, 'existing_reports_var') else ""),
                "proposed_reports_folder": abs_path(self.proposed_reports_var.get() if hasattr(self, 'proposed_reports_var') else ""),
                "alden_qc_file": abs_path(self.alden_qc_var.get() if hasattr(self, 'alden_qc_var') else ""),
                "last_directory": getattr(self, 'last_directory', str(Path.home()))
            }
            
            with open(self.paths_file, 'w') as f:
                json.dump(paths, f, indent=2)
            
            logging.debug("Saved last paths and configuration to JSON")
        except Exception as e:
            logging.error(f"Error saving last paths: {e}")



    def create_widgets(self):
        """Create main GUI widgets"""
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=BOTH, expand=True, padx=10, pady=10)
        
        # Create tabs - Processing first, Info last
        self.create_process_tab(notebook)
        self.create_config_tab(notebook)
        self.create_info_tab(notebook)

    def create_info_tab(self, notebook):
        """Create info tab"""
        info_frame = ttk.Frame(notebook)
        notebook.add(info_frame, text="ℹ️ Info")
        
        text_frame = ttk.Frame(info_frame)
        text_frame.pack(fill=BOTH, expand=True, padx=20, pady=20)
        
        info_text = """
XCEL MAKEREADY SPREADSHEET BUILDER APPLICATION

QUICK START:
1. Go to the Configuration tab to customize settings if needed.
2. Go to the Processing tab.
3. Select the main input Excel file (with nodes, connections, and sections sheets).
4. Select the attachment data Excel file (with SCID sheets).
5. Select the output Excel template file.
6. (Optional) Select Existing Reports folder and/or Proposed Reports folder for PDF data extraction.
7. (Optional) Select Alden QC file for comparison and validation.
8. Click Process Files.

REQUIRED FILES:
- Output Template File: Always required
- Main Input File: Contains nodes, connections, and sections sheets
- Attachment Data File: Contains SCID-specific sheets with attachment data

OPTIONAL FILES:
- Existing Reports Folder: PDF reports for extracting Structure Type and Existing Load
- Proposed Reports Folder: PDF reports for extracting Proposed Load
- Alden QC File: Excel file for quality control comparison and validation

CONFIGURATION:
- Pre-configured for Xcel Energy specifications.
- Power Company: Set the power company name for filtering power attachments.
- Proposed Company: Set company name to exclude from communication columns.
- Power Keywords: Define what counts as power equipment (Primary, Secondary, Riser, etc.).
- Power Equipment Keywords: Keywords for identifying power equipment attachments.
- Communication Keywords: Keywords used to identify communication attachments. Any company NOT matching Power Company is treated as Communication.
- Ignore SCID Keywords: Keywords to ignore when matching SCIDs (e.g., "AT&T", "Xcel").
- Column Mappings: Map processed data elements and attributes to specific Excel output columns.
- Processing Options: Configure output format and behavior.
- Reset to Defaults: Option to restore Xcel default settings.

PROCESSING FEATURES:
DATA EXTRACTION:
- Reads pole data from nodes sheet (filters by node_type = 'pole' or 'reference').
- Processes connections between poles and references.
- Extracts span lengths from connections sheet.
- Processes midspan heights from sections sheet for both pole-to-pole and pole-to-reference connections.
- Reads attachment data from SCID-specific sheets matching filtered nodes.

ATTACHMENT PROCESSING:
- Power Attachments: Identifies lowest power height using power keywords and tracks the matching keyword type.
- Power Equipment: Lists all power equipment (transformers, risers, capacitors) with heights.
- Telecom Attachments: Processes communication attachments by provider and assigns to comm1-4 fields.
- Street Lights: Extracts street light attachment heights.
- Proposed MetroNet: Combines all MetroNet synonyms into a single field.

MIDSPAN DATA:
- Processes midspan heights from sections sheet (POA_* columns).
- Captures midspan data for both pole-to-pole and pole-to-reference connections.
- Assigns midspan heights to appropriate telecom providers and power.

PDF REPORT INTEGRATION:
- Extracts Structure Type from existing reports.
- Extracts Existing Loading from existing reports.
- Extracts Proposed Loading from proposed reports.
- Automatically matches PDF files to poles using SCID normalization.

ALDEN QC COMPARISON:
- Compares MR Notes between template and QC file.
- Compares MetroNet attachment and midspan heights.
- Compares Power attachment and midspan heights.
- Compares Power Type (keyword matching: e.g., "Secondary" matches "Power Secondary").
- Compares Communication heights (comm1, comm2, comm3) for both attachment and midspan.
- Color coding: Green = Match, Red = Mismatch, Light Blue = Pole not found in QC file.
- Highlights poles not found in QC file with light blue color in the Pole column.

OUTPUT FEATURES:
- Generates formatted output Excel file using configurable column mappings.
- Supports multiple worksheets in template.
- Preserves template structure and formatting.
- Output format: Decimal format (default) or feet/inches format (configurable).
- Automatic file naming based on job name from input data.
- Creates output files in an "output" folder adjacent to the template.
- Option to automatically open output file when processing completes.

COLUMN MAPPINGS:
- Power → Lowest Height → Lowest Power at Pole
- Power → Lowest Midspan → Lowest Power at Mid
- Power → Lowest Type → Lowest Power Type (shows which keyword matched)
- Street Light → Lowest Height → Street Light
- Communication → Attachment Ht / Midspan Ht → comm1, comm2, comm3, comm4
- Pole → Various attributes (Number, Tag, Address, Height & Class, etc.)
- New Guy → Size, Lead, Direction, Required
- Power Equipment → Equipment List
- Span → Length

DATA SOURCES:
- Attachment Data: Exclusively from SCID sheets in attachment file
- Midspan Heights: Exclusively from sections sheet in main input file
- Span Length: Exclusively from connections sheet in main input file
- Pole Tag: From pole_tag_tagtext column in nodes sheet
- Pole Height/Class: From pole_spec column in nodes sheet
- PDF Data: From PDF report files (structure type, loading data)
- No fallback mechanisms - missing data is left blank

TIPS:
- The application automatically saves your configuration and file paths.
- Column mappings can be added, removed, or modified in the Configuration tab.
- Use "Reset to Defaults" if you need to restore original Xcel settings.
- Processing log shows detailed information about data extraction and matching.
- Progress bar indicates processing status - you can stop processing if needed.
    """
        
        text_widget = ScrolledText(text_frame, wrap='word', font=("Arial", 11))
        text_widget.pack(fill=BOTH, expand=True)
        text_widget.insert(END, info_text)
        text_widget.config(state=DISABLED)

    def create_config_tab(self, notebook):
        """Create configuration tab"""
        config_frame = ttk.Frame(notebook)
        notebook.add(config_frame, text="⚙️ Configuration")
        
        # Main layout
        main_paned = ttk.PanedWindow(config_frame, orient=HORIZONTAL)
        main_paned.pack(fill=BOTH, expand=True, padx=10, pady=10)
        
        # Left panel with scrollbar
        self.create_left_panel(main_paned)
        
        # Right panel (Column Mappings)
        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=2)
        
        mappings_frame = ttk.LabelFrame(right_frame, text="Column Mappings", padding=15)
        mappings_frame.pack(fill=BOTH, expand=True)
        self.create_mappings_editor(mappings_frame)
        

    def create_left_panel(self, main_paned):
        """Create scrollable left panel"""
        # Create main left frame
        left_main_frame = ttk.Frame(main_paned)
        main_paned.add(left_main_frame, weight=1)
        
        # Create canvas and scrollbar for left panel
        canvas = Canvas(left_main_frame)
        scrollbar = ttk.Scrollbar(left_main_frame, orient="vertical", command=canvas.yview)
        self.scrollable_left_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas_window = canvas.create_window((0, 0), window=self.scrollable_left_frame, anchor="nw")
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Configure scrolling
        def on_frame_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
        
        def on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)
        
        self.scrollable_left_frame.bind("<Configure>", on_frame_configure)
        canvas.bind("<Configure>", on_canvas_configure)
        
        # Mouse wheel scrolling - bind only to this canvas and its children
        def on_mousewheel(event):
            if hasattr(event, 'delta') and event.delta:
                delta = event.delta
            elif hasattr(event, 'num') and event.num in (4, 5):
                delta = 120 if event.num == 4 else -120
            else:
                delta = 0
            if delta:
                canvas.yview_scroll(int(-1 * (delta / 120)), "units")
        
        # Bind mouse wheel only to this specific canvas and its children
        canvas.bind("<MouseWheel>", on_mousewheel)
        canvas.bind("<Button-4>", on_mousewheel)
        canvas.bind("<Button-5>", on_mousewheel)
        
        # Also bind to the scrollable frame and all its children
        self.scrollable_left_frame.bind("<MouseWheel>", on_mousewheel)
        self.scrollable_left_frame.bind("<Button-4>", on_mousewheel)
        self.scrollable_left_frame.bind("<Button-5>", on_mousewheel)
        
        # Create all sections
        self.create_providers_section()


    def create_providers_section(self):
        """Create telecom providers and power keywords sections"""
        # Power Company
        power_frame = ttk.LabelFrame(self.scrollable_left_frame, text="Power Company", padding=15)
        power_frame.pack(fill=X, pady=(0, 10), padx=5)
        
        # Power Company Name
        power_name_frame = ttk.Frame(power_frame)
        power_name_frame.pack(fill=X, pady=(0, 10))
        
        ttk.Label(power_name_frame, text="Power Company Name:").pack(side=LEFT, padx=(0, 10))
        self.power_company_var = StringVar(value=self.config["power_company"])
        power_entry = ttk.Entry(power_name_frame, textvariable=self.power_company_var)
        power_entry.pack(side=LEFT, fill=X, expand=True)
        
        # Prevent mouse wheel from changing entry values
        def prevent_mousewheel(event):
            return "break"
        
        power_entry.bind("<MouseWheel>", prevent_mousewheel)
        power_entry.bind("<Button-4>", prevent_mousewheel)
        power_entry.bind("<Button-5>", prevent_mousewheel)
        
        # Add trace with recursion protection
        def on_power_company_change(*args):
            if getattr(self, '_is_saving_config', False) or getattr(self, '_is_initializing', False):
                return
            self.config["power_company"] = self.power_company_var.get()
            self.auto_save_config()
        
        self.power_company_var.trace('w', on_power_company_change)
        
        # Proposed Company Name
        proposed_name_frame = ttk.Frame(power_frame)
        proposed_name_frame.pack(fill=X)
        
        ttk.Label(proposed_name_frame, text="Proposed Company Name:").pack(side=LEFT, padx=(0, 10))
        self.proposed_company_var = StringVar(value=self.config.get("proposed_company", ""))
        proposed_entry = ttk.Entry(proposed_name_frame, textvariable=self.proposed_company_var)
        proposed_entry.pack(side=LEFT, fill=X, expand=True)
        
        # Prevent mouse wheel from changing entry values
        proposed_entry.bind("<MouseWheel>", prevent_mousewheel)
        proposed_entry.bind("<Button-4>", prevent_mousewheel)
        proposed_entry.bind("<Button-5>", prevent_mousewheel)
        
        # Add trace with recursion protection
        def on_proposed_company_change(*args):
            if getattr(self, '_is_saving_config', False) or getattr(self, '_is_initializing', False):
                return
            self.config["proposed_company"] = self.proposed_company_var.get()
            self.auto_save_config()
        
        self.proposed_company_var.trace('w', on_proposed_company_change)
        
        # Power Keywords
        power_kw_frame = ttk.LabelFrame(self.scrollable_left_frame, text="Power Keywords", padding=15)
        power_kw_frame.pack(fill=X, pady=(0, 10), padx=5)
        self.create_list_editor(power_kw_frame, "power_keywords")
        
        # Power Equipment Keywords
        power_equipment_frame = ttk.LabelFrame(self.scrollable_left_frame, text="Power Equipment Keywords", padding=15)
        power_equipment_frame.pack(fill=X, pady=(0, 10), padx=5)
        self.create_list_editor(power_equipment_frame, "power_equipment_keywords")
        
        # Street Light Keywords
        street_kw_frame = ttk.LabelFrame(self.scrollable_left_frame, text="Street Light Keywords", padding=15)
        street_kw_frame.pack(fill=X, pady=(0, 10), padx=5)
        self.create_list_editor(street_kw_frame, "street_light_keywords")
        
        # Communication Keywords
        comm_kw_frame = ttk.LabelFrame(self.scrollable_left_frame, text="Communication Keywords", padding=15)
        comm_kw_frame.pack(fill=X, pady=(0, 10), padx=5)
        self.create_list_editor(comm_kw_frame, "comm_keywords")
        
        # Ignore SCID Keywords
        ignore_scid_frame = ttk.LabelFrame(self.scrollable_left_frame, text="Ignore SCID Keywords", padding=15)
        ignore_scid_frame.pack(fill=X, pady=(0, 10), padx=5)
        self.create_list_editor(ignore_scid_frame, "ignore_scid_keywords")

    def create_list_editor(self, parent, config_key):
        """Create list editor for telecom providers or power keywords"""
        # Initialize listboxes dict if it doesn't exist
        self.listboxes = getattr(self, 'listboxes', {})
        
        listbox = Listbox(parent, height=6)
        listbox.pack(fill=BOTH, expand=True, pady=(0, 10))
        self.listboxes[config_key] = listbox
        
        # Populate listbox
        for item in self.config[config_key]:
            listbox.insert(END, item)
        
        # Controls
        controls = ttk.Frame(parent)
        controls.pack(fill=X)
        
        entry = ttk.Entry(controls)
        entry.pack(side=LEFT, fill=X, expand=True, padx=(0, 5))
        
        # Prevent mouse wheel from changing entry values
        def prevent_mousewheel(event):
            return "break"
        
        entry.bind("<MouseWheel>", prevent_mousewheel)
        entry.bind("<Button-4>", prevent_mousewheel)
        entry.bind("<Button-5>", prevent_mousewheel)
        
        def add_item():
            item = entry.get().strip()
            if item and item not in self.config[config_key]:
                self.config[config_key].append(item)
                listbox.insert(END, item)
                entry.delete(0, END)
                # Only save if not already saving/initializing to prevent recursion
                if not getattr(self, '_is_saving_config', False) and not getattr(self, '_is_initializing', False):
                    self.auto_save_config()
        
        def remove_item():
            selection = listbox.curselection()
            if selection:
                item = listbox.get(selection[0])
                self.config[config_key].remove(item)
                listbox.delete(selection[0])
                # Only save if not already saving/initializing to prevent recursion
                if not getattr(self, '_is_saving_config', False) and not getattr(self, '_is_initializing', False):
                    self.auto_save_config()
        
        ttk.Button(controls, text="Add", command=add_item).pack(side=LEFT, padx=(0, 5))
        ttk.Button(controls, text="Remove", command=remove_item).pack(side=LEFT)
        
        entry.bind('<Return>', lambda e, func=add_item: func())

    def create_mappings_editor(self, parent):
        """Create column mappings editor"""
        # Header
        header_frame = ttk.Frame(parent)
        header_frame.pack(fill=X, pady=(0, 10))
        
        ttk.Label(header_frame, text="Element", font=("Arial", 11, "bold")).grid(row=0, column=0, sticky=W, padx=(5, 0))
        ttk.Label(header_frame, text="Attribute", font=("Arial", 11, "bold")).grid(row=0, column=1, sticky=W, padx=(25, 0))
        ttk.Label(header_frame, text="Output Column", font=("Arial", 11, "bold")).grid(row=0, column=2, sticky=W, padx=(25, 0))
        
        controls_frame = ttk.Frame(parent)
        controls_frame.pack(fill=X, pady=(0, 10))
        ttk.Button(controls_frame, text="Add Mapping", command=self.add_mapping).pack(side=LEFT)
        
        # Mappings area with scrollbar
        canvas = Canvas(parent)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        self.mappings_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas_window = canvas.create_window((0, 0), window=self.mappings_frame, anchor="nw")
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Configure scrolling
        def on_frame_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
        
        def on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)
        
        self.mappings_frame.bind("<Configure>", on_frame_configure)
        canvas.bind("<Configure>", on_canvas_configure)
        
        # Mouse wheel scrolling - bind only to this canvas and its children
        def on_mousewheel(event):
            if hasattr(event, 'delta') and event.delta:
                delta = event.delta
            elif hasattr(event, 'num') and event.num in (4, 5):
                delta = 120 if event.num == 4 else -120
            else:
                delta = 0
            if delta:
                canvas.yview_scroll(int(-1 * (delta / 120)), "units")
        
        # Bind mouse wheel only to this specific canvas and its children
        canvas.bind("<MouseWheel>", on_mousewheel)
        canvas.bind("<Button-4>", on_mousewheel)
        canvas.bind("<Button-5>", on_mousewheel)
        
        # Also bind to the mappings frame and all its children
        self.mappings_frame.bind("<MouseWheel>", on_mousewheel)
        self.mappings_frame.bind("<Button-4>", on_mousewheel)
        self.mappings_frame.bind("<Button-5>", on_mousewheel)
        
        self.populate_mappings()

    def populate_mappings(self):
        """Populate mappings"""
        for widget in self.mappings_frame.winfo_children():
            widget.destroy()
        
        for i, (element, attribute, output) in enumerate(self.mapping_data):
            self.create_mapping_row(i, element, attribute, output)

    def create_mapping_row(self, row_idx, element, attribute, output):
        """Create a mapping row"""
        row_frame = ttk.Frame(self.mappings_frame)
        row_frame.pack(fill=X, pady=2)
        
        # Element dropdown
        element_var = StringVar(value=element)
        element_combo = ttk.Combobox(row_frame, textvariable=element_var, 
                                   values=self.get_element_options(), state="readonly", width=15)
        element_combo.grid(row=0, column=0, sticky=W)
        
        # Attribute dropdown
        attribute_var = StringVar(value=attribute)
        attribute_combo = ttk.Combobox(row_frame, textvariable=attribute_var,
                                     values=self.get_attribute_options(element), state="readonly", width=15)
        attribute_combo.grid(row=0, column=1, sticky=W, padx=(20, 0))
        
        # Output entry
        output_var = StringVar(value=output)
        output_entry = ttk.Entry(row_frame, textvariable=output_var, width=40)
        output_entry.grid(row=0, column=2, sticky=W, padx=(20, 0))
        
        # Delete button
        ttk.Button(row_frame, text="Delete", command=lambda idx=row_idx: self.delete_mapping(idx)).grid(row=0, column=3, padx=(20, 0))
        
        # Prevent mouse wheel from changing dropdown values
        def prevent_mousewheel(event):
            return "break"
        
        element_combo.bind("<MouseWheel>", prevent_mousewheel)
        element_combo.bind("<Button-4>", prevent_mousewheel)
        element_combo.bind("<Button-5>", prevent_mousewheel)
        attribute_combo.bind("<MouseWheel>", prevent_mousewheel)
        attribute_combo.bind("<Button-4>", prevent_mousewheel)
        attribute_combo.bind("<Button-5>", prevent_mousewheel)
        
        # Trace callbacks
        def on_element_change(*args):
            if getattr(self, '_is_saving_config', False) or getattr(self, '_is_initializing', False):
                return
            try:
                attribute_combo['values'] = self.get_attribute_options(element_var.get())
                if attribute_combo['values']:
                    attribute_var.set(attribute_combo['values'][0])
                self.update_mapping_data()
                self.auto_save_config()
            except Exception as e:
                logging.error(f"Error in element change: {e}")
        
        def on_attribute_change(*args):
            if getattr(self, '_is_saving_config', False) or getattr(self, '_is_initializing', False):
                return
            try:
                self.update_mapping_data()
                self.auto_save_config()
            except Exception as e:
                logging.error(f"Error in attribute change: {e}")
        
        def on_output_change(*args):
            if getattr(self, '_is_saving_config', False) or getattr(self, '_is_initializing', False):
                return
            try:
                self.update_mapping_data()
                self.auto_save_config()
            except Exception as e:
                logging.error(f"Error in output change: {e}")
        
        element_var.trace_add('write', on_element_change)
        attribute_var.trace_add('write', on_attribute_change)
        output_var.trace_add('write', on_output_change)
        
        # Store references
        row_frame.element_var = element_var
        row_frame.attribute_var = attribute_var
        row_frame.output_var = output_var

    def get_element_options(self):
        """Get element options"""
        base = ["Pole", "New Guy", "Power", "Power Equipment", "Span", "System", "Street Light"]
        comm_options = ["comm1", "comm2", "comm3", "comm4"]
        return base + comm_options

    def get_attribute_options(self, element):
        """Get attribute options"""
        options = {
            "Pole": ["Number", "Address", "Height & Class", "MR Notes", "To Pole", "Latitude", "Longitude", "Tag", "Number of Existing Risers", "Existing Structure Type", "Existing Loading", "Proposed Loading"],
            "New Guy": ["Size", "Lead", "Direction", "Required"],
            "Power": ["Lowest Height", "Lowest Midspan", "Lowest Type"],
            "Power Equipment": ["Equipment List"],
            "Span": ["Length"],
            "System": ["Line Number"],
            "Street Light": ["Lowest Height"]
        }
        
        if element in ["comm1", "comm2", "comm3", "comm4"]:
            return ["Attachment Ht", "Midspan Ht"]
        
        return options.get(element, ["Custom"])

    def update_mapping_data(self):
        """Update mapping data from UI"""
        try:
            new_data = []
            for widget in self.mappings_frame.winfo_children():
                if hasattr(widget, 'element_var'):
                    try:
                        element = widget.element_var.get()
                        attribute = widget.attribute_var.get()
                        output = widget.output_var.get()
                        if element and attribute and output.strip():
                            new_data.append((element, attribute, output))
                    except Exception as e:
                        logging.error(f"Error reading mapping row: {e}")
            
            self.mapping_data = new_data
        except Exception as e:
            logging.error(f"Error updating mapping data: {e}")

    def add_mapping(self):
        """Add new mapping"""
        elements = self.get_element_options()
        if elements:
            element = elements[0]
            attributes = self.get_attribute_options(element)
            attribute = attributes[0] if attributes else "Custom"
            self.mapping_data.append((element, attribute, "New Column"))
            self.populate_mappings()
            self.auto_save_config()

    def delete_mapping(self, idx):
        """Delete mapping"""
        try:
            if 0 <= idx < len(self.mapping_data):
                del self.mapping_data[idx]
                self.populate_mappings()
                self.auto_save_config()
        except Exception as e:
            logging.error(f"Error deleting mapping: {e}")

    def create_process_tab(self, notebook):
        """Create processing tab"""
        process_frame = ttk.Frame(notebook)
        notebook.add(process_frame, text="▶️ Processing")
        
        # Create main layout with left and right panels
        main_paned = ttk.PanedWindow(process_frame, orient=HORIZONTAL)
        main_paned.pack(fill=BOTH, expand=True, padx=10, pady=10)
        
        # Left panel for controls
        left_frame = ttk.Frame(main_paned)
        main_paned.add(left_frame, weight=1)
        
        # Right panel for log
        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=1)
        
        # File selection
        self.create_file_selection(left_frame)
        
        
        # Processing options
        self.create_processing_options(left_frame)
        
        # Process button
        self.process_button = ttk.Button(left_frame, text="Process Files", command=self.process_files,
                  style="Accent.TButton")
        self.process_button.pack(pady=20)
        
        # Progress
        self.create_progress_section(left_frame)
        
        # Processing log in right panel
        self.create_log_section(right_frame)
        
        # Setup logging
        self.setup_logging()

    def create_file_selection(self, parent):
        """Create file selection section"""
        file_frame = ttk.LabelFrame(parent, text="File Selection", padding=15)
        file_frame.pack(fill=X, pady=(0, 10))
        
        # Main input file
        node_label = ttk.Label(file_frame, text="Node Section Connection File:")
        node_label.grid(row=0, column=0, sticky=W)
        node_label.bind("<Button-1>", lambda e: self.select_files_from_zip())
        node_label.bind("<Enter>", lambda e: node_label.config(cursor="hand2"))
        node_label.bind("<Leave>", lambda e: node_label.config(cursor=""))
        self.input_var = StringVar(value=self.last_paths["input_file"])
        ttk.Entry(file_frame, textvariable=self.input_var, width=50).grid(row=0, column=1, sticky=EW, padx=(10, 10))
        ttk.Button(file_frame, text="Browse", command=self.browse_input).grid(row=0, column=2)
        
        # Attachment file
        ttk.Label(file_frame, text="Node and Midspan Height File:").grid(row=1, column=0, sticky=W, pady=(10, 0))
        self.attachment_var = StringVar(value=self.last_paths["attachment_file"])
        ttk.Entry(file_frame, textvariable=self.attachment_var, width=50).grid(row=1, column=1, sticky=EW, padx=(10, 10), pady=(10, 0))
        ttk.Button(file_frame, text="Browse", command=self.browse_attachment).grid(row=1, column=2, pady=(10, 0))
        
        # Output file
        output_label = ttk.Label(file_frame, text="Output Template File:")
        output_label.grid(row=2, column=0, sticky=W, pady=(10, 0))
        output_label.bind("<Button-1>", lambda e: self.open_template())
        output_label.bind("<Enter>", lambda e: output_label.config(cursor="hand2"))
        output_label.bind("<Leave>", lambda e: output_label.config(cursor=""))
        self.output_var = StringVar(value=self.last_paths["output_file"])
        ttk.Entry(file_frame, textvariable=self.output_var, width=50).grid(row=2, column=1, sticky=EW, padx=(10, 10), pady=(10, 0))
        ttk.Button(file_frame, text="Browse", command=self.browse_output).grid(row=2, column=2, pady=(10, 0))
        
        # Existing Reports Folder
        ttk.Label(file_frame, text="Existing Reports Folder:").grid(row=3, column=0, sticky=W, pady=(10, 0))
        self.existing_reports_var = StringVar(value=self.last_paths.get("existing_reports_folder", ""))
        ttk.Entry(file_frame, textvariable=self.existing_reports_var, width=50).grid(row=3, column=1, sticky=EW, padx=(10, 10), pady=(10, 0))
        ttk.Button(file_frame, text="Browse", command=self.browse_existing_reports).grid(row=3, column=2, pady=(10, 0))
        
        # Proposed Reports Folder
        ttk.Label(file_frame, text="Proposed Reports Folder:").grid(row=4, column=0, sticky=W, pady=(10, 0))
        self.proposed_reports_var = StringVar(value=self.last_paths.get("proposed_reports_folder", ""))
        ttk.Entry(file_frame, textvariable=self.proposed_reports_var, width=50).grid(row=4, column=1, sticky=EW, padx=(10, 10), pady=(10, 0))
        ttk.Button(file_frame, text="Browse", command=self.browse_proposed_reports).grid(row=4, column=2, pady=(10, 0))
        
        # Alden QC File
        ttk.Label(file_frame, text="Alden QC:").grid(row=5, column=0, sticky=W, pady=(10, 0))
        self.alden_qc_var = StringVar(value=self.last_paths.get("alden_qc_file", ""))
        ttk.Entry(file_frame, textvariable=self.alden_qc_var, width=50).grid(row=5, column=1, sticky=EW, padx=(10, 10), pady=(10, 0))
        ttk.Button(file_frame, text="Browse", command=self.browse_alden_qc).grid(row=5, column=2, pady=(10, 0))
        
        file_frame.grid_columnconfigure(1, weight=1)
        
        # Set last directory from saved paths
        self.last_directory = self.last_paths["last_directory"]


    def create_processing_options(self, parent):
        """Create processing options section"""
        options_frame = ttk.LabelFrame(parent, text="Processing Options", padding=15)
        options_frame.pack(fill=X, pady=(0, 10))
        
        # Initialize with configuration values
        processing_options = self.config.get("processing_options", {})
        self.open_output_var = BooleanVar(value=processing_options.get("open_output", False))
        ttk.Checkbutton(options_frame, text="Open output file when complete", variable=self.open_output_var).pack(anchor=W)

    def create_progress_section(self, parent):
        """Create progress section"""
        progress_frame = ttk.LabelFrame(parent, text="Progress", padding=15)
        progress_frame.pack(fill=X, pady=(0, 10))
        
        self.progress_var = StringVar(value="Ready to process files...")
        ttk.Label(progress_frame, textvariable=self.progress_var).pack(anchor=W)
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='determinate')
        self.progress_bar.pack(fill=X, pady=(10, 0))

    def create_log_section(self, parent):
        """Create log section"""
        log_frame = ttk.LabelFrame(parent, text="Processing Log", padding=15)
        log_frame.pack(fill=BOTH, expand=True)
        
        self.log_text = ScrolledText(log_frame, height=25, font=("Consolas", 9))
        self.log_text.pack(fill=BOTH, expand=True)


    def browse_input(self):
        """Browse for input Excel file"""
        filename = filedialog.askopenfilename(
            title="Select Main Input Excel File",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.input_var.get()) if self.input_var.get() else ""
        )
        if filename:
            self.input_var.set(filename)
            self.auto_save_config()

    def browse_attachment(self):
        """Browse for attachment data file"""
        filename = filedialog.askopenfilename(
            title="Select Attachment Data File",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.attachment_var.get()) if self.attachment_var.get() else ""
        )
        if filename:
            self.attachment_var.set(filename)
            self.auto_save_config()

    def browse_output(self):
        """Browse for output template file"""
        filename = filedialog.askopenfilename(
            title="Select Output Template File",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.output_var.get()) if self.output_var.get() else ""
        )
        if filename:
            self.output_var.set(filename)
            self.auto_save_config()
    
    def select_files_from_zip(self):
        """Select a ZIP file that contains the Node/Section/Connection and Midspan files."""
        downloads_dir = Path.home() / "Downloads"
        if downloads_dir.exists():
            initial_dir = str(downloads_dir)
        else:
            initial_dir = self.last_directory if hasattr(self, 'last_directory') else ""
        zip_path = filedialog.askopenfilename(
            title="Select ZIP File Containing Input Workbooks",
            filetypes=[("ZIP files", "*.zip"), ("All files", "*.*")],
            initialdir=initial_dir
        )
        if not zip_path:
            return
        
        self.last_directory = os.path.dirname(zip_path)
        
        try:
            zip_path_obj = Path(zip_path)
            extract_dir = zip_path_obj.parent / f"{zip_path_obj.stem}_extracted"
            if extract_dir.exists():
                shutil.rmtree(extract_dir, ignore_errors=True)
            extract_dir.mkdir(parents=True, exist_ok=True)
            self._temp_extract_dirs.append(str(extract_dir))
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            logging.info(f"Extracted ZIP to directory: {extract_dir}")
            
            node_file, midspan_file = self._find_zip_targets(extract_dir)
            if node_file:
                self.input_var.set(node_file)
            if midspan_file:
                self.attachment_var.set(midspan_file)
            self.auto_save_config()
        except zipfile.BadZipFile:
            logging.error("The selected file is not a valid ZIP archive.")
        except Exception as e:
            logging.error(f"Failed to extract ZIP: {e}")

    def open_template(self):
        """Open the currently selected template file"""
        template_path = self.output_var.get().strip()
        if not template_path:
            messagebox.showwarning("Template Missing", "Please select an output template file first.")
            return
        if not Path(template_path).exists():
            messagebox.showerror("Template Not Found", f"The template file was not found:\n{template_path}")
            return
        try:
            self.open_output_file(template_path)
        except Exception as e:
            logging.warning(f"Failed to open template file: {e}")

    def browse_existing_reports(self):
        """Browse for existing reports folder"""
        folder_path = filedialog.askdirectory(
            title="Select Existing Reports Folder",
            initialdir=self.existing_reports_var.get() if self.existing_reports_var.get() else ""
        )
        if folder_path:
            self.existing_reports_var.set(folder_path)
            self.auto_save_config()

    def browse_proposed_reports(self):
        """Browse for proposed reports folder"""
        folder_path = filedialog.askdirectory(
            title="Select Proposed Reports Folder",
            initialdir=self.proposed_reports_var.get() if self.proposed_reports_var.get() else ""
        )
        if folder_path:
            self.proposed_reports_var.set(folder_path)
            self.auto_save_config()

    def browse_alden_qc(self):
        """Browse for Alden QC Excel file"""
        filename = filedialog.askopenfilename(
            title="Select Alden QC Excel File",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.alden_qc_var.get()) if self.alden_qc_var.get() else ""
        )
        if filename:
            self.alden_qc_var.set(filename)
            self.auto_save_config()
    
    def _find_zip_targets(self, extract_dir):
        """Locate Node/Section/Connection and Midspan files within extracted directory."""
        try:
            excel_files = list(Path(extract_dir).rglob("*.xls*"))
            if not excel_files:
                return None, None
            
            def match_keywords(path, keywords):
                name = path.name.lower()
                return all(keyword in name for keyword in keywords)
            
            node_keywords = ["node", "section", "connection"]
            midspan_keywords = ["node", "midspan", "height"]
            
            node_file = next((str(p) for p in excel_files if match_keywords(p, node_keywords)), None)
            midspan_file = next((str(p) for p in excel_files if match_keywords(p, midspan_keywords)), None)
            
            if not node_file and excel_files:
                node_file = str(excel_files[0])
            if not midspan_file and len(excel_files) > 1:
                midspan_file = str(excel_files[1])
            
            return node_file, midspan_file
        except Exception as e:
            logging.debug(f"Error scanning extracted ZIP contents: {e}")
            return None, None
    
    def _clean_path(self, p):
        """Return normalized absolute POSIX-style path string"""
        try:
            if not p:
                return ""
            return str(Path(p).expanduser().resolve().as_posix())
        except Exception:
            return str(p).strip()

    def process_files(self):
        """Process the selected files"""
        try:
            # If already processing, stop the process
            if self.processing_thread and self.processing_thread.is_alive():
                self.stop_processing = True
                if self.process_button:
                    self.process_button.config(text="Stopping...", state="disabled")
                return

            # Get all paths from UI StringVars
            input_path = self.input_var.get()
            attachment_path = self.attachment_var.get()
            output_path = self.output_var.get()

            # Validate required paths - Template is always required
            if not output_path:
                messagebox.showerror("Missing Files", "Please provide a path for the Output Template file.")
                return
            
            # Check for valid file combinations
            has_main_sheet = bool(input_path)
            has_attachment_sheet = bool(attachment_path)
            has_existing_reports = bool(self.existing_reports_var.get())
            has_proposed_reports = bool(self.proposed_reports_var.get())
            
            # Validate file combinations - Template + PDF Reports is sufficient
            # No additional validation needed - template-only processing is supported
            
            if not has_existing_reports and not has_proposed_reports:
                logging.warning("No PDF report folders provided. PDF data (Structure Type, Existing Load, Proposed Load) will not be populated.")
            
            logging.info(f"File combination: Main={has_main_sheet}, Attachment={has_attachment_sheet}, Template={bool(output_path)}, Existing Reports={has_existing_reports}, Proposed Reports={has_proposed_reports}")
            
            # Log what data will be populated
            data_sources = []
            if has_main_sheet:
                data_sources.append("Main Sheet (nodes, connections, sections)")
            if has_attachment_sheet:
                data_sources.append("Attachment Sheet (SCID data)")
            if has_existing_reports:
                data_sources.append("Existing Reports (structure type, existing load)")
            if has_proposed_reports:
                data_sources.append("Proposed Reports (proposed load)")
            
            if data_sources:
                logging.info(f"Data will be populated from: {', '.join(data_sources)}")
            else:
                logging.info("Only template data will be used (no additional data sources)")

            # Reset stop flag and update UI
            self.stop_processing = False
            if self.process_button:
                self.process_button.config(text="STOP", state="normal")
            self.log_text.delete(1.0, END)

            def progress_callback(percentage, message):
                # Check if stop was requested
                if self.stop_processing:
                    return False  # Signal to stop processing
                
                self.progress_var.set(message)
                self.progress_bar['value'] = percentage
                self.root.update_idletasks()
                return True  # Continue processing

            # Pass paths explicitly to the worker thread
            self.processing_thread = threading.Thread(
                target=self._process_files_worker,
                args=(progress_callback, input_path, attachment_path, output_path)
            )
            self.processing_thread.daemon = True
            self.processing_thread.start()

        except Exception as e:
            logging.error(f"Error starting file processing: {e}")
            self.reset_process_button()

    def request_stop(self):
        """Stop the current processing operation"""
        if self.processing_thread and self.processing_thread.is_alive():
            self.stop_processing = True
            self.process_button.config(text="Stopping...", state="disabled")
            logging.info("Stop request sent - waiting for processing to complete...")

    def reset_process_button(self):
        """Reset the process button to its initial state"""
        if self.process_button:
            self.process_button.config(text="Process Files", state="normal")
        self.processing_thread = None
        self.stop_processing = False

    def _process_files_worker(self, progress_callback, input_file, attachment_file, output_file):
        """Process files in a background thread."""
        try:
            import pandas as pd
            
            # Check for stop request before starting
            if not progress_callback(0, "Starting processing..."):
                logging.info("Processing stopped by user request")
                self.root.after(0, self.reset_process_button)
                return

            # Update config from UI
            self.update_config_from_ui()

            # Read main input file if provided
            nodes_df = None
            connections_df = None
            sections_df = None
            
            if input_file:
                if not progress_callback(10, "Reading main input file..."):
                    logging.info("Processing stopped by user request")
                    self.root.after(0, self.reset_process_button)
                    return
                nodes_df = pd.read_excel(input_file, sheet_name='nodes', dtype=str).fillna("")
                connections_df = pd.read_excel(input_file, sheet_name='connections', dtype=str).fillna("")
                sections_df = pd.read_excel(input_file, sheet_name='sections', dtype=str).fillna("")
                logging.info(f"Read {len(nodes_df)} nodes, {len(connections_df)} connections, {len(sections_df)} sections")
            else:
                logging.info("No main input file provided - creating empty dataframes")
                nodes_df = pd.DataFrame()
                connections_df = pd.DataFrame()
                sections_df = pd.DataFrame()

            if not progress_callback(15, "Extracting valid SCIDs..."):
                logging.info("Processing stopped by user request")
                self.root.after(0, self.reset_process_button)
                return
            
            # Extract valid SCIDs from nodes data if available
            valid_scids = []
            if not nodes_df.empty and 'scid' in nodes_df.columns:
                from core.utils import Utils
                nodes_df_copy = nodes_df.copy()
                ignore_keywords = self.config.get("ignore_scid_keywords", [])
                nodes_df_copy['scid'] = nodes_df_copy['scid'].apply(lambda x: Utils.normalize_scid(x, ignore_keywords))
                valid_nodes = Utils.filter_valid_nodes(nodes_df_copy)
                valid_scids = valid_nodes['scid'].tolist()
                logging.info(f"Found {len(valid_scids)} valid SCIDs from nodes data")
            else:
                logging.info("No nodes data available - will process all attachment data")

            # Initialize geocoder (disabled)
            geocoder = None

            # Read attachment data if provided
            attachment_reader = None
            if attachment_file:
                if not progress_callback(25, "Reading attachment data..."):
                    logging.info("Processing stopped by user request")
                    self.root.after(0, self.reset_process_button)
                    return
                attachment_reader = AttachmentDataReader(attachment_file, config=self.config, valid_scids=valid_scids)
                logging.info("Attachment data reader initialized")
            else:
                logging.info("No attachment data file provided - attachment data will not be processed")

            # QC reader disabled
            qc_reader = None
            logging.info("Processing all connections without QC filtering")

            # Initialize Alden QC reader if file is provided
            alden_qc_reader = None
            alden_qc_file_path = self.alden_qc_var.get() if hasattr(self, 'alden_qc_var') else ""
            if alden_qc_file_path:
                try:
                    alden_qc_reader = AldenQCReader(alden_qc_file_path)
                    if alden_qc_reader.is_active():
                        logging.info(f"Alden QC reader initialized with {len(alden_qc_reader.get_all_poles())} poles")
                    else:
                        logging.warning("Alden QC reader failed to initialize")
                except Exception as e:
                    logging.error(f"Error initializing Alden QC reader: {e}")
                    alden_qc_reader = None
            else:
                logging.info("No Alden QC file provided - Alden comparison will not be performed")

            if not progress_callback(30, "Initializing data processor..."):
                logging.info("Processing stopped by user request")
                self.root.after(0, self.reset_process_button)
                return

            # Initialize PDF reader with folder paths from UI
            pdf_reader = None
            existing_reports_folder = self.existing_reports_var.get() if hasattr(self, 'existing_reports_var') else ""
            proposed_reports_folder = self.proposed_reports_var.get() if hasattr(self, 'proposed_reports_var') else ""
            
            logging.info(f"GUI: Raw PDF folder paths from UI - Existing: '{existing_reports_folder}', Proposed: '{proposed_reports_folder}'")
            
            # Normalize paths for Windows compatibility
            if existing_reports_folder:
                existing_reports_folder = str(Path(existing_reports_folder).resolve())
                logging.info(f"GUI: Resolved existing reports folder: {existing_reports_folder}")
            if proposed_reports_folder:
                proposed_reports_folder = str(Path(proposed_reports_folder).resolve())
                logging.info(f"GUI: Resolved proposed reports folder: {proposed_reports_folder}")
            
            logging.info(f"GUI: Final PDF folder paths - Existing: '{existing_reports_folder}', Proposed: '{proposed_reports_folder}'")
            
            if existing_reports_folder or proposed_reports_folder:
                try:
                    # Get ignore keywords from config for PDF filename normalization
                    ignore_keywords = self.config.get("ignore_scid_keywords", [])
                    pdf_reader = PDFReportReader(existing_reports_folder, proposed_reports_folder, ignore_keywords)
                    logging.info(f"Initialized PDF reader with existing folder: {existing_reports_folder}, proposed folder: {proposed_reports_folder}")
                except Exception as e:
                    logging.error(f"Error initializing PDF reader: {e}")
                    pdf_reader = None
            else:
                logging.warning("No PDF folder paths provided - PDF data will not be extracted")
            
            processor = PoleDataProcessor(
                config=self.config,
                geocoder=geocoder,
                mapping_data=self.mapping_data,
                attachment_reader=attachment_reader,
                qc_reader=qc_reader,
                pdf_reader=pdf_reader,
                alden_qc_reader=alden_qc_reader
            )

            # Read template SCIDs to filter processing
            if not progress_callback(35, "Reading template connections..."):
                logging.info("Processing stopped by user request")
                self.root.after(0, self.reset_process_button)
                return
            
            template_scids = processor.read_template_scids(output_file)
            if template_scids:
                logging.info(f"Found {len(template_scids)} connections in template - processing only these")
            else:
                logging.warning("Could not read template connections - processing all connections")

            # Process data
            if not progress_callback(40, "Processing pole data..."):
                logging.info("Processing stopped by user request")
                self.root.after(0, self.reset_process_button)
                return
            result_data = processor.process_data(
                nodes_df=nodes_df,
                connections_df=connections_df,
                sections_df=sections_df,
                progress_callback=progress_callback,
                manual_routes=None,
                clear_existing_routes=False
            )

            # Extract job name from nodes_df if available
            progress_callback(85, "Generating output file...")
            job_name = ""
            if not nodes_df.empty and "job_name" in nodes_df.columns and not nodes_df["job_name"].empty:
                job_name = str(nodes_df["job_name"].iloc[0]).strip()
            if not job_name:
                job_name = "Output"

            # Generate actual output file by copying template with job name
            actual_output_file = self.generate_output_file(job_name, output_file)
            if not actual_output_file:
                progress_callback(0, "Failed to generate output file!")
                return
            
            # Check if a unique filename was generated (indicates original file was open)
            if "_" in actual_output_file.name and any(char.isdigit() for char in actual_output_file.name.split("_")[-1]):
                logging.info(f"Original file was open in another application. Generated unique filename: {actual_output_file.name}")

            # Write output to the newly created file
            progress_callback(90, "Writing output file...")
            processor.write_output(result_data, str(actual_output_file))

            progress_callback(100, "Processing complete!")
            logging.info(f"Processing complete. Output written to: {actual_output_file}")

            # Save last paths
            self.save_last_paths()

            # Open output file if requested
            if self.open_output_var.get():
                self.root.after(1000, lambda: self.open_output_file(str(actual_output_file)))

            # Log success message
            logging.info(f"Processing completed successfully! Processed {len(result_data)} poles. Output saved to: {actual_output_file}")
            
            # Reset button on completion
            self.root.after(0, self.reset_process_button)

        except Exception as e:
            logging.error(f"Error during processing: {e}", exc_info=True)
            logging.error(f"An error occurred during processing: {e}")
            progress_callback(0, "Processing failed!")
            # Reset button on error
            self.root.after(0, self.reset_process_button)

    def generate_output_file(self, job_name, output_template):
        """Generate actual output file by copying the template using job_name."""
        import shutil
        from pathlib import Path
        import time
        
        template_path = Path(output_template)
        if not template_path.exists():
            logging.error(f"Output template file not found: {output_template}")
            return None
            
        # Create output directory in the same location as the template
        output_dir = template_path.parent / "output"
        output_dir.mkdir(exist_ok=True)
        logging.info(f"Created/verified output directory: {output_dir}")
            
        # Always use .xlsx format for output files
        base_filename = f"{job_name} Spread Sheet.xlsx"
        
        # Try to find an available filename in the output directory
        counter = 0
        actual_output_file = output_dir / base_filename
        
        while actual_output_file.exists():
            counter += 1
            if counter == 1:
                # First attempt: try with timestamp
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                actual_output_file = output_dir / f"{job_name} Spread Sheet_{timestamp}.xlsx"
            else:
                # Subsequent attempts: try with counter
                actual_output_file = output_dir / f"{job_name} Spread Sheet_{counter}.xlsx"
            
            # Prevent infinite loop
            if counter > 100:
                logging.error(f"Could not find available filename after 100 attempts")
                return None
        
        try:
            shutil.copy(template_path, actual_output_file)
            return actual_output_file
        except PermissionError as e:
            # File is likely open in Excel or another application
            logging.warning(f"Permission denied - file may be open in another application: {actual_output_file}")
            
            # Try with a unique timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            unique_filename = f"{job_name} Spread Sheet_{timestamp}.xlsx"
            actual_output_file = output_dir / unique_filename
            
            try:
                shutil.copy(template_path, actual_output_file)
                return actual_output_file
            except Exception as e2:
                logging.error(f"Failed to copy template even with unique filename: {e2}")
                return None
        except Exception as e:
            logging.error(f"Error copying template file: {e}")
            return None

    def open_output_file(self, filepath):
        """Open the output file"""
        try:
            import subprocess
            import os
            if os.name == 'nt':  # Windows
                os.startfile(filepath)
            elif os.name == 'posix':  # macOS and Linux
                subprocess.call(['open', filepath] if sys.platform == 'darwin' else ['xdg-open', filepath])
        except Exception as e:
            logging.warning(f"Could not open output file: {e}")

    def setup_logging(self):
        """Setup logging to display in GUI"""
        class GuiLogHandler(logging.Handler):
            def __init__(self, text_widget):
                super().__init__()
                self.text_widget = text_widget
            
            def emit(self, record):
                try:
                    msg = self.format(record)
                    self.text_widget.insert(END, msg + '\n')
                    self.text_widget.see(END)
                except Exception:
                    pass
        
        # Create handler
        if hasattr(self, 'log_text'):
            gui_handler = GuiLogHandler(self.log_text)
            gui_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(message)s')
            gui_handler.setFormatter(formatter)
            
            # Add to root logger
            logging.getLogger().addHandler(gui_handler)

    def auto_save_config(self):
        """Automatically save configuration with debouncing"""
        # Cancel any pending save
        if hasattr(self, '_save_timer'):
            self.root.after_cancel(self._save_timer)
        
        # Set flag to prevent recursion
        if getattr(self, '_is_saving_config', False):
            return
            
        # Schedule save after short delay (debouncing)
        self._save_timer = self.root.after(500, self._do_auto_save)

    def _do_auto_save(self):
        """Actually perform the auto save"""
        if getattr(self, '_is_saving_config', False) or getattr(self, '_is_initializing', False):
            return
            
        try:
            self._is_saving_config = True
            self.update_config_from_ui()
            self.save_config()
        except Exception as e:
            logging.error(f"Error in auto save: {e}")
        finally:
            self._is_saving_config = False
    
    def update_config_from_ui(self):
        """Update config from current UI state"""
        try:
            # Update power company
            if hasattr(self, 'power_company_var'):
                self.config["power_company"] = self.power_company_var.get()
            
            # Update proposed company
            if hasattr(self, 'proposed_company_var'):
                self.config["proposed_company"] = self.proposed_company_var.get()
            
            
            # Update processing options
            if not "processing_options" in self.config:
                self.config["processing_options"] = {}
            
            if hasattr(self, 'open_output_var'):
                self.config["processing_options"]["open_output"] = self.open_output_var.get()
            
            # Set decimal output as default
            self.config["processing_options"]["output_decimal"] = True
                    
            # Update column mappings
            self.config["column_mappings"] = self.mapping_data
            
        except Exception as e:
            logging.error(f"Error updating config from UI: {e}")

    
    def update_ui_values(self):
        """Update UI with current config values"""
        try:
            self._is_initializing = True
            
            # Update power company
            if hasattr(self, 'power_company_var'):
                self.power_company_var.set(self.config["power_company"])
            
            # Update proposed company
            if hasattr(self, 'proposed_company_var'):
                self.proposed_company_var.set(self.config.get("proposed_company", ""))
            
            
            # Update processing options
            processing_options = self.config.get("processing_options", {})
            if hasattr(self, 'open_output_var'):
                self.open_output_var.set(processing_options.get("open_output", False))
            
            # Update mapping data
            self.mapping_data = self.config.get("column_mappings", [])
            
        except Exception as e:
            logging.error(f"Error updating UI values: {e}")
        finally:
            self._is_initializing = False

    def update_ui_state(self):
        """Update UI state"""
        try:
            # Refresh listboxes
            for config_key, listbox in getattr(self, 'listboxes', {}).items():
                listbox.delete(0, END)
                for item in self.config[config_key]:
                    listbox.insert(END, item)
            
            # Refresh mappings
            if hasattr(self, 'populate_mappings'):
                self.populate_mappings()
                
        except Exception as e:
            logging.error(f"Error updating UI state: {e}")

    def refresh_ui(self):
        """Refresh UI components that depend on configuration"""
        try:
            # Update mappings if they're already populated
            if hasattr(self, 'populate_mappings'):
                self.populate_mappings()
                
        except Exception as e:
            logging.error(f"Error refreshing UI: {e}")

    def save_config(self):
        """Save current configuration"""
        try:
            self.config_manager.save_config(self.config)
        except Exception as e:
            logging.error(f"Error saving config: {e}")

    def on_closing(self):
        """Handle application closing"""
        try:
            # Save last paths
            self.save_last_paths()
            
            # Save current config
            self.update_config_from_ui()
            self.save_config()
            
            # Clean up any temporary extraction directories
            for temp_dir in getattr(self, '_temp_extract_dirs', []):
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception as cleanup_error:
                    logging.debug(f"Failed to remove temp directory {temp_dir}: {cleanup_error}")
            
            logging.info("Application closing")
            self.root.destroy()
        except Exception as e:
            logging.error(f"Error during application close: {e}")
            self.root.destroy()
    
    def global_exception_handler(self, exc_type, exc_value, exc_traceback):
        """Handle uncaught exceptions"""
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        if issubclass(exc_type, RecursionError):
            logging.error("Recursion error detected. Application will exit.")
        else:
            logging.error(f"An unexpected error occurred: {exc_value}")
            
        # Continue execution without showing message box
