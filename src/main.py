import sys
import logging
from tkinter import Tk

def handle_exception(exc_type, exc_value, exc_traceback):
    """Handle exceptions globally for both sys and tkinter"""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    if issubclass(exc_type, RecursionError):
        logging.error("Recursion error detected. Application will exit.")
    else:
        logging.error(f"An unexpected error occurred: {exc_value}", exc_info=(exc_type, exc_value, exc_traceback))

def main():
    """Main application entry point"""
    
    # Set up basic logging first
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Setup unified exception handler for both sys and tkinter
    sys.excepthook = handle_exception
    
    try:
        # Create root window
        root = Tk()
        root.withdraw()  # Hide the root window initially
        
        # Setup tkinter exception handler (same handler as sys)
        root.report_callback_exception = handle_exception
        
        # Import and start the GUI application
        from gui.main_window import PoleMapperApp
        app = PoleMapperApp(root)
        root.update_idletasks()  # Force Tkinter to process all pending events, including StringVar initialization
        root.deiconify()  # Show the window
        root.mainloop()
            
    except Exception as e:
        logging.error(f"Failed to start application: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()