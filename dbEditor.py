print("Importing Modules...")
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox, simpledialog, LEFT
    from tkcalendar import Calendar, DateEntry
    from datetime import datetime, timedelta
    import customtkinter as ctk
    import concurrent.futures
    import sqlite3
    import time
    import logging
    import math
    import re
    import os
    import sys
    import queue

    print("Finished imports.")
    def get_resource_path(relative_path):
        """Get absolute path to resource, works for dev and PyInstaller-built EXE."""
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS  # PyInstaller temp folder
        else:
            base_path = os.path.abspath(".")

        return os.path.join(base_path, relative_path)
    TestMode = False
    if TestMode:
        DATABASE_PATH = r"C:\Users\coneil\Local Files\Returns Calculator\assets\Acc_Tran_Test.db"
    else:
        DATABASE_PATH = r"C:\Users\coneil\Local Files\Returns Calculator\assets\Acc_Tran.db"

    mainExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
    gui_queue = queue.Queue()

    
    print(DATABASE_PATH)
    dbConnection = sqlite3.connect(DATABASE_PATH)
    dbCursor = dbConnection.cursor()

except Exception as e:
    print(f"Error: {e}")
    time.sleep(5)

class AdminApp:
    def __init__(self, root):
        self.root = root
        self.root.geometry("1250x600")
        self.root.title("Admin App")
        
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        self.adminAppFrame = ctk.CTkFrame(root)
        self.adminAppFrame.grid(row = 0, column = 0, sticky='nsew')

        self.adminAppFrame.rowconfigure(8, weight=1)
        self.adminAppFrame.columnconfigure(0, weight=1)

        # GUI Elements
        self.table_label = ctk.CTkLabel(self.adminAppFrame, text="Select Table:")
        self.table_label.grid(pady=5)

        # Dropdown menu to select table
        self.tables_combobox = ttk.Combobox(self.adminAppFrame)
        self.tables_combobox.grid(pady=5)

        # Button to fetch table content
        self.fetch_button = ctk.CTkButton(self.adminAppFrame, text="Fetch Table Data", command=self.fetch_table_data)
        self.fetch_button.grid(pady=5)

        self.adminButtonFrame = ctk.CTkFrame(self.adminAppFrame)
        self.adminButtonFrame.grid()

        self.refresh_button = ctk.CTkButton(self.adminButtonFrame, text="Refresh App", command=self.refresh)
        self.refresh_button.grid(row=0, column=2, padx=5, pady = 5)

        self.delete_table_button = ctk.CTkButton(self.adminButtonFrame, text="Delete Table", command=self.delete_table)
        self.delete_table_button.grid(row=0, column=3, padx=5, pady=5)
        self.clear_table_button = ctk.CTkButton(self.adminButtonFrame, text="Clear Table", command=self.clear_table)
        self.clear_table_button.grid(row=0, column=4, padx=5, pady=5)
        self.refresh_tables_button = ctk.CTkButton(self.adminButtonFrame, text="Refresh Table List", command=self.load_tables)
        self.refresh_tables_button.grid(row=0, column=5, padx=5, pady=5)
        self.change_db_button = ctk.CTkButton(self.adminButtonFrame, text="Change DB File", command=self.select_new_database)
        self.change_db_button.grid(row=0, column=6, padx=5, pady=5)
        self.addColBtn = ctk.CTkButton(self.adminButtonFrame, text="Add Column", command=self.addColumn)
        self.addColBtn.grid(row=0, column=7, padx=5, pady=5)

        self.filter_string = ctk.StringVar(value="")
        self.filterLabel = ctk.StringVar(value="")
        self.filterDisp = ctk.CTkLabel(self.adminButtonFrame, textvariable=self.filterLabel, corner_radius=5, fg_color="gray", width=100)
        self.filterDisp.grid(row=1, column=0, padx=5,pady=5)
        self.filterDisp.grid_remove()
        
        self.filter_button = ctk.CTkButton(self.adminButtonFrame, text="Add filter", command=self.add_filter)
        self.activeFilters = []
        self.filter_button.grid(row=1, column=1, padx=5, pady=5)
        
        self.remove_filter_button = ctk.CTkButton(self.adminButtonFrame, text="Clear Filters", command=self.clear_filter)
        self.remove_filter_button.grid(row=1, column=2, padx=5,pady=5)

        self.style = ttk.Style()
        self.style.configure("Treeview", rowheight=30)
        # Treeview to display data
        self.tree = ttk.Treeview(self.adminAppFrame)
        self.tree.grid(row=8, column=0, sticky="nsew")
        self.tree.bind('<ButtonRelease-1>', self.on_entry_select)

        #Vertical scrollbar
        vsb = ttk.Scrollbar(self.adminAppFrame, orient="vertical", command=self.tree.yview)
        vsb.grid(row=8, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=vsb.set)

        # Container for Entry widgets
        self.entries_frame = ctk.CTkFrame(self.adminAppFrame)
        self.entries_frame.grid(pady=5)

        # Buttons for editing and deleting entries
        self.delete_button = ctk.CTkButton(self.adminAppFrame, text="Delete Entry(ies)", command=self.delete_entry)
        self.update_button = ctk.CTkButton(self.adminAppFrame, text="Update Entry", command=self.update_entry)
        self.add_button = ctk.CTkButton(self.adminAppFrame, text="Add Entry", command=self.add_entry)
        

        # Load table names
        self.load_tables()
    def add_filter(self):
        filter = simpledialog.askstring("Filter", "Input filter for data:")
        if filter:
            self.activeFilters.append(filter)
            self.filter_string.set(",".join(self.activeFilters))
            self.filterLabel.set("Current Filter: " + ",".join(self.activeFilters))
            self.filterDisp.grid()
            self.fetch_table_data()


    def clear_filter(self):
        self.filter_string.set("")
        self.activeFilters = []
        self.filterLabel.set("")
        self.filterDisp.grid_remove()
        self.fetch_table_data()

    def remove_source(self):
        remove_sourceID = simpledialog.askstring("Remove Source", "Enter exact ID of source to remove:")
        if remove_sourceID:  # Check if user provided input
            query = "DELETE FROM inventory WHERE sourceid = ?"
            try:
                try:
                    dbCursor.execute(query, (remove_sourceID,))
                    dbConnection.commit()  # Commit the changes to the database
                except:
                    pass
                logging.info(f"Source {remove_sourceID} removed from the source inventory.")
            except Exception as e:
                logging.error(f"An error occurred while removing source from system: {e}")
            query = "DELETE FROM sourcemaster WHERE sourceid = ?"
            try:
                try:
                    dbCursor.execute(query, (remove_sourceID,))
                    dbConnection.commit()  # Commit the changes to the database
                except:
                    pass
                logging.info(f"Source {remove_sourceID} removed from the source inventory.")
            except Exception as e:
                logging.error(f"An error occurred while removing source from system: {e}")
    def load_tables(self):
        try:
            dbCursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'  AND name NOT LIKE 'sqlite_%';")
            tables = dbCursor.fetchall()
        except:
            tables = [("Database Down",)]
        self.tables_combobox['values'] = [table[0] for table in tables]

    def fetch_table_data(self):
        if hasattr(self,'selected_rows'):
            del self.selected_rows
        if hasattr(self,'selected_row'):
            del self.selected_row
        selected_table = self.tables_combobox.get()
        if not selected_table:
            messagebox.showwarning("Warning", "Please select a table.")
            return

        query = f"PRAGMA table_info('{selected_table}')"
        try:
            dbCursor.execute(query)
            columns = [col[1] for col in dbCursor.fetchall()]
        except Exception as e:
            logging.warning(f"Failed attempt for admin to load table: {e}")
            return
        
        self.add_button.grid(pady=5)
        # Update the columns in Treeview
        self.tree["columns"] = columns
        self.tree.column("#0", width=0, stretch=tk.NO)  # Hide the tree column
        self.tree.heading("#0", text="")  # Remove the heading
        self.columns = [f"[{col}]" for col in columns]
        for col in columns:
            self.tree.heading(col, text=col)

        dbCursor.execute(f"SELECT * FROM {selected_table}")
        rows = dbCursor.fetchall()

        # Clear existing rows in treeview
        for row in self.tree.get_children():
            self.tree.delete(row)

        self.tree.tag_configure("evenrow", background="#f0f0f0")  # Light gray
        self.tree.tag_configure("oddrow", background="white")      # White
        rowIdx = 0
        for row in reversed(rows):
            sanitized_row = [str(item) for item in row]
            tag = "evenrow" if rowIdx % 2 == 0 else "oddrow"
            if self.filter_string.get() == "":
                rowIdx += 1
                self.tree.insert("", "end", values=sanitized_row, tags=(tag,))
            else:
                allFound = True
                for filter in self.activeFilters:
                    found = False
                    for item in row:
                        if item is not None and filter.lower() in item.lower():
                            found = True
                            break #exit row checking
                    if not found:
                        allFound = False
                        break
                if allFound:
                    rowIdx += 1
                    self.tree.insert("", "end", values=sanitized_row, tags=(tag,))


        # Clear existing Entry widgets
        for widget in self.entries_frame.winfo_children():
            widget.destroy()

        # Create Entry widgets dynamically based on number of columns
        self.entries = []
        model_choices = ['CKS', 'CS-137', 'CS-27', 'CS-45', 'CS-7 Sparrow', 'CS-IQ', 'CS-Raptor', 'CS-SP-PVC', 'CS-Sparrow', 
                 'CS-Special 7cm', 'CS-Static-Heart', 'CS-mMR', 'LS-140', 'LS-15', 'LS-30', 'LS-50', 'LS-55', 'LS-HR+', 'LS-LA', 
                 'LS-MPS', 'LS-mMR', 'MMS02-022', 'MMS02-022-10U', 'MMS09', 'MMS09-022', 'MMS09-022-10U', 
                 'MPS-Beta Flood', 'Marker', 'NA22LMW10', 'NEMA IQ Spec Phantom', 'Orientation Phantom', 
                 'PS-Special', 'Point', 'R&D-Special']
        def validate_input(new_value):
            if "'" in new_value or '"' in new_value:
                return False
            return True

        vcmd = (root.register(validate_input), '%P')
        for i, column in enumerate(columns):
            label = tk.Label(self.entries_frame, text=column)
            label.grid(row=0, column=i, padx=5, pady=5)
            if column == "srchome":
                entry = ttk.Combobox(self.entries_frame,values = ["ECAT Vault Q1","ECAT Vault Q2","ECAT Vault Q3","ECAT Vault Q4"], validate="key", validatecommand=vcmd)
            elif column == "radionuclide":
                entry = entry = ttk.Combobox(self.entries_frame,values = ["Ge-68","Na-22","Cs-137"], validate="key", validatecommand=vcmd)
            elif column == "model":
                entry = entry = ttk.Combobox(self.entries_frame,values = model_choices, validate="key", validatecommand=vcmd)
            elif column == "assaydate":
                entry = DateEntry(self.entries_frame, date_pattern='MM/dd/yyyy')
            else:
                entry = tk.Entry(self.entries_frame, validate="key", validatecommand=vcmd)
            entry.grid(row=1, column=i, padx=5, pady=5)
            self.entries.append(entry)

    def on_entry_select(self, event):
        selected_item = self.tree.selection()
        if selected_item:
            if len(selected_item) == 1:
                self.delete_button.grid(pady=5)
                self.update_button.grid(pady=5)
                self.add_button.grid(pady=5)
                self.selected_rows = selected_item
                self.selected_row = self.tree.item(selected_item[0], "values")
                for i, value in enumerate(self.selected_row):
                    self.entries[i].delete(0, tk.END)
                    self.entries[i].insert(0, value)
            else:
                self.update_button.grid_remove()
                self.add_button.grid_remove()
                self.selected_rows = selected_item
                self.selected_row = self.tree.item(selected_item[0], "values")
                for i, value in enumerate(self.selected_row):
                    self.entries[i].delete(0, tk.END)
                    self.entries[i].insert(0, "Multi Select")
        else:
            self.update_button.grid_remove()
            self.delete_button.grid_remove()
            if hasattr(self,'selected_rows'):
                del self.selected_rows
            if hasattr(self,'selected_row'):
                del self.selected_row
    def addColumn(self):
        try:
            selected_table = self.tables_combobox.get()
            if not selected_table:
                messagebox.showwarning("Warning", "Please select a table.")
                return
            colName = simpledialog.askstring("Column", "Input new column name:")
            colType = simpledialog.askstring("Column", "Input column type:")
            update_query = f"ALTER TABLE {selected_table} ADD COLUMN {colName} {colType};"
            self.adminCommandError = False
            self.send_admin_command(update_query)
            self.root.after(2000,self.fetch_table_data)
            time.sleep(0.5)
            if not self.adminCommandError:
                messagebox.showinfo("Success", "Column added successfully.")
        except Exception as e:
            logging.error(f"Error occured while adding column to database: {e}")
            messagebox.showinfo("Error", "Issue occured while adding column. Try again.")
    def add_entry(self):
        try:
            selected_table = self.tables_combobox.get()
            if not selected_table:
                messagebox.showwarning("Warning", "Please select a table.")
                return
            entry_values = [entry.get() for entry in self.entries]
            colString = "("
            valString = "("
            items = []
            for idx,entry in enumerate(self.columns):
                if idx == 0:
                    colString += f"{self.columns[idx]}"
                    valString += "?"
                else:
                    colString += f", {self.columns[idx]}"
                    valString += ",?"
            colString += ")"
            valString += ")"
            
            for idx,entry in enumerate(entry_values):
                if entry == "":
                    entry = " "
                items.append(entry)
            update_query = f"INSERT INTO {selected_table} " + colString + " VALUES " + valString
            self.adminCommandError = False
            self.send_admin_command(update_query,tuple(items))
            self.root.after(2000,self.fetch_table_data)
            time.sleep(0.5)
            if not self.adminCommandError:
                messagebox.showinfo("Success", "Entry added successfully.")
        except Exception as e:
            logging.error(f"Error occured while adding entry to database: {e}")
            messagebox.showinfo("Error", "Issue occured while adding entry. Try again.")

    def update_entry(self):
        try:
            selected_table = self.tables_combobox.get()
            if not selected_table:
                messagebox.showwarning("Warning", "Please select a table.")
                return
            if not hasattr(self, 'selected_row'):
                messagebox.showwarning("Warning", "Please select an entry.")
                return

            entry_values = [entry.get() for entry in self.entries]


            whereString = " WHERE "
            check = False
            for idx,entry in enumerate(self.selected_row):
                if entry != 'None':
                    entry = entry.replace("'", "''")
                    if idx == 0 or check:
                        whereString += f"{self.columns[idx]} = '{entry}'"
                        check = False
                    else:
                        whereString += f" AND {self.columns[idx]} = '{entry}'"
                elif idx == 0:
                    check = True
            setString = " SET "
            for idx,entry in enumerate(entry_values):
                if entry == "":
                    entry = " "
                entry = entry.replace("'", "''")
                if idx == 0:
                    setString += f"{self.columns[idx]} = '{entry}'"
                else:
                    setString += f", {self.columns[idx]} = '{entry}'"
            
            update_query = f"UPDATE {selected_table}" + setString + whereString 
            self.adminCommandError = False
            self.send_admin_command(update_query)

            # Refresh the data display
            self.root.after(2000,self.fetch_table_data)
            if hasattr(self,'selected_rows'):
                del self.selected_rows
            if hasattr(self,'selected_row'):
                del self.selected_row
            time.sleep(0.5)
            if not self.adminCommandError:
                messagebox.showinfo("Success", "Entry updated successfully.")
        except Exception as e:
            logging.error(f"Error occured while updating entry in database: {e}")
            messagebox.showinfo("Error", "Issue occured while updating entry. Try again.")
    def send_admin_command(self,command, items = None): #convenience of writing thread calls
        self.adminCommandError = False
        mainExecutor.submit(self.admin_thread,command, items)
    def admin_thread(self,command, items):
        admin_bg_connection = sqlite3.connect(DATABASE_PATH)
        admin_bg_cursor = admin_bg_connection.cursor()
        try:
            if items is None:
                admin_bg_cursor.execute(command)
            else:
                admin_bg_cursor.execute(command,items)
            admin_bg_connection.commit()
            admin_bg_cursor.close()
            admin_bg_connection.close()
            logging.info(f"Database alteration sucessfully occured. Command {command}")
        except Exception as e:
            def errorMsg():
                messagebox.showerror("Error","Error occured while altering database. Check Systems logs for more information.")
            if items is None:
                items = ""
            logging.error(f"Error while alterring database entries: {e}. \n Occured from command: {command} , {items}")
            if not self.adminCommandError: #only occurs once in case a large string of errors occurs
                self.adminCommandError = True
                gui_queue.put_nowait(lambda: errorMsg())
    def delete_entry(self):
        try:
            selected_table = self.tables_combobox.get()
            if not selected_table:
                messagebox.showwarning("Warning", "Please select a table.")
                return
            if not hasattr(self, 'selected_row'):
                messagebox.showwarning("Warning", "Please select an entry.")
                return
            response = messagebox.askyesno("Confirmation", "Confirm deletion of entries?")
            if not response:
                return
            self.adminCommandError = False
            for entry in self.selected_rows:
                current_row = self.tree.item(entry, "values")
                whereString = " WHERE "
                check = False
                for idx,entry in enumerate(current_row):
                    if entry != 'None':
                        entry = entry.replace("'", "''")
                        if idx == 0 or check:
                            whereString += f"{self.columns[idx]} = '{entry}'"
                            check = False
                        else:
                            whereString += f" AND {self.columns[idx]} = '{entry}'"
                    elif idx == 0:
                        check = True
                delete_query = f"DELETE FROM {selected_table}" + whereString
                self.send_admin_command(delete_query)
            if hasattr(self,'selected_rows'):
                del self.selected_rows
            if hasattr(self,'selected_row'):
                del self.selected_row
            # Refresh the data display
            self.root.after(2000,self.fetch_table_data)
            time.sleep(0.5)
            if not self.adminCommandError:
                messagebox.showinfo("Success", "Entry deleted successfully.")
        except Exception as e:
            logging.error(f"Error occured while deleted entries from database: {e}")
            messagebox.showinfo("Error", "Issue occured while deleting entries. Try again.")
    def exportAppLogs(self):
        file = get_resource_path("assets/logs/RScheckout.log")
        save_path = filedialog.asksaveasfilename(
            defaultextension=".log",
            filetypes=[("Log Files", "*.log"), ("Text Files", "*.txt"), ("All Files", "*.*")],
            title="Save Log File As"
        )

        # Check if the user canceled the save dialog
        if not save_path:
            return

        try:
            # Read the original .logs file
            with open(file, "r") as source_file:
                file_content = source_file.read()

            # Write the content to the chosen save location
            with open(save_path, "w") as destination_file:
                destination_file.write(file_content)

            logging.info(f"Application logs saved successfully at: {save_path}")

        except Exception as e:
            logging.info(f"Error saving application logs: {e}")
    def refresh(self):
        self.adminAppFrame.destroy()
        AdminApp(self.root)
    def delete_table(self):
        table = self.tables_combobox.get()
        if not table:
            messagebox.showwarning("Warning", "Please select a table to delete.")
            return
        if messagebox.askyesno("Confirm", f"Are you sure you want to permanently delete the table '{table}'?"):
            try:
                dbCursor.execute(f"DROP TABLE IF EXISTS {table}")
                dbConnection.commit()
                messagebox.showinfo("Deleted", f"Table '{table}' deleted.")
                self.load_tables()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete table: {e}")
    def clear_table(self):
        table = self.tables_combobox.get()
        if not table:
            messagebox.showwarning("Warning", "Please select a table to clear.")
            return
        if messagebox.askyesno("Confirm", f"Are you sure you want to clear all entries from table '{table}'?"):
            try:
                dbCursor.execute(f"DELETE FROM {table}")
                dbConnection.commit()
                messagebox.showinfo("Cleared", f"All entries from table '{table}' removed.")
                self.fetch_table_data()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to clear table: {e}")

    def select_new_database(self):
        global dbConnection, dbCursor, DATABASE_PATH
        new_path = filedialog.askopenfilename(filetypes=[("SQLite DB", "*.db"), ("All files", "*.*")])
        if not new_path:
            return
        try:
            test_conn = sqlite3.connect(new_path)
            test_conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            test_conn.close()
            dbConnection.close()
            DATABASE_PATH = new_path
            dbConnection = sqlite3.connect(DATABASE_PATH)
            dbCursor = dbConnection.cursor()
            messagebox.showinfo("Success", f"Database switched to: {DATABASE_PATH}")
            self.load_tables()
        except Exception as e:
            messagebox.showerror("Error", f"Invalid database file: {e}")



# Create the main window
if __name__ == "__main__":
    root = ctk.CTk()
    ctk.set_appearance_mode("Dark")  # Options: "Dark", "Light", "System"
    ctk.set_default_color_theme("blue")  # Other themes: "green", "dark-blue"
    style = ttk.Style()
    style.configure("Treeview", font=("Times New Roman", 15))  # Change font and size
    style.configure("Treeview.Heading", font=("Times New Roman", 18, "bold"))  # Change header font size
    app = AdminApp(root)
    root.mainloop()