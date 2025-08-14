import os
import pandas as pd
from flask import Flask, render_template, request, send_file
from flask_apscheduler import APScheduler
from io import BytesIO


app = Flask(__name__)
# === Scheduler config ===
class Config:
    SCHEDULER_API_ENABLED = True

app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# === Constants ===
ALLOWED_EXTENSIONS = {'xlsx', 'xlsm', 'xltx', 'xltm'}
CLEANUP_DIR = os.path.abspath("combined")

# === Helpers ===
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def delete_excel_temp_files():
    for foldername, _, filenames in os.walk(CLEANUP_DIR):
        for filename in filenames:
            if filename.startswith('~$') and filename.endswith(('.xlsx', '.xlsm')):
                try:
                    os.remove(os.path.join(foldername, filename))
                    print(f"🧹 Deleted temp file: {filename}")
                except Exception as e:
                    print(f"❌ Could not delete {filename}: {e}")

# === Scheduled Job ===
@scheduler.task('interval', id='cleanup_job', minutes=10, misfire_grace_time=300)
def scheduled_cleanup():
    print("⏱️ Running scheduled Excel temp file cleanup...")
    delete_excel_temp_files()

# === Excel Processing Logic ===
def process_excel_files(uploaded_files, sheet_names_list, new_sheet_names_list=None, plant=None):
    from openpyxl import load_workbook
    import pandas as pd
    from io import BytesIO

    combined_output = BytesIO()

    if plant == "anhui":
        def extract_client_name(filename, sheet_name=None):
            """Extract client name from filename or sheet name"""
            # Remove file extension and path
            base_name = filename.rsplit('.', 1)[0] if '.' in filename else filename

            # For chokes file
            if 'Quality follow-up Chokes' in base_name or 'choke' in base_name.lower():
                return 'Chokes'

            # For brushcard files like "Kelier 2025质量汇总表.xlsx"
            if '质量汇总表' in base_name:
                # Remove "2025质量汇总表" and "质量汇总表" to get client name
                client_name = base_name.replace(' 2025质量汇总表', '').strip()
                client_name = client_name.replace('2025质量汇总表', '').strip()
                client_name = client_name.replace(' 质量汇总表', '').strip()
                client_name = client_name.replace('质量汇总表', '').strip()
                return client_name

            # If sheet name is provided and contains 质量汇总表
            if sheet_name and '质量汇总表' in sheet_name:
                client_name = sheet_name.replace('质量汇总表', '').strip()
                return client_name

            return base_name

        def standardize_columns(df, client_name):
            """Standardize column names to match the required format"""

            # Define the target column mapping
            target_columns = {
                '生产日期\nProduction Date': '生产日期\nProduction Date',
                '检验日期\nInspection Date': '检验日期\nInspection Date',
                '型号\nType': '型号\nType',
                '不良部位\nDefective Part': '不良部位\nDefective Part',
                '不良名称\nDefect Name': '不良名称\nDefect Name',
                '数量\nQuantity': '数量\nQuantity',
                '处理方式\nHandling method': '处理方式\nHandling method',
                '原因\nCause of defect': '原因\nCause of defect',
                '检验站别\nInspection station': '检验站别\nInspection station',
                '当日检数量\nInspection quantity': '当日检数量\nInspection quantity',
                '备注\nRemark': '备注\nRemark'
            }

            # Create a copy of the dataframe
            df_copy = df.copy()

            # Add client name column
            df_copy['客户名称\nClient Name'] = client_name

            # Try to map existing columns to target columns
            column_mapping = {}

            for col in df_copy.columns:
                col_str = str(col).strip()

                # Direct matches
                if col_str in target_columns:
                    column_mapping[col] = target_columns[col_str]
                    continue

                # Fuzzy matching for common variations
                col_lower = col_str.lower()

                if any(x in col_lower for x in ['生产日期', 'production date', 'prod date']):
                    column_mapping[col] = '生产日期\nProduction Date'
                elif any(x in col_lower for x in ['检验日期', 'inspection date', 'inspect date']):
                    column_mapping[col] = '检验日期\nInspection Date'
                elif any(x in col_lower for x in ['型号', 'type', 'model']):
                    column_mapping[col] = '型号\nType'
                elif any(x in col_lower for x in ['不良部位', 'defective part', 'defect part']):
                    column_mapping[col] = '不良部位\nDefective Part'
                elif any(x in col_lower for x in ['不良名称', 'defect name', 'defective name']):
                    column_mapping[col] = '不良名称\nDefect Name'
                elif any(x in col_lower for x in ['数量', 'quantity', 'qty']):
                    column_mapping[col] = '数量\nQuantity'
                elif any(x in col_lower for x in ['处理方式', 'handling method', 'handling']):
                    column_mapping[col] = '处理方式\nHandling method'
                elif any(x in col_lower for x in ['原因', 'cause', 'reason']):
                    column_mapping[col] = '原因\nCause of defect'
                elif any(x in col_lower for x in ['检验站别', 'inspection station', 'station']):
                    column_mapping[col] = '检验站别\nInspection station'
                elif any(x in col_lower for x in ['当日检数量', 'inspection quantity', 'daily inspection']):
                    column_mapping[col] = '当日检数量\nInspection quantity'
                elif any(x in col_lower for x in ['备注', 'remark', 'note', 'comment']):
                    column_mapping[col] = '备注\nRemark'

            # Rename columns
            df_copy = df_copy.rename(columns=column_mapping)

            # Ensure all target columns exist (add empty ones if missing)
            all_target_cols = list(target_columns.values()) + ['客户名称\nClient Name']
            for col in all_target_cols:
                if col not in df_copy.columns:
                    df_copy[col] = ''

            # Reorder columns
            df_copy = df_copy[all_target_cols]

            return df_copy

        brushcard_final = []
        chokes_final = []
        sheets_written = 0

        print(f"🔍 Processing {len(uploaded_files)} files for Anhui plant...")

        for file in uploaded_files:
            if file and allowed_file(file.filename):
                try:
                    filename = file.filename
                    print(f"📂 Processing file: {filename}")
                    file_stream = BytesIO(file.read())
                    xls = pd.ExcelFile(file_stream, engine='openpyxl')
                    is_choke = "choke" in filename.lower() or "chocke" in filename.lower()
                    print(f"   📋 Available sheets: {xls.sheet_names}")
                    print(f"   🔧 Is choke file: {is_choke}")

                    if is_choke:
                        # Process chokes data
                        if 'Inspection data' in xls.sheet_names:
                            try:
                                df = pd.read_excel(xls, sheet_name='Inspection data', engine='openpyxl')
                                print(f"   ✅ Loaded {len(df)} rows from Inspection data sheet")

                                # Remove empty rows
                                df = df.dropna(how='all')

                                # For chokes, keep original columns and just add client name
                                client_name = extract_client_name(filename)
                                df['客户名称\nClient Name'] = client_name

                                if not df.empty:
                                    chokes_final.append(df)
                                    print(f"   📊 Added {len(df)} choke rows (Client: {client_name})")

                            except Exception as e:
                                print(f"   ❌ Error processing 'Inspection data' sheet in {filename}: {e}")
                        else:
                            # If 'Inspection data' not found, try the first sheet
                            try:
                                df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], engine='openpyxl')
                                print(f"   ⚠️ 'Inspection data' sheet not found. Using first sheet: {xls.sheet_names[0]}")

                                # Remove empty rows
                                df = df.dropna(how='all')

                                # For chokes, keep original columns and just add client name
                                client_name = extract_client_name(filename)
                                df['客户名称\nClient Name'] = client_name

                                if not df.empty:
                                    chokes_final.append(df)
                                    print(f"   📊 Added {len(df)} choke rows (Client: {client_name})")

                            except Exception as e:
                                print(f"   ❌ Error processing choke file {filename}: {e}")
                    else:
                        # Process brushcard files
                        print(f"📁 Processing brushcard file: {filename}")

                        # Look for sheets containing '质量汇总表'
                        target_sheets = [sheet for sheet in xls.sheet_names if '质量汇总表' in sheet]

                        if not target_sheets:
                            # If no sheet with '质量汇总表' found, try all sheets
                            target_sheets = xls.sheet_names
                            print(f"   ⚠️ No '质量汇总表' sheet found. Processing all sheets: {xls.sheet_names}")

                        processed_sheets = 0
                        for sheet_name in target_sheets:
                            try:
                                print(f"   🎯 Processing sheet: {sheet_name}")
                                df = pd.read_excel(xls, sheet_name=sheet_name, engine='openpyxl')

                                # Remove empty rows
                                df = df.dropna(how='all')

                                if len(df) == 0:
                                    print(f"   ⏭️ Skipping empty sheet: {sheet_name}")
                                    continue

                                # Extract client name from filename first, then try sheet name
                                client_name = extract_client_name(filename, sheet_name)

                                # Standardize columns
                                df_standardized = standardize_columns(df, client_name)
                                brushcard_final.append(df_standardized)
                                processed_sheets += 1

                                print(f"   ✅ Added {len(df_standardized)} rows from sheet '{sheet_name}' (Client: {client_name})")

                            except Exception as e:
                                print(f"   ❌ Error processing sheet '{sheet_name}' in {filename}: {e}")
                                continue

                        if processed_sheets == 0:
                            print(f"   ⚠️ No valid sheets processed in {filename}")
                        else:
                            print(f"   📊 Processed {processed_sheets} sheets from {filename}")

                except Exception as e:
                    print(f"❌ Failed to read file {file.filename}: {e}")

        # Create the Excel writer here, after we know we have data to write
        writer = pd.ExcelWriter(combined_output, engine='openpyxl')

        # Write outputs
        print(f"📊 Summary: brushcard_final has {len(brushcard_final)} items, chokes_final has {len(chokes_final)} items")

        # Write chokes data (keep original structure)
        if chokes_final:
            try:
                combined_chokes = pd.concat(chokes_final, ignore_index=True)
                combined_chokes.to_excel(writer, sheet_name='Chokes', index=False)
                sheets_written += 1
                print(f"📊 Chokes sheet created with {len(combined_chokes)} total rows")
                print(f"📋 Chokes columns: {list(combined_chokes.columns)}")
            except Exception as e:
                print(f"❌ Error creating Chokes sheet: {e}")
        else:
            # Create empty chokes sheet with basic headers
            empty_chokes_df = pd.DataFrame(columns=['客户名称\nClient Name'])
            empty_chokes_df.to_excel(writer, sheet_name='Chokes', index=False)
            sheets_written += 1
            print("📊 Chokes sheet created (empty)")

        # Write brushcard data
        if brushcard_final:
            try:
                combined_brushcard = pd.concat(brushcard_final, ignore_index=True)
                combined_brushcard.to_excel(writer, sheet_name='Brushcards', index=False)
                sheets_written += 1
                print(f"📊 Brushcards sheet created with {len(combined_brushcard)} total rows")
            except Exception as e:
                print(f"❌ Error creating Brushcards sheet: {e}")
        else:
            # Create empty brushcard sheet with headers
            empty_brushcard_df = pd.DataFrame(columns=[
                '生产日期\nProduction Date', '检验日期\nInspection Date', '型号\nType',
                '不良部位\nDefective Part', '不良名称\nDefect Name', '数量\nQuantity',
                '处理方式\nHandling method', '原因\nCause of defect', '检验站别\nInspection station',
                '当日检数量\nInspection quantity', '备注\nRemark', '客户名称\nClient Name'
            ])
            empty_brushcard_df.to_excel(writer, sheet_name='Brushcards', index=False)
            sheets_written += 1
            print("📊 Brushcards sheet created (empty)")

        # Close the writer
        writer.close()
        print("✅ Excel writer closed successfully")

        combined_output.seek(0)
        return combined_output

    else:
        # === Kunshan logic ===
        # Create the Excel writer for Kunshan
        print("🔧 Creating Excel writer for Kunshan...")
        writer = pd.ExcelWriter(combined_output, engine='openpyxl')
        print(f"🔧 Writer created successfully: {writer is not None}")

        # Fixed sheet names mapping
        kunshan_sheet_mapping = {
            "Date": "WindingStationRodChoke",
            "Data": ["GluingStationRodChoke", "RodChokeFinalInspection", "FuseChokeFinalInspection"],
            "Inspection data": "WindingStationFuseChoke"
        }

        print(f"🔍 Processing {len(uploaded_files)} files for Kunshan plant...")

        # Global counter for Data sheets across all files
        data_sheet_counter = 0

        for file in uploaded_files:
            if file and allowed_file(file.filename):
                try:
                    filename = file.filename
                    print(f"📂 Processing file: {filename}")
                    file_stream = BytesIO(file.read())
                    xls = pd.ExcelFile(file_stream, engine='openpyxl')
                    print(f"   📋 Available sheets: {xls.sheet_names}")

                    # Process each sheet according to the mapping

                    for sheet_name in xls.sheet_names:
                        sheet_name_clean = sheet_name.strip()
                        new_sheet_name = None

                        # Determine the new sheet name based on mapping
                        if sheet_name_clean == "Date":
                            new_sheet_name = kunshan_sheet_mapping["Date"]
                        elif sheet_name_clean == "Data":
                            # Handle multiple Data sheets
                            print(f"   📊 Found Data sheet #{data_sheet_counter + 1}")
                            if data_sheet_counter < len(kunshan_sheet_mapping["Data"]):
                                new_sheet_name = kunshan_sheet_mapping["Data"][data_sheet_counter]
                                print(f"   🎯 Mapping Data sheet #{data_sheet_counter + 1} → {new_sheet_name}")
                                data_sheet_counter += 1
                            else:
                                print(f"   ⚠️ More 'Data' sheets found than expected in {filename}")
                                new_sheet_name = f"Data_{data_sheet_counter + 1}"
                                data_sheet_counter += 1
                        elif sheet_name_clean == "Inspection data":
                            new_sheet_name = kunshan_sheet_mapping["Inspection data"]
                        else:
                            # Skip sheets that are not in our mapping
                            print(f"   ⏭️ Skipping sheet '{sheet_name_clean}' (not in predefined mapping)")
                            continue

                        if new_sheet_name:
                            try:
                                print(f"   🎯 Processing sheet: {sheet_name_clean} → {new_sheet_name}")
                                print(f"   🔧 Writer object exists: {writer is not None}")

                                df_raw = pd.read_excel(xls, sheet_name=sheet_name_clean, engine='openpyxl', header=None)
                                df_trimmed = df_raw.iloc[2:].reset_index(drop=True)
                                df_trimmed.columns = df_trimmed.iloc[0]
                                df = df_trimmed.iloc[1:].reset_index(drop=True)

                                # Standardize column names
                                new_columns = {}
                                for col in df.columns:
                                    col_lower = str(col).strip().lower()
                                    if col_lower in ['day', 'inspect date']:
                                        new_columns[col] = 'date'
                                df.rename(columns=new_columns, inplace=True)

                                # Remove 'type' columns
                                df.drop(columns=[col for col in df.columns if str(col).strip().lower() == 'type'], inplace=True)

                                # Ensure sheet name is within Excel limits (31 characters)
                                final_sheet_name = new_sheet_name[:31]
                                print(f"   📝 About to write to sheet: {final_sheet_name}")
                                df.to_excel(writer, sheet_name=final_sheet_name, index=False)

                                print(f"   ✅ Created sheet '{final_sheet_name}' with {len(df)} rows")

                            except Exception as e:
                                print(f"   ❌ Error processing sheet '{sheet_name_clean}' in {filename}: {e}")
                                import traceback
                                traceback.print_exc()

                except Exception as e:
                    print(f"❌ Error processing file {filename}: {e}")

        # Check if any sheets were created, if not create a summary sheet
        if not writer.sheets:
            print("⚠️ No sheets were processed successfully. Creating summary sheet.")
            summary_df = pd.DataFrame({
                "Status": ["No Data Processed"],
                "Message": ["No valid sheets found matching the predefined mapping"],
                "Expected_Sheets": ["Date, Data (multiple), Inspection data"],
                "Files_Processed": [len(uploaded_files)]
            })
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

        writer.close()
        combined_output.seek(0)
        return combined_output

# === Reusable POST Logic ===
def handle_post_request(plant):
    uploaded_files = request.files.getlist('files')

    # For Kunshan, we no longer need sheet names input as they are predefined
    if plant == "kunshan":
        sheet_names_input = None
        new_sheet_names_input = None
    else:
        # For other plants (like Anhui), sheet names might still be needed
        sheet_names_input = request.form.get('sheet_names')
        new_sheet_names_input = request.form.get('new_sheet_names')

    # Only require sheet names for plants other than Anhui and Kunshan
    if plant not in ["anhui", "kunshan"] and not sheet_names_input:
        return "Error: Sheet names input is required.", 400

    sheet_names_list = [group.strip().split(',') for group in sheet_names_input.strip().split(';')] if sheet_names_input else []
    new_sheet_names_list = [group.strip().split(',') for group in new_sheet_names_input.strip().split(';')] if new_sheet_names_input else None

    output = process_excel_files(uploaded_files, sheet_names_list, new_sheet_names_list, plant=plant)

    filename = f"{plant}_combined.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

# === Routes ===
@app.route('/kunshan', methods=['GET', 'POST'])
def kunshan():
    if request.method == 'POST':
        return handle_post_request('kunshan')
    return render_template('kunshan.html')

@app.route('/anhui', methods=['GET', 'POST'])
def anhui():
    if request.method == 'POST':
        return handle_post_request('anhui')
    return render_template('anhui.html')

# Optional default redirect to /kunshan
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        return handle_post_request('kunshan')
    return render_template('kunshan.html')
